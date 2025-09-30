/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.dist.worker;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.intersect;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.isNULLRange;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.tenantBeginKey;

import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.dist.worker.schema.cache.RouteDetail;
import org.apache.bifromq.dist.worker.schema.cache.RouteDetailCache;
import org.apache.bifromq.type.RouteMatcher;

@Slf4j
class TenantsStats implements ITenantsStats {
    private final Map<String, TenantStats> tenantStatsMap = new ConcurrentHashMap<>();
    private final Supplier<IKVCloseableReader> readerSupplier;
    private final String[] tags;
    // ultra-simple async queue and single drainer
    private final ConcurrentLinkedQueue<Runnable> taskQueue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean draining = new AtomicBoolean(false);
    private transient Boundary boundary;

    TenantsStats(Supplier<IKVCloseableReader> readerSupplier, String... tags) {
        this.readerSupplier = readerSupplier;
        this.tags = tags;
        try (IKVCloseableReader reader = readerSupplier.get()) {
            boundary = reader.boundary();
        }
    }

    @Override
    public void incNormalRoutes(String tenantId) {
        incNormalRoutes(tenantId, 1);
    }

    @Override
    public void incNormalRoutes(String tenantId, int count) {
        assert count > 0;
        taskQueue.offer(() -> doAddNormalRoutes(tenantId, count));
        trigger();
    }

    @Override
    public void decNormalRoutes(String tenantId) {
        decNormalRoutes(tenantId, 1);
    }

    @Override
    public void decNormalRoutes(String tenantId, int count) {
        assert count > 0;
        taskQueue.offer(() -> doAddNormalRoutes(tenantId, -count));
        trigger();
    }

    @Override
    public void incSharedRoutes(String tenantId) {
        incSharedRoutes(tenantId, 1);
    }

    @Override
    public void incSharedRoutes(String tenantId, int count) {
        assert count > 0;
        taskQueue.offer(() -> doAddSharedRoutes(tenantId, count));
        trigger();
    }

    @Override
    public void decSharedRoutes(String tenantId) {
        decSharedRoutes(tenantId, 1);
    }

    @Override
    public void decSharedRoutes(String tenantId, int count) {
        assert count > 0;
        taskQueue.offer(() -> doAddSharedRoutes(tenantId, -count));
        trigger();
    }

    @Override
    public void toggleMetering(boolean isLeader) {
        taskQueue.offer(() -> tenantStatsMap.values().forEach(s -> s.toggleMetering(isLeader)));
        trigger();
    }

    @Override
    public void reset() {
        taskQueue.offer(this::doReset);
        trigger();
    }

    @Override
    public void close() {
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        taskQueue.offer(() -> {
            tenantStatsMap.values().forEach(TenantStats::destroy);
            tenantStatsMap.clear();
            closeFuture.complete(null);
        });
        trigger();
        closeFuture.join();
    }

    private Supplier<Number> getSpaceUsageProvider(String tenantId) {
        return () -> {
            try (IKVCloseableReader reader = readerSupplier.get()) {
                ByteString tenantStartKey = tenantBeginKey(tenantId);
                Boundary tenantSection = intersect(boundary, toBoundary(tenantStartKey, upperBound(tenantStartKey)));
                if (isNULLRange(tenantSection)) {
                    return 0;
                }
                return reader.size(tenantSection);
            } catch (Exception e) {
                log.error("Unexpected error", e);
                return 0;
            }
        };
    }

    private void doAddNormalRoutes(String tenantId, int delta) {
        if (delta == 0) {
            return;
        }
        tenantStatsMap.compute(tenantId, (k, v) -> {
            if (v == null) {
                if (delta < 0) {
                    // nothing to do for negative delta on non-existing tenant
                    return null;
                }
                v = new TenantStats(tenantId, getSpaceUsageProvider(tenantId), tags);
            }
            v.addNormalRoutes(delta);
            if (v.isNoRoutes()) {
                v.destroy();
                return null;
            }
            return v;
        });
    }

    private void doAddSharedRoutes(String tenantId, int delta) {
        if (delta == 0) {
            return;
        }
        tenantStatsMap.compute(tenantId, (k, v) -> {
            if (v == null) {
                if (delta < 0) {
                    return null;
                }
                v = new TenantStats(tenantId, getSpaceUsageProvider(tenantId), tags);
            }
            v.addSharedRoutes(delta);
            if (v.isNoRoutes()) {
                v.destroy();
                return null;
            }
            return v;
        });
    }

    private void trigger() {
        if (draining.compareAndSet(false, true)) {
            ForkJoinPool.commonPool().execute(this::drain);
        }
    }

    private void drain() {
        try {
            Runnable r;
            while ((r = taskQueue.poll()) != null) {
                try {
                    r.run();
                } catch (Throwable e) {
                    log.warn("DistWorker tenant stats task failed", e);
                }
            }
        } finally {
            draining.set(false);
            if (!taskQueue.isEmpty()) {
                trigger();
            }
        }
    }

    private void doReset() {
        try (IKVCloseableReader reader = readerSupplier.get()) {
            tenantStatsMap.values().forEach(TenantStats::destroy);
            tenantStatsMap.clear();
            reader.refresh();
            boundary = reader.boundary();
            // enqueue full reload task; don't block caller
            IKVIterator itr = reader.iterator();
            for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                RouteDetail routeDetail = RouteDetailCache.get(itr.key());
                if (routeDetail.matcher().getType() == RouteMatcher.Type.Normal) {
                    doAddNormalRoutes(routeDetail.tenantId(), 1);
                } else {
                    doAddSharedRoutes(routeDetail.tenantId(), 1);
                }
            }
        } catch (Throwable e) {
            log.error("Async load dist worker tenant stats failed", e);
        }
    }
}
