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

package org.apache.bifromq.inbox.store;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.intersect;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.isNULLRange;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.isInboxInstanceKey;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.isInboxInstanceStartKey;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.parseInboxInstanceStartKeyPrefix;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.parseTenantId;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.tenantBeginKeyPrefix;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
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
import org.apache.bifromq.inbox.storage.proto.InboxMetadata;

@Slf4j
public class TenantsStats implements ITenantStats {
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
    public void addSessionCount(String tenantId, int delta) {
        taskQueue.offer(() -> doAddSessionCount(tenantId, delta));
        trigger();
    }

    @Override
    public void addSubCount(String tenantId, int delta) {
        taskQueue.offer(() -> doAddSubCount(tenantId, delta));
        trigger();
    }

    @Override
    public void toggleMetering(boolean isLeader) {
        taskQueue.offer(() -> tenantStatsMap.values().forEach(s -> s.toggleMetering(isLeader)));
        trigger();
    }

    @Override
    public void reset(Boundary boundary) {
        taskQueue.offer(() -> doReset(boundary));
        trigger();
    }

    @Override
    public void close() {
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        // Ensure gauges are unregistered and internal state cleared on close
        taskQueue.offer(() -> {
            try {
                tenantStatsMap.values().forEach(TenantStats::destroy);
                tenantStatsMap.clear();
            } finally {
                closeFuture.complete(null);
            }
        });
        trigger();
        closeFuture.join();
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
                    log.warn("InboxStore tenant stats task failed", e);
                }
            }
        } finally {
            draining.set(false);
            if (!taskQueue.isEmpty()) {
                trigger();
            }
        }
    }

    private void doAddSessionCount(String tenantId, int delta) {
        if (delta == 0) {
            return;
        }
        tenantStatsMap.compute(tenantId, (k, v) -> {
            if (v == null) {
                if (delta < 0) {
                    // nothing to do for negative delta on non-existing tenant
                    return null;
                }
                v = new TenantStats(tenantId, getTenantUsedSpaceProvider(tenantId), tags);
            }
            v.addSessionCount(delta);
            if (v.isNoSession()) {
                v.destroy();
                return null;
            }
            return v;
        });
    }

    private void doAddSubCount(String tenantId, int delta) {
        if (delta == 0) {
            return;
        }
        tenantStatsMap.compute(tenantId, (k, v) -> {
            if (v == null) {
                if (delta < 0) {
                    // nothing to do for negative delta on non-existing tenant
                    return null;
                }
                v = new TenantStats(tenantId, getTenantUsedSpaceProvider(tenantId), tags);
            }
            v.addSubCount(delta);
            return v;
        });
    }

    private Supplier<Number> getTenantUsedSpaceProvider(String tenantId) {
        return () -> {
            try (IKVCloseableReader reader = readerSupplier.get()) {
                ByteString startKey = tenantBeginKeyPrefix(tenantId);
                ByteString endKey = upperBound(startKey);
                Boundary tenantBoundary = intersect(boundary, toBoundary(startKey, endKey));
                if (isNULLRange(tenantBoundary)) {
                    return 0;
                }
                return reader.size(tenantBoundary);
            } catch (Exception e) {
                log.error("Failed to get used space for tenant:{}", tenantId, e);
                return 0;
            }
        };
    }

    private void doReset(Boundary boundary) {
        tenantStatsMap.values().forEach(TenantStats::destroy);
        tenantStatsMap.clear();
        try (IKVCloseableReader reader = readerSupplier.get()) {
            this.boundary = boundary;
            reader.refresh();
            IKVIterator itr = reader.iterator();
            for (itr.seekToFirst(); itr.isValid(); ) {
                String tenantId = parseTenantId(itr.key());
                loadStats(tenantId, itr);
                itr.seek(upperBound(tenantBeginKeyPrefix(tenantId)));
            }
        } catch (Throwable e) {
            log.error("Async load inbox store tenant stats failed", e);
        }
    }

    private void loadStats(String tenantId, IKVIterator itr) {
        String inboxId = null;
        InboxMetadata inboxMetadata;
        ByteString beginKeyPrefix = tenantBeginKeyPrefix(tenantId);
        int probe = 0;
        for (itr.seek(beginKeyPrefix); itr.isValid() && itr.key().startsWith(beginKeyPrefix); ) {
            if (isInboxInstanceStartKey(itr.key())) {
                try {
                    inboxMetadata = InboxMetadata.parseFrom(itr.value());
                    if (inboxId == null || !inboxId.equals(inboxMetadata.getInboxId())) {
                        inboxId = inboxMetadata.getInboxId();
                        doAddSessionCount(tenantId, 1);
                    }
                    doAddSubCount(tenantId, inboxMetadata.getTopicFiltersCount());
                } catch (InvalidProtocolBufferException e) {
                    log.error("Unexpected error", e);
                } finally {
                    itr.next();
                    probe++;
                }
            } else {
                if (probe < 20) {
                    itr.next();
                    probe++;
                } else {
                    if (isInboxInstanceKey(itr.key())) {
                        itr.seek(upperBound(parseInboxInstanceStartKeyPrefix(itr.key())));
                    } else {
                        itr.next();
                        probe++;
                    }
                }
            }
        }
    }
}
