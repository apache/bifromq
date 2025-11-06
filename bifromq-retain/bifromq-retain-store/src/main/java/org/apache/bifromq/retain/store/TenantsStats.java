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

package org.apache.bifromq.retain.store;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.intersect;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.isNULLRange;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.retain.store.schema.KVSchemaUtil.tenantBeginKey;

import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.api.IKVRangeRefreshableReader;

class TenantsStats {
    private final Map<String, TenantStats> retainedSet = new ConcurrentHashMap<>();
    private final Supplier<IKVRangeRefreshableReader> readerSupplier;
    private final String[] tags;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final StampedLock closeLock = new StampedLock();

    TenantsStats(Supplier<IKVRangeRefreshableReader> readerSupplier, String... tags) {
        this.readerSupplier = readerSupplier;
        this.tags = tags;
    }

    void increaseTopicCount(String tenantId, int delta) {
        retainedSet.compute(tenantId, (k, v) -> {
            if (v == null) {
                v = new TenantStats(tenantId, getTenantUsedSpaceProvider(tenantId), tags);
            }
            if (v.incrementTopicCount(delta) == 0) {
                v.destroy();
                return null;
            }
            return v;
        });
    }

    public void toggleMetering(boolean isLeader) {
        retainedSet.values().forEach(s -> s.toggleMetering(isLeader));
    }

    private Supplier<Number> getTenantUsedSpaceProvider(String tenantId) {
        return () -> {
            long stamped = closeLock.readLock();
            if (closed.get()) {
                closeLock.unlock(stamped);
                return 0;
            }
            ByteString tenantBeginKey = tenantBeginKey(tenantId);
            try (IKVRangeRefreshableReader reader = readerSupplier.get()) {
                Boundary tenantBoundary =
                    intersect(toBoundary(tenantBeginKey, upperBound(tenantBeginKey)), reader.boundary());
                if (isNULLRange(tenantBoundary)) {
                    return 0L;
                }
                return reader.size(tenantBoundary);
            } finally {
                closeLock.unlock(stamped);
            }
        };
    }

    void reset() {
        // clear gauges without marking closed
        long stamp = closeLock.writeLock();
        try {
            if (!closed.get()) {
                retainedSet.values().forEach(TenantStats::destroy);
                retainedSet.clear();
            }
        } finally {
            closeLock.unlock(stamp);
        }
    }

    void close() {
        long stamp = closeLock.writeLock();
        try {
            if (closed.compareAndSet(false, true)) {
                retainedSet.values().forEach(TenantStats::destroy);
                retainedSet.clear();
            }
        } finally {
            closeLock.unlock(stamp);
        }
    }
}
