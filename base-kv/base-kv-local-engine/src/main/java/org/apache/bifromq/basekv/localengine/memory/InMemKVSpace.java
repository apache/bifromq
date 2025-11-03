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

package org.apache.bifromq.basekv.localengine.memory;

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.bifromq.basekv.localengine.AbstractKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceRefreshableReader;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.localengine.SyncContext;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.slf4j.Logger;

abstract class InMemKVSpace<
    E extends InMemKVEngine<E, T>,
    T extends InMemKVSpace<E, T>> extends AbstractKVSpace<InMemKVSpaceEpoch> {
    protected final E engine;
    protected final ISyncContext syncContext = new SyncContext();
    protected final ISyncContext.IRefresher metadataRefresher = syncContext.refresher();
    protected final TrackedBoundaryIndex tracker = new TrackedBoundaryIndex();

    protected InMemKVSpace(String id,
                           Struct conf,
                           E engine,
                           Runnable onDestroy,
                           KVSpaceOpMeters opMeters,
                           Logger logger,
                           String... tags) {
        super(id, onDestroy, opMeters, logger, tags);
        this.engine = engine;
    }

    ISyncContext syncContext() {
        return syncContext;
    }

    @Override
    public IKVSpaceRefreshableReader reader() {
        return new InMemKVSpaceReader(id, opMeters, logger, syncContext.refresher(), this::handle, tracker);
    }

    protected void loadMetadata() {
        metadataRefresher.runIfNeeded((genBumped) -> {
            if (!handle().metadataMap().isEmpty()) {
                updateMetadata(Collections.unmodifiableMap(handle().metadataMap()));
            }
        });
    }

    protected long doSize(Boundary boundary) {
        if (!boundary.hasStartKey() && !boundary.hasEndKey()) {
            return handle().totalDataBytes();
        }
        // Track boundary size lazily and keep it updated on writes
        return tracker.sizeOrTrack(handle(), boundary);
    }

    static final class TrackedBoundaryIndex {
        private static final int MAX_TRACKED = 1024;
        private final NavigableMap<Boundary, TrackedBucket> buckets =
            new ConcurrentSkipListMap<>(BoundaryUtil::compare);

        long sizeOrTrack(InMemKVSpaceEpoch epoch, Boundary boundary) {
            TrackedBucket b = buckets.get(boundary);
            if (b != null) {
                b.touch();
                return b.bytes;
            }
            long sized = InMemKVHelper.sizeOfRange(epoch.dataMap(), boundary);
            if (buckets.size() >= MAX_TRACKED) {
                buckets.pollFirstEntry();
            }
            buckets.put(boundary, new TrackedBucket(sized));
            return sized;
        }

        void updateOnWrite(InMemKVSpaceWriterHelper.WriteImpact impact, NavigableMap<ByteString, ByteString> data) {
            if (impact == null || buckets.isEmpty()) {
                return;
            }
            Map<ByteString, Integer> delta = impact.pointDeltaBytes();
            if (delta == null || delta.isEmpty()) {
                return;
            }
            final long now = System.nanoTime();
            for (Map.Entry<Boundary, TrackedBucket> e : buckets.entrySet()) {
                final Boundary tracked = e.getKey();
                final TrackedBucket bucket = e.getValue();
                long deltaSum = 0L;
                // accumulate per-key delta inside the boundary
                for (Map.Entry<ByteString, Integer> de : delta.entrySet()) {
                    if (BoundaryUtil.inRange(de.getKey(), tracked)) {
                        deltaSum += de.getValue();
                    }
                }
                if (deltaSum != 0L) {
                    long newBytes = bucket.bytes + deltaSum;
                    bucket.bytes = Math.max(newBytes, 0L);
                    bucket.lastAccessNanos = now;
                }
            }
        }

        void invalidateAll() {
            buckets.clear();
        }

        private static final class TrackedBucket {
            volatile long bytes;
            volatile long lastAccessNanos;

            TrackedBucket(long bytes) {
                this.bytes = bytes;
                this.lastAccessNanos = System.nanoTime();
            }

            void touch() {
                lastAccessNanos = System.nanoTime();
            }
        }
    }
}
