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

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.IKVSpaceRefreshableReader;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.slf4j.Logger;

class InMemKVSpaceReader extends AbstractInMemKVSpaceReader implements IKVSpaceRefreshableReader {
    private static final int MAX_CACHE_ENTRIES = 1024;
    private final ISyncContext.IRefresher refresher;
    private final Supplier<InMemKVSpaceEpoch> epochSupplier;
    private final InMemKVSpace.TrackedBoundaryIndex tracker;
    private final Set<InMemKVSpaceIterator> openedIterators = Sets.newConcurrentHashSet();
    private final Map<Boundary, Long> sizeCache = new ConcurrentSkipListMap<>(BoundaryUtil::compare);
    private volatile InMemKVSpaceEpoch currentEpoch;

    InMemKVSpaceReader(String id,
                       KVSpaceOpMeters readOpMeters,
                       Logger logger,
                       ISyncContext.IRefresher refresher,
                       Supplier<InMemKVSpaceEpoch> epochSupplier,
                       InMemKVSpace.TrackedBoundaryIndex tracker) {
        super(id, readOpMeters, logger);
        this.refresher = refresher;
        this.epochSupplier = epochSupplier;
        this.tracker = tracker;
        this.currentEpoch = epochSupplier.get();
    }

    @Override
    protected Map<ByteString, ByteString> metadataMap() {
        return currentEpoch.metadataMap();
    }

    @Override
    protected NavigableMap<ByteString, ByteString> rangeData() {
        return currentEpoch.dataMap();
    }

    @Override
    public void close() {

    }

    @Override
    public void refresh() {
        refresher.runIfNeeded((genBumped) -> {
            currentEpoch = epochSupplier.get();
            sizeCache.clear();
            openedIterators.forEach(itr -> itr.refresh(currentEpoch.dataMap()));
        });
    }

    @Override
    protected IKVSpaceIterator doNewIterator(Boundary subBoundary) {
        InMemKVSpaceIterator itr = new InMemKVSpaceIterator(rangeData(), subBoundary, openedIterators::remove);
        openedIterators.add(itr);
        return itr;
    }

    @Override
    protected long doSize(Boundary boundary) {
        // Fast path for full-range size
        if (!boundary.hasStartKey() && !boundary.hasEndKey()) {
            return currentEpoch.totalDataBytes();
        }
        // Consult reader-local cache first
        Long cached = sizeCache.get(boundary);
        if (cached != null) {
            return cached;
        }
        // Delegate to space-level tracker for lazy tracking
        long sized = tracker.sizeOrTrack(currentEpoch, boundary);
        if (sizeCache.size() >= MAX_CACHE_ENTRIES) {
            try {
                @SuppressWarnings("unchecked")
                ConcurrentSkipListMap<Boundary, Long> sl = (ConcurrentSkipListMap<Boundary, Long>) sizeCache;
                if (sl.pollFirstEntry() == null && !sizeCache.isEmpty()) {
                    // fallback best-effort
                    sizeCache.remove(sizeCache.keySet().iterator().next());
                }
            } catch (ClassCastException ignored) {
                // best-effort fallback if type changes in future
                if (!sizeCache.isEmpty()) {
                    sizeCache.remove(sizeCache.keySet().iterator().next());
                }
            }
        }
        sizeCache.put(boundary, sized);
        return sized;
    }
}
