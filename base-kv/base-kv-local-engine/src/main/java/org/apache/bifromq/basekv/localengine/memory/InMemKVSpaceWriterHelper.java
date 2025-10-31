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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.proto.Boundary;

class InMemKVSpaceWriterHelper {
    private final Map<String, InMemKVSpaceEpoch> kvSpaceEpochMap;
    private final Map<String, WriteBatch> batchMap;
    private final Map<String, Consumer<Boolean>> afterWriteCallbacks = new HashMap<>();
    private final Map<String, Consumer<WriteImpact>> afterImpactCallbacks = new HashMap<>();
    private final Map<String, Boolean> metadataChanges = new HashMap<>();
    private final Set<ISyncContext.IMutator> mutators = new HashSet<>();

    InMemKVSpaceWriterHelper() {
        this.kvSpaceEpochMap = new HashMap<>();
        this.batchMap = new HashMap<>();
    }

    void addMutators(String id,
                     InMemKVSpaceEpoch epoch,
                     ISyncContext.IMutator mutator) {
        kvSpaceEpochMap.put(id, epoch);
        mutators.add(mutator);
    }

    void addAfterWriteCallback(String rangeId, Consumer<Boolean> afterWrite) {
        afterWriteCallbacks.put(rangeId, afterWrite);
        metadataChanges.put(rangeId, false);
    }

    void addAfterImpactCallback(String rangeId, Consumer<WriteImpact> afterImpact) {
        afterImpactCallbacks.put(rangeId, afterImpact);
    }

    void metadata(String rangeId, ByteString metaKey, ByteString metaValue) {
        batchMap.computeIfAbsent(rangeId, k -> new WriteBatch(rangeId)).metadata(metaKey, metaValue);
        metadataChanges.computeIfPresent(rangeId, (k, v) -> true);
    }

    void insert(String rangeId, ByteString key, ByteString value) {
        batchMap.computeIfAbsent(rangeId, k -> new WriteBatch(rangeId)).insert(key, value);
    }

    void put(String rangeId, ByteString key, ByteString value) {
        batchMap.computeIfAbsent(rangeId, k -> new WriteBatch(rangeId)).put(key, value);
    }

    void delete(String rangeId, ByteString key) {
        batchMap.computeIfAbsent(rangeId, k -> new WriteBatch(rangeId)).delete(key);
    }

    void clear(String rangeId, Boundary boundary) {
        batchMap.computeIfAbsent(rangeId, k -> new WriteBatch(rangeId)).deleteRange(boundary);
    }

    void done() {
        ISyncContext.IMutation doneFn = () -> {
            Map<String, WriteImpact> impacts = new HashMap<>();
            batchMap.values().forEach(batch -> {
                WriteImpact impact = batch.endAndCollectImpact();
                if (impact != null) {
                    impacts.put(batch.rangeId, impact);
                }
            });
            impacts.forEach((id, imp) -> {
                Consumer<WriteImpact> cb = afterImpactCallbacks.get(id);
                if (cb != null) {
                    cb.accept(imp);
                }
            });
            return false;
        };
        AtomicReference<ISyncContext.IMutation> finalRun = new AtomicReference<>();
        for (ISyncContext.IMutator mutator : mutators) {
            if (finalRun.get() == null) {
                finalRun.set(() -> mutator.run(doneFn));
            } else {
                ISyncContext.IMutation innerRun = finalRun.get();
                finalRun.set(() -> mutator.run(innerRun));
            }
        }
        finalRun.get().mutate();
        for (String rangeId : afterWriteCallbacks.keySet()) {
            afterWriteCallbacks.get(rangeId).accept(metadataChanges.get(rangeId));
        }
    }

    void abort() {
        batchMap.clear();
    }

    int count() {
        return batchMap.values().stream().map(WriteBatch::count).reduce(0, Integer::sum);
    }

    protected interface KVAction {
        KVAction.Type type();

        enum Type { Put, Delete, DeleteRange }
    }

    public record WriteImpact(List<ByteString> pointKeys,
                              List<Boundary> deleteRanges,
                              Map<ByteString, Integer> pointDeltaBytes) {
    }

    protected class WriteBatch {
        private final String rangeId;
        Map<ByteString, ByteString> metadata = new HashMap<>();
        List<KVAction> actions = new ArrayList<>();

        protected WriteBatch(String spaceId) {
            this.rangeId = spaceId;
        }

        public void metadata(ByteString key, ByteString value) {
            metadata.put(key, value);
        }

        public int count() {
            return actions.size();
        }

        public void insert(ByteString key, ByteString value) {
            actions.add(new WriteBatch.Put(key, value));
        }

        public void put(ByteString key, ByteString value) {
            actions.add(new WriteBatch.Put(key, value));
        }

        public void delete(ByteString key) {
            actions.add(new WriteBatch.Delete(key));
        }

        public void deleteRange(Boundary boundary) {
            actions.add(new WriteBatch.DeleteRange(boundary));
        }

        public WriteImpact endAndCollectImpact() {
            List<ByteString> putOrDeleteKeys = new ArrayList<>();
            List<Boundary> deleteRanges = new ArrayList<>();
            Map<ByteString, Integer> pointDeltaBytes = new HashMap<>();
            InMemKVSpaceEpoch epoch = kvSpaceEpochMap.get(rangeId);

            metadata.forEach(epoch::setMetadata);

            for (KVAction action : actions) {
                switch (action.type()) {
                    case Put -> {
                        WriteBatch.Put put = (WriteBatch.Put) action;
                        // compute delta before mutation
                        ByteString old = epoch.dataMap().get(put.key);
                        int oldBytes = old == null ? 0 : (put.key.size() + old.size());
                        int newBytes = put.key.size() + put.value.size();
                        int delta = newBytes - oldBytes;
                        if (delta != 0) {
                            pointDeltaBytes.merge(put.key, delta, Integer::sum);
                        }
                        epoch.putData(put.key, put.value);
                        putOrDeleteKeys.add(put.key);
                    }
                    case Delete -> {
                        WriteBatch.Delete delete = (WriteBatch.Delete) action;
                        // compute delta before mutation
                        ByteString old = epoch.dataMap().get(delete.key);
                        if (old != null) {
                            int delta = -(delete.key.size() + old.size());
                            pointDeltaBytes.merge(delete.key, delta, Integer::sum);
                            epoch.removeData(delete.key);
                        }
                        putOrDeleteKeys.add(delete.key);
                    }
                    case DeleteRange -> {
                        WriteBatch.DeleteRange deleteRange = (WriteBatch.DeleteRange) action;
                        Boundary boundary = deleteRange.boundary;
                        NavigableSet<ByteString> inRangeKeys;
                        if (!boundary.hasStartKey() && !boundary.hasEndKey()) {
                            inRangeKeys = epoch.dataMap().navigableKeySet();
                        } else if (!boundary.hasStartKey()) {
                            inRangeKeys = epoch.dataMap().headMap(boundary.getEndKey(), false).navigableKeySet();
                        } else if (!boundary.hasEndKey()) {
                            inRangeKeys = epoch.dataMap().tailMap(boundary.getStartKey(), true).navigableKeySet();
                        } else {
                            inRangeKeys = epoch.dataMap()
                                .subMap(boundary.getStartKey(), true, boundary.getEndKey(), false)
                                .navigableKeySet();
                        }
                        // accumulate negative delta per key before removal
                        for (ByteString k : inRangeKeys) {
                            ByteString v = epoch.dataMap().get(k);
                            if (v != null) {
                                int delta = -(k.size() + v.size());
                                pointDeltaBytes.merge(k, delta, Integer::sum);
                            }
                        }
                        inRangeKeys.forEach(epoch::removeData);
                        deleteRanges.add(boundary);
                    }
                    default -> {
                        // no-op
                    }
                }
            }
            return new WriteImpact(putOrDeleteKeys, deleteRanges, pointDeltaBytes);
        }

        public void abort() {

        }

        record Put(ByteString key, ByteString value) implements KVAction {
            @Override
            public Type type() {
                return Type.Put;
            }
        }

        record Delete(ByteString key) implements KVAction {
            @Override
            public Type type() {
                return Type.Delete;
            }

        }

        record DeleteRange(Boundary boundary) implements KVAction {
            @Override
            public Type type() {
                return Type.DeleteRange;
            }
        }
    }
}
