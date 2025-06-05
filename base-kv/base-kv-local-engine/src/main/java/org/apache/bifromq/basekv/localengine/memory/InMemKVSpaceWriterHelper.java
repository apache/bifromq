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

import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class InMemKVSpaceWriterHelper {
    private final Map<String, Map<ByteString, ByteString>> metadataMap;
    private final Map<String, ConcurrentSkipListMap<ByteString, ByteString>> rangeDataMap;
    private final Map<String, WriteBatch> batchMap;
    private final Map<String, Consumer<Boolean>> afterWriteCallbacks = new HashMap<>();
    private final Map<String, Boolean> metadataChanges = new HashMap<>();
    private final Set<ISyncContext.IMutator> mutators = new HashSet<>();

    InMemKVSpaceWriterHelper() {
        this.metadataMap = new HashMap<>();
        this.rangeDataMap = new HashMap<>();
        this.batchMap = new HashMap<>();
    }

    void addMutators(String rangeId,
                     Map<ByteString, ByteString> metadata,
                     ConcurrentSkipListMap<ByteString, ByteString> rangeData,
                     ISyncContext.IMutator mutator) {
        metadataMap.put(rangeId, metadata);
        rangeDataMap.put(rangeId, rangeData);
        mutators.add(mutator);
    }

    void addAfterWriteCallback(String rangeId, Consumer<Boolean> afterWrite) {
        afterWriteCallbacks.put(rangeId, afterWrite);
        metadataChanges.put(rangeId, false);
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
        Runnable doneFn = () -> batchMap.values().forEach(WriteBatch::end);
        AtomicReference<Runnable> finalRun = new AtomicReference<>();
        for (ISyncContext.IMutator mutator : mutators) {
            if (finalRun.get() == null) {
                finalRun.set(() -> mutator.run(doneFn));
            } else {
                Runnable innerRun = finalRun.get();
                finalRun.set(() -> mutator.run(innerRun));
            }
        }
        finalRun.get().run();
        for (String rangeId : afterWriteCallbacks.keySet()) {
            afterWriteCallbacks.get(rangeId).accept(metadataChanges.get(rangeId));
        }
    }

    void abort() {
        rangeDataMap.clear();
    }

    int count() {
        return batchMap.values().stream().map(WriteBatch::count).reduce(0, Integer::sum);
    }

    protected interface KVAction {
        KVAction.Type type();

        enum Type {Put, Delete, DeleteRange}
    }

    protected class WriteBatch {
        private final String rangeId;
        Map<ByteString, ByteString> metadata = new HashMap<>();
        List<KVAction> actions = new ArrayList<>();

        protected WriteBatch(String rangeId) {
            this.rangeId = rangeId;
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

        public void end() {
            metadataMap.get(rangeId).putAll(metadata);
            for (KVAction action : actions) {
                switch (action.type()) {
                    case Put -> {
                        WriteBatch.Put put = (WriteBatch.Put) action;
                        rangeDataMap.get(rangeId).put(put.key, put.value);
                    }
                    case Delete -> {
                        WriteBatch.Delete delete = (WriteBatch.Delete) action;
                        rangeDataMap.get(rangeId).remove(delete.key);
                    }
                    case DeleteRange -> {
                        WriteBatch.DeleteRange deleteRange = (WriteBatch.DeleteRange) action;
                        Boundary boundary = deleteRange.boundary;
                        NavigableSet<ByteString> inRangeKeys;
                        if (!boundary.hasStartKey() && !boundary.hasEndKey()) {
                            inRangeKeys = rangeDataMap.get(rangeId).keySet();
                        } else if (!boundary.hasStartKey()) {
                            inRangeKeys = rangeDataMap.get(rangeId).headMap(boundary.getEndKey()).keySet();
                        } else if (!boundary.hasEndKey()) {
                            inRangeKeys = rangeDataMap.get(rangeId).tailMap(boundary.getStartKey()).keySet();
                        } else {
                            inRangeKeys =
                                rangeDataMap.get(rangeId).subMap(boundary.getStartKey(), boundary.getEndKey()).keySet();
                        }
                        inRangeKeys.forEach(k -> rangeDataMap.get(rangeId).remove(k));
                    }
                }
            }
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
