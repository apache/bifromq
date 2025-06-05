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

package org.apache.bifromq.basekv.localengine.rocksdb;

import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.DATA_SECTION_END;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.DATA_SECTION_START;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.toDataKey;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.toMetaKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.endKeyBytes;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.startKeyBytes;

import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

class RocksDBKVSpaceWriterHelper {
    private final RocksDB db;
    private final WriteOptions writeOptions;
    private final WriteBatch batch;
    private final Map<ColumnFamilyHandle, Consumer<Map<ByteString, ByteString>>> afterWriteCallbacks = new HashMap<>();
    private final Map<ColumnFamilyHandle, Map<ByteString, ByteString>> metadataChanges = new HashMap<>();
    private final Set<ISyncContext.IMutator> mutators = new HashSet<>();

    RocksDBKVSpaceWriterHelper(RocksDB db, WriteOptions writeOptions) {
        this.db = db;
        this.writeOptions = writeOptions;
        this.batch = new WriteBatch();

    }

    void addMutators(ISyncContext.IMutator mutator) {
        mutators.add(mutator);
    }

    void addAfterWriteCallback(ColumnFamilyHandle cfHandle, Consumer<Map<ByteString, ByteString>> afterWrite) {
        afterWriteCallbacks.put(cfHandle, afterWrite);
        metadataChanges.put(cfHandle, new HashMap<>());
    }

    void metadata(ColumnFamilyHandle cfHandle, ByteString metaKey, ByteString metaValue) throws RocksDBException {
        byte[] key = toMetaKey(metaKey);
        batch.singleDelete(cfHandle, key);
        batch.put(cfHandle, key, metaValue.toByteArray());
        metadataChanges.computeIfPresent(cfHandle, (k, v) -> {
            v.put(metaKey, metaValue);
            return v;
        });
    }

    void insert(ColumnFamilyHandle cfHandle, ByteString key, ByteString value) throws RocksDBException {
        batch.put(cfHandle, toDataKey(key), value.toByteArray());
    }

    void put(ColumnFamilyHandle cfHandle, ByteString key, ByteString value) throws RocksDBException {
        byte[] dataKey = toDataKey(key);
        batch.singleDelete(cfHandle, dataKey);
        batch.put(cfHandle, dataKey, value.toByteArray());
    }

    void delete(ColumnFamilyHandle cfHandle, ByteString key) throws RocksDBException {
        batch.singleDelete(cfHandle, toDataKey(key));
    }

    void clear(ColumnFamilyHandle cfHandle, Boundary boundary) throws RocksDBException {
        byte[] startKey = startKeyBytes(boundary);
        byte[] endKey = endKeyBytes(boundary);
        startKey = startKey == null ? DATA_SECTION_START : toDataKey(startKey);
        endKey = endKey == null ? DATA_SECTION_END : toDataKey(endKey);
        batch.deleteRange(cfHandle, startKey, endKey);
    }

    void done() throws RocksDBException {
        Runnable doneFn = () -> {
            try {
                if (batch.count() > 0) {
                    db.write(writeOptions, batch);
                    batch.clear();
                    batch.close();
                }
            } catch (Throwable e) {
                throw new KVEngineException("Range write error", e);
            } finally {
                if (batch.isOwningHandle()) {
                    batch.close();
                }
            }
        };
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
        for (ColumnFamilyHandle columnFamilyHandle : afterWriteCallbacks.keySet()) {
            Map<ByteString, ByteString> updatedMetadata = metadataChanges.get(columnFamilyHandle);
            afterWriteCallbacks.get(columnFamilyHandle).accept(updatedMetadata);
            updatedMetadata.clear();
        }
    }

    void abort() {
        batch.clear();
        batch.close();
    }

    int count() {
        return batch.count();
    }
}
