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

import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.IKVSpaceMigratableWriter;
import org.apache.bifromq.basekv.localengine.IRestoreSession;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.proto.Boundary;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;

class RocksDBKVSpaceMigratableWriter extends RocksDBKVSpaceWriter
    implements IKVSpaceMigratableWriter {

    RocksDBKVSpaceMigratableWriter(String id,
                                   IRocksDBKVSpaceEpochHandle dbHandle,
                                   RocksDBKVEngine<?> engine,
                                   WriteOptions writeOptions,
                                   ISyncContext syncContext,
                                   IWriteStatsRecorder.IRecorder writeStatsRecorder,
                                   Consumer<Map<ByteString, ByteString>> afterWrite,
                                   KVSpaceOpMeters opMeters,
                                   Logger logger) {
        super(id, dbHandle, engine, writeOptions, syncContext, writeStatsRecorder, afterWrite, opMeters, logger);
    }

    @Override
    public IRestoreSession migrateTo(String targetSpaceId, Boundary boundary) {
        try {
            RocksDBCPableKVSpace targetKVSpace = (RocksDBCPableKVSpace) engine.createIfMissing(targetSpaceId);
            IRestoreSession targetSpaceRestoreSession = targetKVSpace.startRestore((count, bytes) ->
                logger.debug("Migrate {} kv to space[{}] from space[{}]: startKey={}, endKey={}",
                    count, targetSpaceId, id, boundary.getStartKey().toStringUtf8(),
                    boundary.getEndKey().toStringUtf8()));
            try (IKVSpaceIterator itr = new RocksDBKVSpaceIterator(new RocksDBSnapshot(dbHandle, null), boundary,
                new IteratorOptions(false, 52428))) {
                for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                    targetSpaceRestoreSession.put(itr.key(), itr.value());
                }
            }
            // clear moved data in left range
            helper.clear(dbHandle.cf(), boundary);
            return targetSpaceRestoreSession;
        } catch (Throwable e) {
            throw new KVEngineException("Delete range in batch failed", e);
        }
    }
}
