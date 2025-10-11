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

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.toMetaKey;

import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.bifromq.basekv.localengine.IKVSpaceWriter;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.proto.Boundary;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;

class RocksDBKVSpaceWriter<E extends RocksDBKVEngine<E, T, C>, T extends
    RocksDBKVSpace<E, T, C>, C extends RocksDBKVEngineConfigurator<C>>
    extends RocksDBKVSpaceReader implements IKVSpaceWriter {
    protected final E engine;
    protected final RocksDBKVSpaceWriterHelper helper;
    private final IKVSpaceDBHandle dbHandle;
    private final ISyncContext syncContext;
    private final IWriteStatsRecorder.IRecorder writeStatsRecorder;

    RocksDBKVSpaceWriter(String id,
                         IKVSpaceDBHandle dbHandle,
                         E engine,
                         WriteOptions writeOptions,
                         ISyncContext syncContext,
                         IWriteStatsRecorder.IRecorder writeStatsRecorder,
                         Consumer<Map<ByteString, ByteString>> afterWrite,
                         KVSpaceOpMeters opMeters,
                         Logger logger) {
        this(id, dbHandle, engine, syncContext, new RocksDBKVSpaceWriterHelper(dbHandle.db(), writeOptions),
            writeStatsRecorder, afterWrite, opMeters, logger);
    }

    private RocksDBKVSpaceWriter(String id,
                                 IKVSpaceDBHandle dbHandle,
                                 E engine,
                                 ISyncContext syncContext,
                                 RocksDBKVSpaceWriterHelper writerHelper,
                                 IWriteStatsRecorder.IRecorder writeStatsRecorder,
                                 Consumer<Map<ByteString, ByteString>> afterWrite,
                                 KVSpaceOpMeters opMeters,
                                 Logger logger) {
        super(id, opMeters, logger);
        this.dbHandle = dbHandle;
        this.engine = engine;
        this.syncContext = syncContext;
        this.helper = writerHelper;
        this.writeStatsRecorder = writeStatsRecorder;
        writerHelper.addMutators(syncContext.mutator());
        writerHelper.addAfterWriteCallback(dbHandle.cf(), afterWrite);
    }

    @Override
    public IKVSpaceWriter metadata(ByteString metaKey, ByteString metaValue) {
        try {
            helper.metadata(dbHandle.cf(), metaKey, metaValue);
            return this;
        } catch (RocksDBException e) {
            throw new KVEngineException("Put in batch failed", e);
        }
    }

    @Override
    public IKVSpaceWriter insert(ByteString key, ByteString value) {
        try {
            helper.insert(dbHandle.cf(), key, value);
            writeStatsRecorder.recordInsert();
            return this;
        } catch (RocksDBException e) {
            throw new KVEngineException("Insert in batch failed", e);
        }
    }

    @Override
    public IKVSpaceWriter put(ByteString key, ByteString value) {
        try {
            helper.put(dbHandle.cf(), key, value);
            writeStatsRecorder.recordPut();
            return this;
        } catch (RocksDBException e) {
            throw new KVEngineException("Put in batch failed", e);
        }
    }

    @Override
    public IKVSpaceWriter delete(ByteString key) {
        try {
            helper.delete(dbHandle.cf(), key);
            writeStatsRecorder.recordDelete();
            return this;
        } catch (RocksDBException e) {
            throw new KVEngineException("Single delete in batch failed", e);
        }
    }

    @Override
    public IKVSpaceWriter clear() {
        return clear(Boundary.getDefaultInstance());
    }

    @Override
    public IKVSpaceWriter clear(Boundary boundary) {
        try {
            helper.clear(dbHandle.cf(), boundary);
            writeStatsRecorder.recordDeleteRange();
        } catch (Throwable e) {
            throw new KVEngineException("Delete range in batch failed", e);
        }
        return this;
    }

    @Override
    public void done() {
        opMeters.batchWriteCallTimer.record(() -> {
            try {
                opMeters.writeBatchSizeSummary.record(helper.count());
                helper.done();
                writeStatsRecorder.stop();
            } catch (Throwable e) {
                logger.error("Write Batch commit failed", e);
                throw new KVEngineException("Batch commit failed", e);
            }
        });
    }

    @Override
    public void reset() {
        helper.reset();
    }

    @Override
    public void abort() {
        helper.abort();
    }

    @Override
    public int count() {
        return helper.count();
    }

    @Override
    protected Optional<ByteString> doMetadata(ByteString metaKey) {
        try {
            byte[] metaValBytes = dbHandle.db().get(dbHandle.cf(), toMetaKey(metaKey));
            return Optional.ofNullable(metaValBytes == null ? null : unsafeWrap(metaValBytes));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected IKVSpaceDBInstance handle() {
        return dbHandle;
    }

    @Override
    protected ISyncContext.IRefresher newRefresher() {
        return syncContext.refresher();
    }
}
