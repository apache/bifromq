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

import static org.apache.bifromq.basekv.localengine.IKVEngine.DEFAULT_NS;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.toMetaKey;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBKVSpace.deleteDir;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

class RocksDBKVSpaceCheckpoint extends RocksDBKVSpaceReader implements IRocksDBKVSpaceCheckpoint {
    private static final Cleaner CLEANER = Cleaner.create();
    private final String cpId;
    private final DBOptions dbOptions;
    private final RocksDB roDB;
    private final ColumnFamilyDescriptor cfDesc;
    private final ColumnFamilyHandle cfHandle;
    private final Cleaner.Cleanable cleanable;

    RocksDBKVSpaceCheckpoint(String id,
                             String cpId,
                             File cpDir,
                             Predicate<String> isLatest,
                             KVSpaceOpMeters opMeters,
                             Logger logger) {
        super(id, opMeters, logger);
        this.cpId = cpId;
        try {
            cfDesc = new ColumnFamilyDescriptor(DEFAULT_NS.getBytes());
            cfDesc.getOptions().setTableFormatConfig(new BlockBasedTableConfig()
                .setNoBlockCache(true)
                .setBlockCache(null));
            dbOptions = new DBOptions();

            List<ColumnFamilyDescriptor> cfDescs = List.of(cfDesc);

            List<ColumnFamilyHandle> handles = new ArrayList<>();
            roDB = RocksDB.openReadOnly(dbOptions, cpDir.getAbsolutePath(), cfDescs, handles);

            cfHandle = handles.get(0);
            cleanable = CLEANER.register(this, new ClosableResources(id,
                cpId,
                cpDir,
                cfDesc,
                cfHandle,
                roDB,
                dbOptions,
                isLatest,
                this.logger));
        } catch (RocksDBException e) {
            throw new KVEngineException("Failed to open checkpoint", e);
        }
        this.logger.debug("Checkpoint[{}] of kvspace[{}] created", cpId, id);
    }

    @Override
    public String cpId() {
        return cpId;
    }

    @Override
    protected Optional<ByteString> doMetadata(ByteString metaKey) {
        try {
            byte[] valBytes = roDB.get(cfHandle(), toMetaKey(metaKey));
            if (valBytes == null) {
                return Optional.empty();
            }
            return Optional.of(unsafeWrap(valBytes));
        } catch (RocksDBException e) {
            throw new KVEngineException("Failed to read metadata", e);
        }
    }

    @Override
    public void close() {
        cleanable.clean();
    }

    @Override
    protected RocksDB db() {
        return roDB;
    }

    @Override
    protected ColumnFamilyHandle cfHandle() {
        return cfHandle;
    }

    @Override
    protected ISyncContext.IRefresher newRefresher() {
        return new ISyncContext.IRefresher() {

            @Override
            public void runIfNeeded(Runnable runnable) {
                // no need to do any refresh, since it's readonly
            }

            @Override
            public <T> T call(Supplier<T> supplier) {
                return supplier.get();
            }
        };
    }

    private record ClosableResources(
        String id,
        String cpId,
        File cpDir,
        ColumnFamilyDescriptor cfDesc,
        ColumnFamilyHandle cfHandle,
        RocksDB roDB,
        DBOptions dbOptions,
        Predicate<String> isLatest,
        Logger log
    ) implements Runnable {
        @Override
        public void run() {
            log.debug("Clean up checkpoint[{}] of kvspace[{}]", cpId, id);
            roDB.destroyColumnFamilyHandle(cfHandle);
            cfDesc.getOptions().close();

            roDB.close();
            dbOptions.close();

            if (!isLatest.test(cpId)) {
                log.debug("delete checkpoint[{}] of kvspace[{}] in path: {}", cpId, id, cpDir.getAbsolutePath());
                try {
                    deleteDir(cpDir.toPath());
                } catch (IOException e) {
                    log.error("Failed to clean checkpoint at path:{}", cpDir);
                }
            }
        }
    }
}
