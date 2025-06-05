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

import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.toDataKey;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.toMetaKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.isValid;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import org.apache.bifromq.basekv.localengine.AbstractKVSpaceReader;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;
import java.lang.ref.Cleaner;
import java.util.Optional;
import java.util.function.Supplier;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.slf4j.Logger;

class RocksDBKVSpaceSnapshot extends AbstractKVSpaceReader implements IRocksDBKVSpaceCheckpoint {
    private static final Cleaner CLEANER = Cleaner.create();
    private static final ISyncContext.IRefresher DUMMY_REFRESHER = new ISyncContext.IRefresher() {
        @Override
        public void runIfNeeded(Runnable runnable) {
            // no need to do any refresh, since it's readonly
        }

        @Override
        public <T> T call(Supplier<T> supplier) {
            return supplier.get();
        }
    };
    private final Snapshot snapshot;
    private final ColumnFamilyHandle cfHandle;
    private final RocksDB db;
    private final ReadOptions readOptions;
    private final Cleaner.Cleanable cleanable;

    RocksDBKVSpaceSnapshot(String id,
                           Snapshot snapshot,
                           ColumnFamilyHandle cfHandle,
                           RocksDB db,
                           KVSpaceOpMeters readOpMeters,
                           Logger logger) {
        super(id, readOpMeters, logger);
        this.snapshot = snapshot;
        this.cfHandle = cfHandle;
        this.db = db;
        this.readOptions = new ReadOptions().setSnapshot(snapshot);
        cleanable = CLEANER.register(this, new ClosableResources(readOptions, snapshot, db));
    }

    @Override
    public String cpId() {
        return Long.toUnsignedString(snapshot.getSequenceNumber());
    }

    @Override
    public void close() {
        cleanable.clean();
    }

    @Override
    protected Optional<ByteString> doMetadata(ByteString metaKey) {
        try {
            byte[] valBytes = db.get(cfHandle, readOptions, toMetaKey(metaKey));
            if (valBytes == null) {
                return Optional.empty();
            }
            return Optional.of(unsafeWrap(valBytes));
        } catch (RocksDBException e) {
            throw new KVEngineException("Failed to read metadata", e);
        }
    }

    @Override
    protected long doSize(Boundary boundary) {
        throw new UnsupportedOperationException("Getting size of snapshot is unsupported");
    }

    @Override
    protected boolean doExist(ByteString key) {
        return get(key).isPresent();
    }

    @Override
    protected Optional<ByteString> doGet(ByteString key) {
        try {
            byte[] data = db.get(cfHandle, readOptions, toDataKey(key));
            return Optional.ofNullable(data == null ? null : unsafeWrap(data));
        } catch (RocksDBException rocksDBException) {
            throw new KVEngineException("Get failed", rocksDBException);
        }
    }

    @Override
    protected IKVSpaceIterator doNewIterator() {
        return new RocksDBKVSpaceIterator(db, cfHandle, snapshot, Boundary.getDefaultInstance(), DUMMY_REFRESHER
        );
    }

    @Override
    protected IKVSpaceIterator doNewIterator(Boundary subBoundary) {
        assert isValid(subBoundary);
        return new RocksDBKVSpaceIterator(db, cfHandle, snapshot, subBoundary, DUMMY_REFRESHER);
    }

    private record ClosableResources(ReadOptions readOptions, Snapshot snapshot, RocksDB db) implements Runnable {
        @Override
        public void run() {
            readOptions.close();
            db.releaseSnapshot(snapshot);
        }
    }
}
