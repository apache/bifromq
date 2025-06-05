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
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.fromDataKey;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.toDataKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.endKeyBytes;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.startKeyBytes;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;
import java.lang.ref.Cleaner;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.Snapshot;

class RocksDBKVSpaceIterator implements IKVSpaceIterator {
    private static final Cleaner CLEANER = Cleaner.create();
    private final RocksDBKVEngineIterator rocksItr;
    private final ISyncContext.IRefresher refresher;
    private final Cleaner.Cleanable onClose;
    public RocksDBKVSpaceIterator(RocksDB db,
                                  ColumnFamilyHandle cfHandle,
                                  Boundary boundary,
                                  ISyncContext.IRefresher refresher) {
        this(db, cfHandle, null, boundary, refresher);
    }

    public RocksDBKVSpaceIterator(RocksDB db,
                                  ColumnFamilyHandle cfHandle,
                                  Snapshot snapshot,
                                  Boundary boundary,
                                  ISyncContext.IRefresher refresher) {
        byte[] startKey = startKeyBytes(boundary);
        byte[] endKey = endKeyBytes(boundary);
        startKey = startKey != null ? toDataKey(startKey) : DATA_SECTION_START;
        endKey = endKey != null ? toDataKey(endKey) : DATA_SECTION_END;
        this.rocksItr = new RocksDBKVEngineIterator(db, cfHandle, snapshot, startKey, endKey);
        this.refresher = refresher;
        onClose = CLEANER.register(this, new State(rocksItr));
    }

    @Override
    public ByteString key() {
        return fromDataKey(rocksItr.key());
    }

    @Override
    public ByteString value() {
        return unsafeWrap(rocksItr.value());
    }

    @Override
    public boolean isValid() {
        return rocksItr.isValid();
    }

    @Override
    public void next() {
        rocksItr.next();
    }

    @Override
    public void prev() {
        rocksItr.prev();
    }

    @Override
    public void seekToFirst() {
        rocksItr.seekToFirst();
    }

    @Override
    public void seekToLast() {
        rocksItr.seekToLast();
    }

    @Override
    public void seek(ByteString target) {
        rocksItr.seek(toDataKey(target));
    }

    @Override
    public void seekForPrev(ByteString target) {
        rocksItr.seekForPrev(toDataKey(target));
    }

    @Override
    public void refresh() {
        refresher.runIfNeeded(rocksItr::refresh);
    }

    @Override
    public void close() {
        onClose.clean();
    }

    private record State(RocksDBKVEngineIterator rocksItr) implements Runnable {
        @Override
        public void run() {
            rocksItr.close();
        }
    }
}
