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
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.DATA_SECTION_END;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.DATA_SECTION_START;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.fromDataKey;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.toDataKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.endKeyBytes;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.startKeyBytes;

import com.google.protobuf.ByteString;
import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.proto.Boundary;

@Slf4j
class RocksDBKVSpaceIterator implements IKVSpaceIterator {
    private static final Cleaner CLEANER = Cleaner.create();
    private final Supplier<IKVSpaceDBInstance> dbHandleProvider;
    private final ISyncContext.IRefresher refresher;
    private final byte[] startKey;
    private final byte[] endKey;
    private final boolean fillCache;
    private final AtomicReference<RocksItrHolder> rocksItrHolder = new AtomicReference<>();

    public RocksDBKVSpaceIterator(Supplier<IKVSpaceDBInstance> dbHandleProvider,
                                  Boundary boundary,
                                  ISyncContext.IRefresher refresher) {
        this(dbHandleProvider, boundary, refresher, true);
    }

    public RocksDBKVSpaceIterator(Supplier<IKVSpaceDBInstance> dbHandleProvider,
                                  Boundary boundary,
                                  ISyncContext.IRefresher refresher,
                                  boolean fillCache) {
        this.dbHandleProvider = dbHandleProvider;
        byte[] boundaryStartKey = startKeyBytes(boundary);
        byte[] boundaryEndKey = endKeyBytes(boundary);
        startKey = boundaryStartKey != null ? toDataKey(boundaryStartKey) : DATA_SECTION_START;
        endKey = boundaryEndKey != null ? toDataKey(boundaryEndKey) : DATA_SECTION_END;
        this.fillCache = fillCache;
        this.refresher = refresher;
        this.rocksItrHolder.set(newRocksItrHolder());
    }

    @Override
    public ByteString key() {
        return fromDataKey(this.rocksItrHolder.get().rocksItr.key());
    }

    @Override
    public ByteString value() {
        return unsafeWrap(this.rocksItrHolder.get().rocksItr.value());
    }

    @Override
    public boolean isValid() {
        return this.rocksItrHolder.get().rocksItr.isValid();
    }

    @Override
    public void next() {
        this.rocksItrHolder.get().rocksItr.next();
    }

    @Override
    public void prev() {
        this.rocksItrHolder.get().rocksItr.prev();
    }

    @Override
    public void seekToFirst() {
        this.rocksItrHolder.get().rocksItr.seekToFirst();
    }

    @Override
    public void seekToLast() {
        this.rocksItrHolder.get().rocksItr.seekToLast();
    }

    @Override
    public void seek(ByteString target) {
        this.rocksItrHolder.get().rocksItr.seek(toDataKey(target));
    }

    @Override
    public void seekForPrev(ByteString target) {
        this.rocksItrHolder.get().rocksItr.seekForPrev(toDataKey(target));
    }

    @Override
    public void refresh() {
        refresher.runIfNeeded((genBumped) -> {
            // create new iterator if gen bumped
            if (genBumped) {
                RocksItrHolder old = this.rocksItrHolder.getAndSet(newRocksItrHolder());
                if (old != null) {
                    old.onClose.clean();
                }
            } else {
                this.rocksItrHolder.get().rocksItr.refresh();
            }
        });
    }

    @Override
    public void close() {
        this.rocksItrHolder.get().onClose.clean();
    }

    private RocksItrHolder newRocksItrHolder() {
        RocksDBKVEngineIterator rocksItr = new RocksDBKVEngineIterator(dbHandleProvider.get(), null,
            startKey, endKey, fillCache);
        Cleaner.Cleanable onClose = CLEANER.register(this, new State(rocksItr));
        return new RocksItrHolder(rocksItr, onClose);
    }

    private record RocksItrHolder(RocksDBKVEngineIterator rocksItr, Cleaner.Cleanable onClose) {
    }

    private record State(RocksDBKVEngineIterator rocksItr) implements Runnable {
        @Override
        public void run() {
            rocksItr.close();
        }
    }
}
