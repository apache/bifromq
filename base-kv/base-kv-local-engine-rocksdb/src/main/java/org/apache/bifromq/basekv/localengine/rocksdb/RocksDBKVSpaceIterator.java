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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.proto.Boundary;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

@Slf4j
class RocksDBKVSpaceIterator implements IKVSpaceIterator {
    private final byte[] startKey;
    private final byte[] endKey;
    private final IteratorOptions options;
    private final AtomicReference<RocksDBItrHolder> rocksItrHolder = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CloseListener closeListener;

    public RocksDBKVSpaceIterator(RocksDBSnapshot snapshot, Boundary boundary, IteratorOptions options) {
        this(snapshot, boundary, itr -> {
        }, options);
    }

    public RocksDBKVSpaceIterator(RocksDBSnapshot snapshot,
                                  Boundary boundary,
                                  CloseListener closeListener,
                                  IteratorOptions options) {
        byte[] boundaryStartKey = startKeyBytes(boundary);
        byte[] boundaryEndKey = endKeyBytes(boundary);
        startKey = boundaryStartKey != null ? toDataKey(boundaryStartKey) : DATA_SECTION_START;
        endKey = boundaryEndKey != null ? toDataKey(boundaryEndKey) : DATA_SECTION_END;
        this.options = options;
        this.closeListener = closeListener;
        refresh(snapshot);
    }

    @Override
    public ByteString key() {
        return fromDataKey(rocksItrHolder.get().rocksIterator.key());
    }

    @Override
    public ByteString value() {
        return unsafeWrap(rocksItrHolder.get().rocksIterator.value());
    }

    @Override
    public boolean isValid() {
        return rocksItrHolder.get().rocksIterator.isValid();
    }

    @Override
    public void next() {
        rocksItrHolder.get().rocksIterator.next();
    }

    @Override
    public void prev() {
        rocksItrHolder.get().rocksIterator.prev();
    }

    @Override
    public void seekToFirst() {
        rocksItrHolder.get().rocksIterator.seekToFirst();
    }

    @Override
    public void seekToLast() {
        rocksItrHolder.get().rocksIterator.seekToLast();
    }

    @Override
    public void seek(ByteString target) {
        rocksItrHolder.get().rocksIterator.seek(toDataKey(target));
    }

    @Override
    public void seekForPrev(ByteString target) {
        rocksItrHolder.get().rocksIterator.seekForPrev(toDataKey(target));
    }

    public void refresh(RocksDBSnapshot snapshot) {
        if (closed.get()) {
            return;
        }
        try {
            RocksDBItrHolder rocksItrHolder = this.rocksItrHolder.get();
            if (rocksItrHolder == null) {
                this.rocksItrHolder.set(build(snapshot));
            } else if (rocksItrHolder.epoch == snapshot.epoch()) {
                // same epoch, just refresh the snapshot
                rocksItrHolder.rocksIterator.refresh(snapshot.snapshot());
            } else {
                this.rocksItrHolder.set(build(snapshot));
                rocksItrHolder.close();
            }
        } catch (Throwable e) {
            throw new KVEngineException("Unable to refresh iterator", e);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                RocksDBItrHolder rocksItrHolder = this.rocksItrHolder.getAndSet(null);
                if (rocksItrHolder != null) {
                    rocksItrHolder.close();
                }
            } finally {
                closeListener.onClose(this);
            }
        }
    }

    private RocksDBItrHolder build(RocksDBSnapshot snapshot) {
        ReadOptions readOptions = new ReadOptions()
            .setPinData(true)
            .setFillCache(options.fillCache())
            .setReadaheadSize(options.readAheadSize())
            .setAutoPrefixMode(true);
        Slice lowerSlice = new Slice(startKey);
        readOptions.setIterateLowerBound(lowerSlice);
        Slice upperSlice = new Slice(endKey);
        readOptions.setIterateUpperBound(upperSlice);
        if (snapshot != null) {
            readOptions.setSnapshot(snapshot.snapshot());
        }
        RocksIterator rocksItr = snapshot.epoch().db().newIterator(snapshot.epoch().cf(), readOptions);
        return new RocksDBItrHolder(snapshot.epoch(), rocksItr, readOptions, lowerSlice, upperSlice);
    }

    interface CloseListener {
        void onClose(RocksDBKVSpaceIterator itr);
    }

    /**
     * Holder for RocksDB iterator and its resources; close is idempotent.
     */
    private static final class RocksDBItrHolder {
        final IRocksDBKVSpaceEpoch epoch;
        final RocksIterator rocksIterator;
        final ReadOptions readOptions;
        final Slice lowerSlice;
        final Slice upperSlice;
        private final AtomicBoolean closed = new AtomicBoolean();

        RocksDBItrHolder(IRocksDBKVSpaceEpoch epoch,
                         RocksIterator rocksIterator,
                         ReadOptions readOptions,
                         Slice lowerSlice,
                         Slice upperSlice) {
            this.epoch = epoch;
            this.rocksIterator = rocksIterator;
            this.readOptions = readOptions;
            this.lowerSlice = lowerSlice;
            this.upperSlice = upperSlice;
        }

        public void close() {
            // Ensure native resources are freed exactly once
            if (closed.compareAndSet(false, true)) {
                rocksIterator.close();
                readOptions.close();
                if (lowerSlice != null) {
                    lowerSlice.close();
                }
                if (upperSlice != null) {
                    upperSlice.close();
                }
            }
        }
    }
}
