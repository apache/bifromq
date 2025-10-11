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

import java.lang.ref.Cleaner;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Snapshot;

class RocksDBKVEngineIterator implements AutoCloseable {
    private static final Cleaner CLEANER = Cleaner.create();
    private final RocksIterator rocksIterator;
    private final Cleaner.Cleanable onClose;
    // keep a strong reference to dbHandle to prevent it from being closed before iterator
    private final IKVSpaceDBInstance dbHandle;

    RocksDBKVEngineIterator(IKVSpaceDBInstance dbHandle, Snapshot snapshot, byte[] startKey, byte[] endKey) {
        this(dbHandle, snapshot, startKey, endKey, true);
    }

    RocksDBKVEngineIterator(IKVSpaceDBInstance dbHandle, Snapshot snapshot, byte[] startKey, byte[] endKey, boolean fillCache) {
        this.dbHandle = dbHandle;
        ReadOptions readOptions = new ReadOptions().setPinData(true).setFillCache(fillCache);
        Slice lowerSlice = null;
        if (startKey != null) {
            lowerSlice = new Slice(startKey);
            readOptions.setIterateLowerBound(lowerSlice);
        }
        Slice upperSlice = null;
        if (endKey != null) {
            upperSlice = new Slice(endKey);
            readOptions.setIterateUpperBound(upperSlice);
        }
        if (snapshot != null) {
            readOptions.setSnapshot(snapshot);
        }
        rocksIterator = dbHandle.db().newIterator(dbHandle.cf(), readOptions);
        onClose = CLEANER.register(this, new NativeState(rocksIterator, readOptions, lowerSlice, upperSlice));

    }

    public byte[] key() {
        return rocksIterator.key();
    }

    public byte[] value() {
        return rocksIterator.value();
    }

    public boolean isValid() {
        return rocksIterator.isValid();
    }

    public void next() {
        rocksIterator.next();
    }

    public void prev() {
        rocksIterator.prev();
    }

    public void seekToFirst() {
        rocksIterator.seekToFirst();
    }

    public void seekToLast() {
        rocksIterator.seekToLast();
    }

    public void seek(byte[] target) {
        rocksIterator.seek(target);
    }

    public void seekForPrev(byte[] target) {
        rocksIterator.seekForPrev(target);
    }

    public void refresh() {
        try {
            rocksIterator.refresh();
        } catch (Throwable e) {
            throw new KVEngineException("Unable to refresh iterator", e);
        }
    }

    @Override
    public void close() {
        onClose.clean();
    }

    private record NativeState(RocksIterator itr, ReadOptions readOptions, Slice lowerSlice, Slice upperSlice)
        implements Runnable {

        @Override
        public void run() {
            itr.close();
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
