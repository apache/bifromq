/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.basekv.localengine.rocksdb;

import org.apache.bifromq.basekv.localengine.KVEngineException;
import java.lang.ref.Cleaner;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Snapshot;

class RocksDBKVEngineIterator implements AutoCloseable {
    private static final Cleaner CLEANER = Cleaner.create();

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

    private final RocksIterator rocksIterator;
    private final Cleaner.Cleanable onClose;

    RocksDBKVEngineIterator(RocksDB db,
                            ColumnFamilyHandle cfHandle,
                            Snapshot snapshot,
                            byte[] startKey,
                            byte[] endKey) {
        ReadOptions readOptions = new ReadOptions();
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
        rocksIterator = db.newIterator(cfHandle, readOptions);
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
}
