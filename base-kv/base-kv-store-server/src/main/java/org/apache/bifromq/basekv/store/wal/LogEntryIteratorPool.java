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

package org.apache.bifromq.basekv.store.wal;

import static org.apache.bifromq.basekv.store.wal.KVRangeWALKeys.logEntryKey;

import org.apache.bifromq.baseenv.ZeroCopyParser;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.IWALableKVSpace;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.store.exception.KVRangeStoreException;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Manages a pool of LogEntryIterators to avoid the overhead of creating new IKVSpaceIterators each time.
 */
class LogEntryIteratorPool {
    private final IWALableKVSpace kvSpace;
    private final Queue<PooledLogEntryIterator> pool = new ConcurrentLinkedQueue<>();

    LogEntryIteratorPool(IWALableKVSpace kvSpace) {
        this.kvSpace = kvSpace;
    }

    /**
     * Acquires a new iterator from the pool or creates a new one if the pool is empty.
     *
     * @param startIndex         the starting index (inclusive)
     * @param endIndex           the ending index (exclusive)
     * @param maxSize            maximum accumulated size of data to be read
     * @param logEntriesKeyInfix current log entries key infix
     * @return an iterator over LogEntry objects
     */
    Iterator<LogEntry> acquire(long startIndex, long endIndex, long maxSize, int logEntriesKeyInfix) {
        PooledLogEntryIterator it = pool.poll();
        if (it == null) {
            it = new PooledLogEntryIterator(kvSpace.newIterator(), this);
        }
        it.refresh(startIndex, endIndex, maxSize, logEntriesKeyInfix);
        return it;
    }

    /**
     * Release the iterator back to the pool.
     */
    void release(PooledLogEntryIterator it) {
        pool.offer(it);
    }

    /**
     * A pooled LogEntryIterator implementation that implements Iterator<LogEntry>.
     */
    static class PooledLogEntryIterator implements Iterator<LogEntry> {
        private final IKVSpaceIterator iterator;
        private final LogEntryIteratorPool pool;

        private long endIndex;
        private long maxSize;
        private long currentIndex;
        private long accumulatedSize;
        private boolean released;

        PooledLogEntryIterator(IKVSpaceIterator iterator, LogEntryIteratorPool pool) {
            this.iterator = iterator;
            this.pool = pool;
        }

        private void refresh(long startIndex, long endIndex, long maxSize, int logEntriesKeyInfix) {
            this.endIndex = endIndex;
            this.maxSize = maxSize;
            this.currentIndex = startIndex;
            this.accumulatedSize = 0;
            this.released = false;

            ByteString startKey = logEntryKey(logEntriesKeyInfix, startIndex);

            iterator.refresh();
            iterator.seek(startKey);
        }

        @Override
        public boolean hasNext() {
            if (released) {
                return false;
            }
            if (currentIndex >= endIndex || accumulatedSize > maxSize || !iterator.isValid()) {
                releaseIfNotAlready();
                return false;
            }
            return true;
        }

        @Override
        public LogEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                ByteString value = iterator.value();
                currentIndex++;
                LogEntry entry = ZeroCopyParser.parse(value, LogEntry.parser());
                accumulatedSize += entry.getData().size();
                iterator.next();
                return entry;
            } catch (Throwable e) {
                throw new KVRangeStoreException("Log data corruption", e);
            }
        }

        private void releaseIfNotAlready() {
            if (!released) {
                released = true;
                pool.release(this);
            }
        }
    }
}
