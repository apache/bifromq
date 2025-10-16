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

import static org.apache.bifromq.basekv.store.wal.KVRangeWALKeys.logEntriesKeyPrefixInfix;
import static org.apache.bifromq.basekv.store.wal.KVRangeWALKeys.logEntryKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;

import com.google.protobuf.ByteString;
import java.util.NoSuchElementException;
import org.apache.bifromq.baseenv.ZeroCopyParser;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.IKVSpaceRefreshableReader;
import org.apache.bifromq.basekv.localengine.IWALableKVSpace;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.raft.ILogEntryIterator;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.store.exception.KVRangeStoreException;

final class LogEntryIterator implements ILogEntryIterator {
    private final IKVSpaceIterator iterator;
    private final long endIndex;
    private final long maxSize;
    private final Runnable cleanable;

    private long currentIndex;
    private long accumulatedSize;

    LogEntryIterator(IWALableKVSpace kvSpace, long startIndex, long endIndex, long maxSize, int logEntriesKeyInfix) {
        IKVSpaceRefreshableReader reader = kvSpace.reader();
        ByteString startBound = logEntriesKeyPrefixInfix(logEntriesKeyInfix);
        ByteString endBound = upperBound(logEntriesKeyPrefixInfix(logEntriesKeyInfix));
        this.iterator = reader.newIterator(Boundary.newBuilder()
            .setStartKey(startBound)
            .setEndKey(endBound)
            .build());
        this.endIndex = endIndex;
        this.maxSize = maxSize;
        this.currentIndex = startIndex;
        this.accumulatedSize = 0;
        ByteString startKey = logEntryKey(logEntriesKeyInfix, startIndex);
        iterator.seek(startKey);
        cleanable = new NativeResource(reader, iterator);
    }

    @Override
    public boolean hasNext() {
        if (currentIndex >= endIndex || accumulatedSize > maxSize || !iterator.isValid()) {
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

    @Override
    public void close() {
        cleanable.run();
    }

    private record NativeResource(IKVSpaceRefreshableReader reader, IKVSpaceIterator itr) implements Runnable {
        @Override
        public void run() {
            itr.close();
            reader.close();
        }
    }
}
