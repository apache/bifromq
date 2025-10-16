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
import static org.testng.Assert.assertEquals;

import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.IKVSpaceRefreshableReader;
import org.apache.bifromq.basekv.localengine.IKVSpaceWriter;
import org.apache.bifromq.basekv.localengine.IWALableKVSpace;
import org.apache.bifromq.basekv.localengine.KVSpaceDescriptor;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.store.exception.KVRangeStoreException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LogEntryIteratorTest {
    private static void put(InMemWALSpace space, int infix, long index, int dataSize) {
        LogEntry entry = LogEntry.newBuilder()
            .setTerm(1)
            .setIndex(index)
            .setData(ByteString.copyFrom(new byte[dataSize]))
            .build();
        space.putRaw(logEntryKey(infix, index), entry.toByteString());
    }

    @Test
    public void testBoundaryNotCrossInfix() {
        InMemWALSpace space = new InMemWALSpace("t");
        int infix0 = 0;
        int infix1 = 1;
        // infix=0 entries
        put(space, infix0, 1, 10);
        put(space, infix0, 2, 10);
        // infix=1 entries
        put(space, infix1, 1, 10);
        put(space, infix1, 2, 10);

        try (LogEntryIterator itr = new LogEntryIterator(space, 1, 10, Long.MAX_VALUE, infix0)) {
            List<Long> indexes = new ArrayList<>();
            while (itr.hasNext()) {
                indexes.add(itr.next().getIndex());
            }
            // Should only read indices from infix0
            assertEquals(indexes, List.of(1L, 2L));
        }
    }

    @Test
    public void testMaxSizeTooSmallReturnsAtLeastOne() {
        InMemWALSpace space = new InMemWALSpace("t");
        int infix = 0;
        put(space, infix, 1, 64);
        put(space, infix, 2, 64);

        try (LogEntryIterator itr = new LogEntryIterator(space, 1, 3, 1, infix)) {
            List<Long> indexes = new ArrayList<>();
            while (itr.hasNext()) {
                indexes.add(itr.next().getIndex());
            }
            // At least the first entry should be returned
            assertEquals(indexes, List.of(1L));
        }
    }

    @Test
    public void testMaxSizeMayExceedTotal() {
        InMemWALSpace space = new InMemWALSpace("t");
        int infix = 0;
        put(space, infix, 1, 60);
        put(space, infix, 2, 60);

        try (LogEntryIterator itr = new LogEntryIterator(space, 1, 3, 100, infix)) {
            List<Long> indexes = new ArrayList<>();
            while (itr.hasNext()) {
                indexes.add(itr.next().getIndex());
            }
            // Two entries returned even though 60+60 > 100
            assertEquals(indexes, List.of(1L, 2L));
        }
    }

    @Test
    public void testEndIndexExclusive() {
        InMemWALSpace space = new InMemWALSpace("t");
        int infix = 0;
        put(space, infix, 1, 10);
        put(space, infix, 2, 10);
        put(space, infix, 3, 10);
        put(space, infix, 4, 10);

        try (LogEntryIterator itr = new LogEntryIterator(space, 2, 4, Long.MAX_VALUE, infix)) {
            List<Long> indexes = new ArrayList<>();
            while (itr.hasNext()) {
                indexes.add(itr.next().getIndex());
            }
            assertEquals(indexes, List.of(2L, 3L));
        }
    }

    @Test(expectedExceptions = KVRangeStoreException.class)
    public void testCorruptedValueThrows() {
        InMemWALSpace space = new InMemWALSpace("t");
        int infix = 0;
        // good entry
        put(space, infix, 1, 10);
        // corrupted bytes that cannot be parsed as a valid protobuf (malformed varint)
        space.putRaw(logEntryKey(infix, 2), ByteString.copyFrom(new byte[] {(byte) 0xFF}));

        try (LogEntryIterator itr = new LogEntryIterator(space, 1, 3, Long.MAX_VALUE, infix)) {
            // first ok
            Assert.assertTrue(itr.hasNext());
            LogEntry e1 = itr.next();
            assertEquals(e1.getIndex(), 1L);
            // second should throw
            itr.next();
        }
    }

    private static class InMemWALSpace implements IWALableKVSpace {
        private final String id;
        private final NavigableMap<ByteString, ByteString> store;

        InMemWALSpace(String id) {
            this.id = id;
            // Use unsigned lexicographical comparator to align with ByteString ordering in production.
            this.store = new TreeMap<>(ByteString.unsignedLexicographicalComparator());
        }

        void putRaw(ByteString key, ByteString value) {
            store.put(Objects.requireNonNull(key), Objects.requireNonNull(value));
        }

        @Override
        public CompletableFuture<Long> flush() {
            // Not used in tests
            return CompletableFuture.completedFuture(System.nanoTime());
        }

        @Override
        public IKVSpaceRefreshableReader reader() {
            // For IWALableKVSpace, the reader() is inherited from IKVSpace (refreshable).
            return new Reader();
        }

        @Override
        public IKVSpaceWriter toWriter() {
            throw new UnsupportedOperationException("Not needed in tests");
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public Observable<Map<ByteString, ByteString>> metadata() {
            return Observable.never();
        }

        @Override
        public KVSpaceDescriptor describe() {
            return new KVSpaceDescriptor(id, Map.of());
        }

        @Override
        public void open() {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public void destroy() {
            store.clear();
        }

        @Override
        public long size() {
            return store.size();
        }

        /**
         * Iterator view over a navigable map with ByteString keys.
         */
        private static class IteratorView implements IKVSpaceIterator {
            private final NavigableMap<ByteString, ByteString> view;
            private Map.Entry<ByteString, ByteString> current;

            IteratorView(NavigableMap<ByteString, ByteString> view) {
                this.view = view;
                seekToFirst();
            }

            @Override
            public ByteString key() {
                return current.getKey();
            }

            @Override
            public ByteString value() {
                return current.getValue();
            }

            @Override
            public boolean isValid() {
                return current != null;
            }

            @Override
            public void next() {
                if (current == null) {
                    return;
                }
                current = view.higherEntry(current.getKey());
            }

            @Override
            public void prev() {
                if (current == null) {
                    return;
                }
                current = view.lowerEntry(current.getKey());
            }

            @Override
            public void seekToFirst() {
                current = view.isEmpty() ? null : view.firstEntry();
            }

            @Override
            public void seekToLast() {
                current = view.isEmpty() ? null : view.lastEntry();
            }

            @Override
            public void seek(ByteString target) {
                current = view.ceilingEntry(target);
            }

            @Override
            public void seekForPrev(ByteString target) {
                current = view.floorEntry(target);
            }

            @Override
            public void close() {
                current = null;
            }
        }

        private class Reader implements IKVSpaceRefreshableReader {
            @Override
            public Optional<ByteString> metadata(ByteString metaKey) {
                return Optional.empty();
            }

            @Override
            public boolean exist(ByteString key) {
                return store.containsKey(key);
            }

            @Override
            public Optional<ByteString> get(ByteString key) {
                return Optional.ofNullable(store.get(key));
            }

            @Override
            public IKVSpaceIterator newIterator() {
                return new IteratorView(store);
            }

            @Override
            public IKVSpaceIterator newIterator(Boundary subBoundary) {
                // Apply [start, end) semantics consistent with production readers.
                NavigableMap<ByteString, ByteString> view = store;
                if (subBoundary.hasStartKey() && subBoundary.hasEndKey()) {
                    view = store.subMap(subBoundary.getStartKey(), true, subBoundary.getEndKey(), false);
                } else if (subBoundary.hasStartKey()) {
                    view = store.tailMap(subBoundary.getStartKey(), true);
                } else if (subBoundary.hasEndKey()) {
                    view = store.headMap(subBoundary.getEndKey(), false);
                }
                return new IteratorView(view);
            }

            @Override
            public long size(Boundary boundary) {
                ByteString start = boundary.hasStartKey() ? boundary.getStartKey() : null;
                ByteString end = boundary.hasEndKey() ? boundary.getEndKey() : null;
                NavigableMap<ByteString, ByteString> view = store;
                if (start != null && end != null) {
                    view = store.subMap(start, true, end, false);
                } else if (start != null) {
                    view = store.tailMap(start, true);
                } else if (end != null) {
                    view = store.headMap(end, false);
                }
                long total = 0;
                for (ByteString v : view.values()) {
                    total += v.size();
                }
                return total;
            }

            @Override
            public String id() {
                return id;
            }

            @Override
            public void close() {
                // no-op
            }

            @Override
            public void refresh() {
                // no-op for in-memory
            }
        }
    }
}
