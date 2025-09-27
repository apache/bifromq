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

package org.apache.bifromq.inbox.store;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.bufferedMsgKey;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.inboxInstanceStartKey;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.inboxStartKeyPrefix;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.qos0MsgKey;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVReader;
import org.apache.bifromq.basekv.store.util.KVUtil;
import org.apache.bifromq.inbox.storage.proto.InboxMetadata;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InboxMetaCacheTest {
    private static final java.util.Comparator<ByteString> COMPARATOR =
        ByteString.unsignedLexicographicalComparator();

    private InboxMetaCache cache;
    private FakeKVReader reader;

    @BeforeMethod
    public void setup() {
        cache = new InboxMetaCache(Duration.ofSeconds(60));
        reader = new FakeKVReader();
    }

    @Test
    public void loadAndCacheHit() {
        String tenant = "t1";
        String inbox = "i1";

        putInstance(tenant, inbox, 1);
        putInstance(tenant, inbox, 2);
        ByteString instPrefix = inboxInstanceStartKey(tenant, inbox, 1);
        reader.put(qos0MsgKey(instPrefix, 1), KVUtil.toByteString(1));
        reader.put(bufferedMsgKey(instPrefix, 2), KVUtil.toByteString(2));

        SortedMap<Long, InboxMetadata> map1 = cache.get(tenant, inbox, reader);
        assertEquals(map1.size(), 2);
        assertEquals(reader.refreshCount, 1);

        SortedMap<Long, InboxMetadata> map2 = cache.get(tenant, inbox, reader);
        assertSame(map1, map2);
        assertEquals(reader.refreshCount, 1);
    }

    @Test
    public void getByIncarnation() {
        String tenant = "t1";
        String inbox = "i1";
        putInstance(tenant, inbox, 100);
        putInstance(tenant, inbox, 200);

        Optional<InboxMetadata> m100 = cache.get(tenant, inbox, 100, reader);
        Optional<InboxMetadata> m300 = cache.get(tenant, inbox, 300, reader);
        assertTrue(m100.isPresent());
        assertFalse(m300.isPresent());
    }

    @Test
    public void upsertAndRemove() {
        String tenant = "t1";
        String inbox = "i1";

        InboxMetadata meta1 = InboxMetadata.newBuilder().setInboxId(inbox).setIncarnation(1).build();
        assertTrue(cache.upsert(tenant, meta1, reader)); // first entry
        assertEquals(cache.get(tenant, inbox, reader).size(), 1);

        InboxMetadata meta2 = InboxMetadata.newBuilder().setInboxId(inbox).setIncarnation(2).build();
        assertFalse(cache.upsert(tenant, meta2, reader));
        assertEquals(cache.get(tenant, inbox, reader).size(), 2);

        assertFalse(cache.remove(tenant, inbox, 1, reader));
        assertEquals(cache.get(tenant, inbox, reader).size(), 1);

        assertTrue(cache.remove(tenant, inbox, 2, reader)); // map becomes empty and invalidated
        reader.refreshCount = 0;
        cache.get(tenant, inbox, reader);
        assertEquals(reader.refreshCount, 1);
    }

    @Test
    public void resetInvalidatesOutsideBoundary() {
        String tenant = "t1";
        String inbox1 = "i1";
        String inbox2 = "i2";
        putInstance(tenant, inbox1, 1);
        putInstance(tenant, inbox2, 2);

        cache.get(tenant, inbox1, reader);
        cache.get(tenant, inbox2, reader);
        reader.refreshCount = 0;

        // boundary only covers inbox1 prefix, so inbox2 should be invalidated
        ByteString start = inboxStartKeyPrefix(tenant, inbox1);
        ByteString end = start.concat(ByteString.copyFrom(new byte[] {(byte) 0xFF}));
        Boundary boundary = toBoundary(start, end);
        cache.reset(boundary);

        // inbox1 should still be cached
        cache.get(tenant, inbox1, reader);
        assertEquals(reader.refreshCount, 0);

        // inbox2 should be reloaded (invalidated)
        cache.get(tenant, inbox2, reader);
        assertEquals(reader.refreshCount, 1);
    }

    @Test
    public void probeJumpSeekPath() {
        String tenant = "t1";
        String inbox = "i1";
        // 2 instances
        ByteString inst1 = putInstance(tenant, inbox, 1);
        putInstance(tenant, inbox, 2);
        // add >20 non-start keys, including inbox-instance keys to trigger jump
        for (int i = 0; i < 25; i++) {
            if (i % 2 == 0) {
                reader.put(qos0MsgKey(inst1, i + 1), KVUtil.toByteString(i + 1));
            } else {
                reader.put(bufferedMsgKey(inst1, i + 1), KVUtil.toByteString(i + 1));
            }
        }

        cache.get(tenant, inbox, reader);
        assertTrue(reader.iter.seekCount.get() > 0);
    }

    @Test
    public void skipBadValue() {
        String tenant = "t1";
        String inbox = "i1";
        // good instance
        putInstance(tenant, inbox, 1);
        // bad instance value at incarnation=2
        ByteString key = inboxInstanceStartKey(tenant, inbox, 2);
        reader.put(key, ByteString.copyFromUtf8("bad"));

        SortedMap<Long, InboxMetadata> map = cache.get(tenant, inbox, reader);
        assertEquals(map.size(), 1);
        assertTrue(map.containsKey(1L));
    }

    private ByteString putInstance(String tenant, String inbox, long incarnation) {
        ByteString key = inboxInstanceStartKey(tenant, inbox, incarnation);
        InboxMetadata meta = InboxMetadata.newBuilder()
            .setInboxId(inbox)
            .setIncarnation(incarnation)
            .build();
        reader.put(key, meta.toByteString());
        return key;
    }

    private static class FakeKVReader implements IKVReader {
        private final NavigableMap<ByteString, ByteString> data =
            new ConcurrentSkipListMap<>(COMPARATOR);
        int refreshCount = 0;
        FakeKVIterator iter;
        private Boundary boundary = Boundary.getDefaultInstance();

        void put(ByteString key, ByteString value) {
            data.put(key, value);
        }

        @Override
        public Boundary boundary() {
            return boundary;
        }

        @Override
        public long size(Boundary boundary) {
            return data.size();
        }

        @Override
        public boolean exist(ByteString key) {
            return data.containsKey(key);
        }

        @Override
        public Optional<ByteString> get(ByteString key) {
            return Optional.ofNullable(data.get(key));
        }

        @Override
        public IKVIterator iterator() {
            iter = new FakeKVIterator(data);
            return iter;
        }

        @Override
        public void refresh() {
            refreshCount++;
        }
    }

    private static class FakeKVIterator implements IKVIterator {
        private final NavigableMap<ByteString, ByteString> data;
        private final java.util.concurrent.atomic.AtomicInteger seekCount = new java.util.concurrent.atomic.AtomicInteger();
        private NavigableMap.Entry<ByteString, ByteString> cursor;

        FakeKVIterator(NavigableMap<ByteString, ByteString> data) {
            this.data = data;
        }

        @Override
        public ByteString key() {
            return cursor == null ? null : cursor.getKey();
        }

        @Override
        public ByteString value() {
            return cursor == null ? null : cursor.getValue();
        }

        @Override
        public boolean isValid() {
            return cursor != null;
        }

        @Override
        public void next() {
            if (cursor == null) {
                cursor = data.firstEntry();
            } else {
                cursor = data.higherEntry(cursor.getKey());
            }
        }

        @Override
        public void prev() {
            if (cursor == null) {
                cursor = data.lastEntry();
            } else {
                cursor = data.lowerEntry(cursor.getKey());
            }
        }

        @Override
        public void seekToFirst() {
            cursor = data.firstEntry();
            seekCount.incrementAndGet();
        }

        @Override
        public void seekToLast() {
            cursor = data.lastEntry();
            seekCount.incrementAndGet();
        }

        @Override
        public void seek(ByteString key) {
            cursor = data.ceilingEntry(key);
            seekCount.incrementAndGet();
        }

        @Override
        public void seekForPrev(ByteString key) {
            cursor = data.floorEntry(key);
            seekCount.incrementAndGet();
        }
    }
}
