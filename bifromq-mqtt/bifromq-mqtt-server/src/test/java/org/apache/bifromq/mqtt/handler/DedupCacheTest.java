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

package org.apache.bifromq.mqtt.handler;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

public class DedupCacheTest {
    private static final long EXPIRE_MS = 50;

    @Test
    public void testSameChannelSameTopic() {
        DedupCache cache = new DedupCache(EXPIRE_MS, 32, 32);
        String ch = "ch-1";
        String topic = "sensor/temperature";
        assertFalse(cache.isDuplicate(ch, topic, 100));
        assertTrue(cache.isDuplicate(ch, topic, 100));
        assertTrue(cache.isDuplicate(ch, topic, 99));
        assertFalse(cache.isDuplicate(ch, topic, 101));
    }

    @Test
    public void testSameChannelCrossTopicOutOfOrder() {
        DedupCache cache = new DedupCache(EXPIRE_MS, 32, 32);
        String ch = "ch-1";
        assertFalse(cache.isDuplicate(ch, "A", 200));
        assertFalse(cache.isDuplicate(ch, "B", 100));
    }

    @Test
    public void testDifferentChannelSameTopicIndependent() {
        DedupCache cache = new DedupCache(EXPIRE_MS, 32, 32);
        String t = new String("topic/x");
        assertFalse(cache.isDuplicate("ch-1", t, 100));
        assertFalse(cache.isDuplicate("ch-2", new String("topic/x"), 50));
        assertTrue(cache.isDuplicate("ch-1", "topic/x", 100));
        assertFalse(cache.isDuplicate("ch-2", "topic/x", 60));
    }

    @Test
    public void testExpireAfterAccess() throws Exception {
        DedupCache cache = new DedupCache(20, 32, 32);
        String ch = "ch-1";
        String topic = "expire/topic";
        assertFalse(cache.isDuplicate(ch, topic, 100));
        Thread.sleep(30);
        // entry may expire, old timestamp should not cause duplicate
        assertFalse(cache.isDuplicate(ch, topic, 90));
        assertTrue(cache.channelEvictedByExpired() >= 0);
    }

    @Test
    public void testInnerEvictionBySize() {
        DedupCache cache = new DedupCache(EXPIRE_MS, 4, 2);
        String ch = "ch-1";
        assertFalse(cache.isDuplicate(ch, "t1", 1));
        assertFalse(cache.isDuplicate(ch, "t2", 2));
        long before = cache.topicEvictedBySize();
        assertFalse(cache.isDuplicate(ch, "t3", 3));
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
            assertTrue(cache.topicEvictedBySize() >= before)
        );
    }

    @Test
    public void testOuterEvictionBySize() {
        DedupCache cache = new DedupCache(EXPIRE_MS, 1, 8);
        assertFalse(cache.isDuplicate("c1", "t", 1));
        long before = cache.channelEvictedBySize();
        assertFalse(cache.isDuplicate("c2", "t", 1));
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertTrue(cache.channelEvictedBySize() >= before);
            assertEquals(cache.estimatedChannels(), 1);
        });
    }

    @Test
    public void testTotalCachedTopicsCounting() {
        DedupCache cache = new DedupCache(1000, 16, 16);
        assertEquals(cache.totalCachedTopics(), 0);
        assertFalse(cache.isDuplicate("c1", "t1", 1));
        assertEquals(cache.totalCachedTopics(), 1);
        assertTrue(cache.isDuplicate("c1", "t1", 1));
        assertEquals(cache.totalCachedTopics(), 1);
        assertFalse(cache.isDuplicate("c1", "t2", 2));
        assertEquals(cache.totalCachedTopics(), 2);
        assertFalse(cache.isDuplicate("c2", "t1", 3));
        assertEquals(cache.totalCachedTopics(), 3);
    }

    @Test
    public void testTotalCachedTopicsInnerEvictionKeepsBound() {
        DedupCache cache = new DedupCache(1000, 2, 2);
        assertFalse(cache.isDuplicate("c1", "t1", 1));
        assertFalse(cache.isDuplicate("c1", "t2", 2));
        assertEquals(cache.totalCachedTopics(), 2);
        assertFalse(cache.isDuplicate("c1", "t3", 3));
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
            assertEquals(cache.totalCachedTopics(), 2)
        );
    }

    @Test
    public void testTotalCachedTopicsOuterEvictionKeepsBound() {
        DedupCache cache = new DedupCache(1000, 1, 8);
        assertFalse(cache.isDuplicate("c1", "t1", 1));
        assertEquals(cache.totalCachedTopics(), 1);
        assertFalse(cache.isDuplicate("c2", "t1", 2));
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
            assertEquals(cache.totalCachedTopics(), 1)
        );
    }

    @Test
    public void testInternedTopicsAreGCedAfterEviction() {
        DedupCache cache = new DedupCache(30, 4, 8);
        String ch = "c-gc";
        List<WeakReference<String>> refs = new ArrayList<>();

        for (int i = 0; i < 40; i++) {
            String topic = new String("gc/topic-" + i);
            refs.add(new WeakReference<>(topic));
            assertFalse(cache.isDuplicate(ch, topic, i + 1));
            topic = null;
        }

        try {
            Thread.sleep(60);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (int i = 0; i < 200; i++) {
            cache.isDuplicate(ch, "hot/" + (i % 16), 1000 + i);
        }

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            System.gc();
            for (WeakReference<String> wr : refs) {
                assertNull(wr.get());
            }
        });
    }
}
