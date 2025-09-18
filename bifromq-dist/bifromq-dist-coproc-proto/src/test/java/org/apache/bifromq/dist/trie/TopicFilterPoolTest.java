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

package org.apache.bifromq.dist.trie;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.github.benmanes.caffeine.cache.Ticker;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TopicFilterPoolTest {

    @BeforeMethod
    public void resetPools() {
        NTopicFilterTrieNode.poolClear();
        STopicFilterTrieNode.poolClear();
        MTopicFilterTrieNode.poolClear();
        // reset ticker back to system
        NTopicFilterTrieNode.setTicker(null);
        STopicFilterTrieNode.setTicker(null);
        MTopicFilterTrieNode.setTicker(null);
    }

    @AfterMethod
    public void cleanup() {
        NTopicFilterTrieNode.poolClear();
        STopicFilterTrieNode.poolClear();
        MTopicFilterTrieNode.poolClear();
    }

    @Test
    public void reusePoolEntryForNTopicNode() {
        TopicTrieNode<String> root = buildLocalTrie();
        TopicTrieNode<String> aNode = root.child("a");

        NTopicFilterTrieNode<String> n1 = NTopicFilterTrieNode.borrow(null, "a", Set.of(aNode));
        int id1 = System.identityHashCode(n1);
        // basic behavior sanity
        assertEquals(n1.levelName(), "a");
        assertTrue(n1.backingTopics().contains(aNode));
        NTopicFilterTrieNode.release(n1);

        NTopicFilterTrieNode<String> n2 = NTopicFilterTrieNode.borrow(null, "a", Set.of(aNode));
        int id2 = System.identityHashCode(n2);
        assertSame(n1, n2);
        assertEquals(id1, id2);
        // re-init takes effect
        assertEquals(n2.levelName(), "a");
        NTopicFilterTrieNode.release(n2);
    }

    @Test
    public void reusePoolEntryForSTopicNode() {
        TopicTrieNode<String> root = buildLocalTrie();
        Set<TopicTrieNode<String>> wildcardMatchables = new HashSet<>();
        for (TopicTrieNode<String> child : root.children().values()) {
            if (child.wildcardMatchable()) {
                wildcardMatchables.add(child);
            }
        }

        STopicFilterTrieNode<String> s1 = STopicFilterTrieNode.borrow(null, wildcardMatchables);
        STopicFilterTrieNode.release(s1);
        STopicFilterTrieNode<String> s2 = STopicFilterTrieNode.borrow(null, wildcardMatchables);
        assertSame(s1, s2);
        STopicFilterTrieNode.release(s2);
    }

    @Test
    public void reusePoolEntryForMTopicNode() {
        TopicTrieNode<String> root = buildLocalTrie();
        Set<TopicTrieNode<String>> wildcardMatchables = new HashSet<>();
        for (TopicTrieNode<String> child : root.children().values()) {
            if (child.wildcardMatchable()) {
                wildcardMatchables.add(child);
            }
        }
        MTopicFilterTrieNode<String> m1 = MTopicFilterTrieNode.borrow(null, wildcardMatchables);
        assertTrue(m1.backingTopics().stream().anyMatch(TopicTrieNode::isUserTopic));
        MTopicFilterTrieNode.release(m1);
        MTopicFilterTrieNode<String> m2 = MTopicFilterTrieNode.borrow(null, wildcardMatchables);
        assertSame(m1, m2);
        MTopicFilterTrieNode.release(m2);
    }

    @Test
    public void ttlExpiryEvictsEntries() {
        FakeTicker ticker = new FakeTicker();
        NTopicFilterTrieNode.setTicker(ticker);
        STopicFilterTrieNode.setTicker(ticker);
        MTopicFilterTrieNode.setTicker(ticker);

        TopicTrieNode<String> root = buildLocalTrie();
        TopicTrieNode<String> aNode = root.child("a");
        Set<TopicTrieNode<String>> wildcardMatchables = new HashSet<>();
        for (TopicTrieNode<String> child : root.children().values()) {
            if (child.wildcardMatchable()) {
                wildcardMatchables.add(child);
            }
        }

        // put one entry in each pool
        NTopicFilterTrieNode.release(NTopicFilterTrieNode.borrow(null, "a", Set.of(aNode)));
        STopicFilterTrieNode.release(STopicFilterTrieNode.borrow(null, wildcardMatchables));
        MTopicFilterTrieNode.release(MTopicFilterTrieNode.borrow(null, wildcardMatchables));

        await().until(() -> NTopicFilterTrieNode.poolApproxSize() == 1
            && STopicFilterTrieNode.poolApproxSize() == 1
            && MTopicFilterTrieNode.poolApproxSize() == 1);

        // advance beyond TTL and force cleanup
        ticker.advance(Duration.ofMinutes(2));
        NTopicFilterTrieNode.poolCleanUp();
        STopicFilterTrieNode.poolCleanUp();
        MTopicFilterTrieNode.poolCleanUp();

        await().until(() -> NTopicFilterTrieNode.poolApproxSize() == 0
            && STopicFilterTrieNode.poolApproxSize() == 0
            && MTopicFilterTrieNode.poolApproxSize() == 0);
    }

    @Test
    public void iteratorBacktraceReleasesNodesNoLeak() {
        TopicTrieNode.Builder<String> builder = TopicTrieNode.builder(false);
        for (int i = 0; i < 10; i++) {
            builder.addTopic(List.of("a" + i), "v" + i);
            for (int j = 0; j < 3; j++) {
                builder.addTopic(List.of("a" + i, "b" + j), "v" + i + j);
            }
        }
        builder.addTopic(List.of("$sys", "x"), "sys");
        TopicFilterIterator<String> itr = new TopicFilterIterator<>();
        itr.init(builder.build());

        for (int round = 0; round < 6; round++) {
            if ((round & 1) == 0) {
                // forward full scan
                itr.seek(List.of());
                for (; itr.isValid(); itr.next()) {
                    // no-op
                }
            } else {
                // backward full scan
                itr.seekPrev(List.of());
                for (; itr.isValid(); itr.prev()) {
                    // no-op
                }
            }
            // after each round, pools should not grow unbounded
            assertTrue(NTopicFilterTrieNode.poolApproxSize() < 256);
            assertTrue(STopicFilterTrieNode.poolApproxSize() < 256);
            assertTrue(MTopicFilterTrieNode.poolApproxSize() < 256);
        }
    }

    private TopicTrieNode<String> buildLocalTrie() {
        return TopicTrieNode.<String>builder(false)
            .addTopic(List.of("a"), "v1")
            .addTopic(List.of("a", "b"), "v2")
            .addTopic(List.of("c"), "v3")
            .addTopic(List.of("$sys", "x"), "sys")
            .build();
    }

    static class FakeTicker implements Ticker {
        private final AtomicLong nanos = new AtomicLong(System.nanoTime());

        @Override
        public long read() {
            return nanos.get();
        }

        void advance(Duration d) {
            nanos.addAndGet(d.toNanos());
        }
    }
}

