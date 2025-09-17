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

package org.apache.bifromq.util.index;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class TopicLevelTrieTest {
    @AfterMethod
    public void resetHooks() {
        TopicLevelTrie.TestHook.beforeParentContractCas = null;
    }

    @Test
    public void testCleanParentRetriesAfterContractCasFailure() throws Exception {
        TestTrie trie = new TestTrie();

        trie.addPath(Arrays.asList("a", "b"), "v1");
        trie.addPath(List.of("x"), "v2");

        CountDownLatch ready = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicBoolean hookFired = new AtomicBoolean();

        TopicLevelTrie.TestHook.beforeParentContractCas = () -> {
            if (hookFired.compareAndSet(false, true)) {
                ready.countDown();
                try {
                    assertTrue(proceed.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        };

        Thread remover = new Thread(() -> trie.removePath(Arrays.asList("a", "b"), "v1"));
        remover.start();

        assertTrue(ready.await(5, TimeUnit.SECONDS));

        trie.addPath(List.of("z"), "v3");

        proceed.countDown();
        remover.join();

        assertFalse(hasZombieBranch(trie, "a"));
    }

    @Test
    public void testRemoveDeepTopicTrimsAllAncestors() throws Exception {
        TestTrie trie = new TestTrie();
        List<String> topicLevels = Arrays.asList("$iot", "tenant", "user", "device", "up", "sensor");
        String value = "payload";

        assertTrue(isEmpty(trie));
        trie.addPath(topicLevels, value);
        assertFalse(isEmpty(trie));
        trie.removePath(topicLevels, value);
        assertTrue(isEmpty(trie));
    }

    private boolean hasZombieBranch(TestTrie trie, String topicLevel) throws Exception {
        Field rootField = TopicLevelTrie.class.getDeclaredField("root");
        rootField.setAccessible(true);
        @SuppressWarnings("unchecked")
        INode<String> root = (INode<String>) rootField.get(trie);
        MainNode<String> main = root.main();
        if (main.cNode == null) {
            return false;
        }
        Branch<String> branch = main.cNode.branches.get(topicLevel);
        if (branch == null || branch.iNode == null) {
            return false;
        }
        return branch.iNode.main().tNode != null;
    }

    private boolean isEmpty(TestTrie trie) throws Exception {
        Field rootField = TopicLevelTrie.class.getDeclaredField("root");
        rootField.setAccessible(true);
        @SuppressWarnings("unchecked")
        INode<String> root = (INode<String>) rootField.get(trie);
        return root.main().cNode.branches.isEmpty();
    }

    private static final class TestTrie extends TopicLevelTrie<String> {
        void addPath(List<String> topicLevels, String value) {
            add(topicLevels, value);
        }

        void removePath(List<String> topicLevels, String value) {
            remove(topicLevels, value);
        }
    }
}
