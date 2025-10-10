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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    public void testRemoveDeepTopicTrimsAllAncestors() {
        TestTrie trie = new TestTrie();
        List<String> topicLevels = Arrays.asList("$iot", "tenant", "user", "device", "up", "sensor");
        String value = "payload";

        assertTrue(isEmpty(trie));
        trie.addPath(topicLevels, value);
        assertFalse(isEmpty(trie));
        trie.removePath(topicLevels, value);
        assertTrue(isEmpty(trie));
    }

    @Test
    public void testDetachParentINodeWhenParentHasValuesAndChildRemoved() {
        TestTrie trie = new TestTrie();

        trie.addPath(List.of("root"), "parentValue");
        trie.addPath(Arrays.asList("root", "child"), "childValue");

        trie.removePath(Arrays.asList("root", "child"), "childValue");

        Branch<String> branch = getRootBranch(trie, "root");
        assertNotNull(branch);
        assertTrue(branch.values().contains("parentValue"));
        assertNull(branch.iNode);
    }

    @Test
    public void testDetachParentINodeWhenGrandparentIsNonRoot() {
        // Arrange: root -> g -> p -> c; keep value at 'p' level
        TestTrie trie = new TestTrie();
        trie.addPath(Arrays.asList("g", "p"), "parentValue");
        trie.addPath(Arrays.asList("g", "p", "c"), "childValue");

        // Act: remove only child path so parent keeps its value but child subtree becomes empty
        trie.removePath(Arrays.asList("g", "p", "c"), "childValue");

        // Assert (expected after fix): at grandparent 'g' (non-root), branch 'p' must detach its iNode
        INode<String> root = trie.root();
        MainNode<String> rootMain = root.main();
        assertNotNull(rootMain.cNode);
        Branch<String> gBranch = rootMain.cNode.branches().get("g");
        assertNotNull(gBranch);
        assertNotNull(gBranch.iNode);

        MainNode<String> gMain = gBranch.iNode.main();
        assertNotNull(gMain.cNode);
        Branch<String> pBranch = gMain.cNode.branches().get("p");
        assertNotNull(pBranch);
        assertTrue(pBranch.values().contains("parentValue"));
        assertNull(pBranch.iNode, "Non-root grandparent must nullify iNode when child subtree is removed");
    }

    @Test
    public void testPruneOnlyTargetChildAmongSiblings() {
        // Arrange: siblings under the same parent
        TestTrie trie = new TestTrie();
        trie.addPath(Arrays.asList("g", "p"), "parentValue");
        trie.addPath(Arrays.asList("g", "p", "c1"), "child1");
        trie.addPath(Arrays.asList("g", "p", "c2"), "child2");

        // Act: remove only c1
        trie.removePath(Arrays.asList("g", "p", "c1"), "child1");

        // Assert: parent keeps value and only c2 remains
        INode<String> root = trie.root();
        MainNode<String> rootMain = root.main();
        Branch<String> gBranch = rootMain.cNode.branches().get("g");
        assertNotNull(gBranch);
        MainNode<String> gMain = gBranch.iNode.main();
        Branch<String> pBranch = gMain.cNode.branches().get("p");
        assertNotNull(pBranch);
        assertTrue(pBranch.values().contains("parentValue"));
        assertNotNull(pBranch.iNode);
        MainNode<String> pMain = pBranch.iNode.main();
        assertNotNull(pMain.cNode);
        assertFalse(pMain.cNode.branches().containsKey("c1"));
        assertTrue(pMain.cNode.branches().containsKey("c2"));
    }

    @Test
    public void testReinsertAndLookupAfterPrune() {
        // Arrange: build siblings and prune one
        TestTrie trie = new TestTrie();
        trie.addPath(Arrays.asList("g", "p"), "parentValue");
        trie.addPath(Arrays.asList("g", "p", "c1"), "child1");
        trie.addPath(Arrays.asList("g", "p", "c2"), "child2");
        trie.removePath(Arrays.asList("g", "p", "c1"), "child1");

        // Reinsert pruned child with a different value
        trie.addPath(Arrays.asList("g", "p", "c1"), "child1V2");

        // Lookup exact path semantics
        assertTrue(trie.lookupExact(List.of("g", "p")).contains("parentValue"));
        // g/p/c1 should include parentValue and child1V2
        {
            var res = trie.lookupExact(List.of("g", "p", "c1"));
            assertTrue(res.contains("parentValue"));
            assertTrue(res.contains("child1V2"));
        }
        // g/p/c2 should include parentValue and child2
        {
            var res = trie.lookupExact(List.of("g", "p", "c2"));
            assertTrue(res.contains("parentValue"));
            assertTrue(res.contains("child2"));
        }
    }

    private boolean hasZombieBranch(TestTrie trie, String topicLevel) {
        INode<String> root = trie.root();;
        MainNode<String> main = root.main();
        if (main.cNode == null) {
            return false;
        }
        Branch<String> branch = main.cNode.branches().get(topicLevel);
        if (branch == null || branch.iNode == null) {
            return false;
        }
        return branch.iNode.main().tNode != null;
    }

    private Branch<String> getRootBranch(TestTrie trie, String topicLevel) {
        INode<String> root = trie.root();
        MainNode<String> main = root.main();
        if (main.cNode == null) {
            return null;
        }
        return main.cNode.branches().get(topicLevel);
    }

    private boolean isEmpty(TestTrie trie) {
        INode<String> root = trie.root();
        return root.main().cNode.branchCount() == 0;
    }

    private static final class TestTrie extends TopicLevelTrie<String> {
        void addPath(List<String> topicLevels, String value) {
            add(topicLevels, value);
        }

        void removePath(List<String> topicLevels, String value) {
            remove(topicLevels, value);
        }

        // Exact-match lookup selector for test validation
        Set<String> lookupExact(List<String> topicLevels) {
            BranchSelector selector = new BranchSelector() {
                @Override
                public <V> Map<Branch<V>, Action> selectBranch(Map<String, Branch<V>> branches,
                                                               List<String> levels,
                                                               int currentLevel) {
                    Map<Branch<V>, Action> m = new HashMap<>();
                    if (currentLevel >= levels.size()) {
                        return m;
                    }
                    @SuppressWarnings("unchecked")
                    Branch<V> br = branches.get(levels.get(currentLevel));
                    if (br != null) {
                        boolean last = currentLevel == levels.size() - 1;
                        m.put(br, last ? Action.MATCH_AND_STOP : Action.MATCH_AND_CONTINUE);
                    }
                    return m;
                }
            };
            return lookup(topicLevels, selector);
        }
    }
}
