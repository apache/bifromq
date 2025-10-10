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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class TopicLevelTrieValueStrategyTest {

    @Test
    public void testDedupByCustomStrategy() {
        TestTrie trie = new TestTrie(new TestValueStrategy());
        TestVal v1 = new TestVal("k", "d1");
        TestVal v2 = new TestVal("k", "d2"); // same key, different data -> equivalent
        trie.addPath(List.of("a", "b"), v1);
        trie.addPath(List.of("a", "b"), v2);
        Set<TestVal> res = trie.lookupExact(List.of("a", "b"));
        assertEquals(res.size(), 1, "Should dedup by key only");
        assertTrue(res.iterator().next().key().equals("k"));
    }

    @Test
    public void testRemoveByEquivalentValue() {
        TestTrie trie = new TestTrie(new TestValueStrategy());
        TestVal v1 = new TestVal("k", "d1");
        TestVal v2 = new TestVal("k", "d2");
        trie.addPath(List.of("x"), v1);
        trie.removePath(List.of("x"), v2);
        Set<TestVal> res = trie.lookupExact(List.of("x"));
        assertTrue(res.isEmpty(), "Remove should match by key equivalence");
    }

    @Test
    public void testLookupAcrossLevelsWithParentAndChild() {
        TestTrie trie = new TestTrie(new TestValueStrategy());
        TestVal parent = new TestVal("p", "pd");
        TestVal child = new TestVal("c", "cd");
        trie.addPath(List.of("g", "p"), parent);
        trie.addPath(List.of("g", "p", "c"), child);
        Set<TestVal> resP = trie.lookupExact(List.of("g", "p"));
        assertTrue(resP.contains(parent));
        Set<TestVal> resC = trie.lookupExact(List.of("g", "p", "c"));
        // contains both parent and child at leaf
        assertTrue(resC.contains(parent));
        assertTrue(resC.contains(child));
    }

    @Test
    public void testValuesViewContainsUsesStrategy() {
        TestTrie trie = new TestTrie(new TestValueStrategy());
        TestVal v = new TestVal("k", "d1");
        trie.addPath(List.of("r"), v);
        // Query via exact lookup and assert contains works with equivalent value
        Set<TestVal> res = trie.lookupExact(List.of("r"));
        assertTrue(res.contains(new TestVal("k", "d2")), "values().contains should respect strategy equivalence");
        assertFalse(res.contains(new TestVal("other", "d1")));
    }

    private static class TestTrie extends TopicLevelTrie<TestVal> {

        TestTrie(ValueStrategy<TestVal> strategy) {
            super(strategy);
        }

        void addPath(List<String> topicLevels, TestVal value) {
            add(topicLevels, value);
        }

        void removePath(List<String> topicLevels, TestVal value) {
            remove(topicLevels, value);
        }

        Set<TestVal> lookupExact(List<String> topicLevels) {
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

