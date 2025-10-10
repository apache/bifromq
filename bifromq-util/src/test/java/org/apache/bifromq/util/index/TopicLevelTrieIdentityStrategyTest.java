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

public class TopicLevelTrieIdentityStrategyTest {

    @Test
    public void testIdentityInsertAndLookup() {
        ValueStrategy<Box> s = ValueStrategy.identity();
        TestTrie trie = new TestTrie(s);
        Box a1 = new Box("a");
        Box a2 = new Box("a");
        trie.addPath(List.of("t", "x"), a1);
        trie.addPath(List.of("t", "x"), a2);
        Set<Box> res = trie.lookupExact(List.of("t", "x"));
        assertEquals(res.size(), 2);
        assertTrue(res.contains(a1));
        assertTrue(res.contains(a2));
    }

    @Test
    public void testIdentityRemoveRequiresSameInstance() {
        ValueStrategy<Box> s = ValueStrategy.identity();
        TestTrie trie = new TestTrie(s);
        Box a1 = new Box("a");
        Box a2 = new Box("a");
        trie.addPath(List.of("t"), a1);
        trie.removePath(List.of("t"), a2); // not the same instance
        assertFalse(trie.lookupExact(List.of("t")).isEmpty());
        trie.removePath(List.of("t"), a1);
        assertTrue(trie.lookupExact(List.of("t")).isEmpty());
    }

    private static final class TestTrie extends TopicLevelTrie<Box> {
        TestTrie(ValueStrategy<Box> s) {
            super(s);
        }

        void addPath(List<String> topicLevels, Box value) {
            add(topicLevels, value);
        }

        void removePath(List<String> topicLevels, Box value) {
            remove(topicLevels, value);
        }

        Set<Box> lookupExact(List<String> topicLevels) {
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
