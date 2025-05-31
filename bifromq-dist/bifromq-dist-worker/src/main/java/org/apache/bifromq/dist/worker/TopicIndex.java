/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.dist.worker;

import static org.apache.bifromq.util.TopicConst.MULTI_WILDCARD;
import static org.apache.bifromq.util.TopicConst.SINGLE_WILDCARD;
import static org.apache.bifromq.util.TopicConst.SYS_PREFIX;

import org.apache.bifromq.util.TopicUtil;
import org.apache.bifromq.util.index.Branch;
import org.apache.bifromq.util.index.TopicLevelTrie;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Concurrent Index for searching Topics against TopicFilter.
 */
public final class TopicIndex<V> extends TopicLevelTrie<V> {
    private static final BranchSelector TopicMatcher = new BranchSelector() {
        @Override
        public <T> Map<Branch<T>, Action> selectBranch(Map<String, Branch<T>> branches,
                                                       List<String> topicLevels,
                                                       int currentLevel) {
            if (currentLevel < topicLevels.size() - 1) {
                // not last level
                String topicLevelToMatch = topicLevels.get(currentLevel);
                boolean matchParent = currentLevel + 1 == topicLevels.size() - 1
                    && topicLevels.get(currentLevel + 1).equals(MULTI_WILDCARD);
                switch (topicLevelToMatch) {
                    case SINGLE_WILDCARD -> {
                        Map<Branch<T>, Action> result = new HashMap<>();
                        for (Map.Entry<String, Branch<T>> entry : branches.entrySet()) {
                            Branch<T> branch = entry.getValue();
                            if (currentLevel == 0 && entry.getKey().startsWith(SYS_PREFIX)) {
                                // + skip SYS topic
                                continue;
                            }
                            result.put(branch, matchParent ? Action.MATCH_AND_CONTINUE : Action.CONTINUE);
                        }
                        return result;
                    }
                    default -> {
                        assert !topicLevelToMatch.equals(MULTI_WILDCARD) : "MULTI_WILDCARD should be the last level";
                        if (branches.containsKey(topicLevelToMatch)) {
                            return Map.of(branches.get(topicLevelToMatch),
                                matchParent ? Action.MATCH_AND_CONTINUE : Action.CONTINUE);
                        }
                        return Collections.emptyMap();
                    }
                }
            } else if (currentLevel == topicLevels.size() - 1) {
                // last level
                String topicLevelToMatch = topicLevels.get(currentLevel);
                switch (topicLevelToMatch) {
                    case SINGLE_WILDCARD -> {
                        Map<Branch<T>, Action> result = new HashMap<>();
                        for (Map.Entry<String, Branch<T>> entry : branches.entrySet()) {
                            Branch<T> branch = entry.getValue();
                            if (currentLevel == 0 && entry.getKey().startsWith(SYS_PREFIX)) {
                                // + skip SYS topic
                                continue;
                            }
                            result.put(branch, Action.MATCH_AND_STOP);
                        }
                        return result;
                    }
                    case MULTI_WILDCARD -> {
                        Map<Branch<T>, Action> result = new HashMap<>();
                        for (Map.Entry<String, Branch<T>> entry : branches.entrySet()) {
                            Branch<T> branch = entry.getValue();
                            if (currentLevel == 0 && entry.getKey().startsWith(SYS_PREFIX)) {
                                // # skip SYS topic
                                continue;
                            }
                            result.put(branch, Action.MATCH_AND_CONTINUE);
                        }
                        return result;
                    }
                    default -> {
                        if (branches.containsKey(topicLevelToMatch)) {
                            return Map.of(branches.get(topicLevelToMatch), Action.MATCH_AND_STOP);
                        }
                        return Collections.emptyMap();
                    }
                }
            } else {
                // # matches all descendant levels
                Map<Branch<T>, Action> result = new HashMap<>();
                for (Map.Entry<String, Branch<T>> entry : branches.entrySet()) {
                    Branch<T> branch = entry.getValue();
                    result.put(branch, Action.MATCH_AND_CONTINUE);
                }
                return result;
            }
        }
    };

    private static final BranchSelector TopicGetter = new BranchSelector() {
        @Override
        public <T> Map<Branch<T>, Action> selectBranch(Map<String, Branch<T>> branches,
                                                       List<String> topicLevels,
                                                       int currentLevel) {
            String topicLevelToMatch = topicLevels.get(currentLevel);
            if (branches.containsKey(topicLevelToMatch)) {
                return Map.of(branches.get(topicLevelToMatch),
                    currentLevel < topicLevels.size() - 1 ? Action.CONTINUE : Action.MATCH_AND_STOP);
            }
            return Collections.emptyMap();
        }
    };

    public void add(String topic, V value) {
        add(TopicUtil.parse(topic, false), value);
    }

    public void remove(String topic, V value) {
        remove(TopicUtil.parse(topic, false), value);
    }

    public Set<V> get(String topic) {
        return lookup(TopicUtil.parse(topic, false), TopicGetter);
    }

    public Set<V> match(String topicFilter) {
        return match(TopicUtil.parse(topicFilter, false));
    }

    public Set<V> match(List<String> topicFilterLevels) {
        return lookup(topicFilterLevels, TopicMatcher);
    }
}
