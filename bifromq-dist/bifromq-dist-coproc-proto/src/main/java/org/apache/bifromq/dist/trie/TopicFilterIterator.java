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

import static org.apache.bifromq.util.TopicConst.NUL;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Stack;

/**
 * The iterator for topics' expansion set.
 *
 * @param <V> the value type for topic associated value
 */
public class TopicFilterIterator<V> implements ITopicFilterIterator<V> {
    private final TopicTrieNode<V> topicTrieRoot;
    // the invariant of the stack:
    // empty: no valid topic filter to iterator from  expansion set
    // non-empty: always point to a valid topic filter in expansion set when there is no operation
    private final Stack<TopicFilterTrieNode<V>> traverseStack = new Stack<>();

    public TopicFilterIterator(TopicTrieNode<V> root) {
        this.topicTrieRoot = root;
        seek(Collections.emptyList());
    }

    @Override
    public void seek(List<String> filterLevels) {
        traverseStack.clear();
        traverseStack.add(TopicFilterTrieNode.from(topicTrieRoot));
        int i = -1;
        out:
        while (!traverseStack.isEmpty() && i < filterLevels.size()) {
            String levelNameToSeek = i == -1 ? NUL : filterLevels.get(i);
            i++;
            TopicFilterTrieNode<V> node = traverseStack.peek();
            String levelName = node.levelName();
            int cmp = levelNameToSeek.compareTo(levelName);
            if (cmp < 0) {
                // levelNameToSeek < levelName
                break;
            } else if (cmp == 0) {
                // levelNameToSeek == levelName
                if (i == filterLevels.size()) {
                    break;
                }
                String nextLevelNameToSeek = filterLevels.get(i);
                node.seekChild(nextLevelNameToSeek);
                if (node.atValidChild()) {
                    traverseStack.push(node.childNode());
                } else {
                    // backtrace
                    traverseStack.pop();
                    if (traverseStack.isEmpty()) {
                        break;
                    }
                    // backtrace and replace current node with its next sibling
                    while (!traverseStack.isEmpty()) {
                        TopicFilterTrieNode<V> parent = traverseStack.peek();
                        parent.nextChild();
                        if (parent.atValidChild()) {
                            traverseStack.push(parent.childNode());
                            break out;
                        } else {
                            traverseStack.pop();
                        }
                    }
                }
            } else {
                // levelNameToSeek > levelName
                // no least next topicfilter exists in expansion set for the given topic filter
                // drain the stack
                while (!traverseStack.isEmpty()) {
                    traverseStack.pop();
                }
            }
        }
        // prepare current stack to point to the least next topicfilter
        while (!traverseStack.isEmpty()) {
            TopicFilterTrieNode<V> node = traverseStack.peek();
            if (node.backingTopics().isEmpty()) {
                assert node.atValidChild();
                traverseStack.push(node.childNode());
            } else {
                break;
            }
        }
    }

    @Override
    public void seekPrev(List<String> filterLevels) {
        traverseStack.clear();
        traverseStack.add(TopicFilterTrieNode.from(topicTrieRoot));
        int i = -1;
        out:
        while (!traverseStack.isEmpty() && i < filterLevels.size()) {
            String levelNameToSeek = i == -1 ? NUL : filterLevels.get(i);
            i++;
            TopicFilterTrieNode<V> node = traverseStack.peek();
            String levelName = node.levelName();
            int cmp = levelNameToSeek.compareTo(levelName);
            if (cmp > 0) {
                // levelNameToSeek > levelName
                break;
            } else if (cmp == 0) {
                // levelNameToSeek == levelName
                if (i == filterLevels.size()) {
                    // backtrace to find strictly greatest prev topic filter
                    traverseStack.pop();
                    if (traverseStack.isEmpty()) {
                        break;
                    }
                    while (!traverseStack.isEmpty()) {
                        TopicFilterTrieNode<V> parent = traverseStack.peek();
                        // backtrace and replace current node with its prev sibling
                        parent.prevChild();
                        if (parent.atValidChild()) {
                            traverseStack.push(parent.childNode());
                            break out;
                        } else if (parent.backingTopics().isEmpty()) {
                            // if current stack do not represent a topicfilter in expansion set, backtrace one level up
                            traverseStack.pop();
                        } else {
                            // current stack represents the greatest previous topic filter
                            // make sure it points to valid child position
                            parent.seekToFirstChild();
                            return;
                        }
                    }
                } else {
                    String nextLevelNameToSeek = filterLevels.get(i);
                    node.seekPrevChild(nextLevelNameToSeek);
                    if (node.atValidChild()) {
                        traverseStack.push(node.childNode());
                    } else {
                        // so far matches
                        if (!node.backingTopics().isEmpty()) {
                            // current stack represents a topicfilter in expansion set
                            // make sure it points to valid child position
                            node.seekToFirstChild();
                            return;
                        }
                        // backtrace
                        traverseStack.pop();
                        if (traverseStack.isEmpty()) {
                            break;
                        }
                        while (!traverseStack.isEmpty()) {
                            TopicFilterTrieNode<V> parent = traverseStack.peek();
                            // backtrace and replace current node with its prev sibling
                            parent.prevChild();
                            if (parent.atValidChild()) {
                                // has prev sibling
                                traverseStack.push(parent.childNode());
                                break out;
                            } else if (parent.backingTopics().isEmpty()) {
                                // if current stack not represent a valid topic, backtrace one level up
                                traverseStack.pop();
                            } else {
                                // current stack represents a topicfilter in expansion set
                                // make sure it points to valid child position
                                node.seekToFirstChild();
                                return;
                            }
                        }
                    }
                }
            } else {
                // levelNameToSeek < levelName
                // no greatest prev topicfilter exists in expansion set for the given topic filter
                // drain the stack
                while (!traverseStack.isEmpty()) {
                    traverseStack.pop();
                }
            }
        }
        // prepare current stack to point to the greatest topicfilter
        while (!traverseStack.isEmpty()) {
            TopicFilterTrieNode<V> node = traverseStack.peek();
            node.seekToLastChild();
            if (node.atValidChild()) {
                traverseStack.push(node.childNode());
            } else {
                // if no child, then current stack must represent the greatest previous topic filter
                assert !node.backingTopics().isEmpty();
                break;
            }
        }
    }

    @Override
    public boolean isValid() {
        return !traverseStack.isEmpty();
    }

    @Override
    public void prev() {
        assert traverseStack.isEmpty() || !traverseStack.peek().backingTopics().isEmpty();
        while (!traverseStack.isEmpty()) {
            traverseStack.pop();
            if (traverseStack.isEmpty()) {
                return;
            }
            TopicFilterTrieNode<V> parent = traverseStack.peek();
            parent.prevChild();
            if (parent.atValidChild()) {
                TopicFilterTrieNode<V> subNode = parent.childNode();
                traverseStack.add(subNode);
                // seek to greatest topicfilter
                while (!traverseStack.isEmpty()) {
                    TopicFilterTrieNode<V> node = traverseStack.peek();
                    node.seekToLastChild();
                    if (node.atValidChild()) {
                        traverseStack.push(node.childNode());
                    } else if (!node.backingTopics().isEmpty()) {
                        return;
                    }
                }
            } else if (!parent.backingTopics().isEmpty()) {
                return;
            }
        }
    }

    @Override
    public void next() {
        assert traverseStack.isEmpty() || !traverseStack.peek().backingTopics().isEmpty();
        while (!traverseStack.isEmpty()) {
            TopicFilterTrieNode<V> node = traverseStack.peek();
            if (node.atValidChild()) {
                TopicFilterTrieNode<V> subNode = node.childNode();
                traverseStack.add(subNode);
                if (!subNode.backingTopics().isEmpty()) {
                    break;
                }
            } else {
                traverseStack.pop();
                if (!traverseStack.isEmpty()) {
                    traverseStack.peek().nextChild();
                }
            }
        }
    }

    @Override
    public List<String> key() {
        if (traverseStack.isEmpty()) {
            throw new NoSuchElementException();
        }
        TopicFilterTrieNode<V> filterNode = traverseStack.peek();
        List<String> topicFilter = new LinkedList<>(filterNode.topicFilterPrefix());
        topicFilter.add(filterNode.levelName());
        return topicFilter;
    }

    @Override
    public Map<List<String>, Set<V>> value() {
        if (traverseStack.isEmpty()) {
            throw new NoSuchElementException();
        }
        Map<List<String>, Set<V>> value = new HashMap<>();
        for (TopicTrieNode<V> topicTrieNode : traverseStack.peek().backingTopics()) {
            value.put(topicTrieNode.topic(), topicTrieNode.values());
        }
        return value;
    }
}
