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

package org.apache.bifromq.dist.trie;

import static org.apache.bifromq.util.TopicConst.MULTI_WILDCARD;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

import com.google.common.collect.Sets;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Multi-level topic filter trie node.
 *
 * @param <V> value type
 */
final class MTopicFilterTrieNode<V> extends TopicFilterTrieNode<V> {
    private final Set<TopicTrieNode<V>> backingTopics;

    MTopicFilterTrieNode(TopicFilterTrieNode<V> parent, Set<TopicTrieNode<V>> siblingTopicTrieNodes) {
        super(parent);
        Set<TopicTrieNode<V>> topics = parent != null ? parent.backingTopics() : emptySet();
        for (TopicTrieNode<V> sibling : siblingTopicTrieNodes) {
            topics = collectTopics(sibling, topics);
        }
        backingTopics = topics;
    }

    @Override
    String levelName() {
        return MULTI_WILDCARD;
    }

    @Override
    Set<TopicTrieNode<V>> backingTopics() {
        return backingTopics;
    }

    private Set<TopicTrieNode<V>> collectTopics(TopicTrieNode<V> node, Set<TopicTrieNode<V>> topics) {
        if (node.isUserTopic()) {
            topics = Sets.union(topics, singleton(node));
        }
        for (TopicTrieNode<V> child : node.children().values()) {
            topics = collectTopics(child, topics);
        }
        return topics;
    }


    @Override
    void seekChild(String childLevelName) {

    }

    @Override
    void seekPrevChild(String childLevelName) {

    }

    @Override
    void seekToFirstChild() {

    }

    @Override
    void seekToLastChild() {

    }

    @Override
    boolean atValidChild() {
        return false;
    }

    @Override
    void nextChild() {

    }

    @Override
    void prevChild() {

    }

    @Override
    TopicFilterTrieNode<V> childNode() {
        throw new NoSuchElementException();
    }
}
