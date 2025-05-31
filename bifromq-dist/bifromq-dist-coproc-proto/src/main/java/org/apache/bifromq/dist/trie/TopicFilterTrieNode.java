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

import static org.apache.bifromq.util.TopicConst.NUL;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Set;

/**
 * The abstract class of topic filter trie node.
 *
 * @param <V> value type
 */
abstract class TopicFilterTrieNode<V> {
    protected final TopicFilterTrieNode<V> parent;

    protected TopicFilterTrieNode(TopicFilterTrieNode<V> parent) {
        this.parent = parent;
    }

    static <V> TopicFilterTrieNode<V> from(TopicTrieNode<V> root) {
        return new NTopicFilterTrieNode<>(null, root.levelName(), Set.of(root));
    }

    /**
     * Get the level name of the topic filter node.
     *
     * @return level name
     */
    abstract String levelName(); // filter level name

    /**
     * Get the topic filter prefix of the topic filter node.
     *
     * @return topic filter prefix
     */
    final List<String> topicFilterPrefix() {
        if (parent == null) {
            return List.of();
        }
        List<String> topicFilterPrefix = Lists.newLinkedList(parent.topicFilterPrefix());
        if (!parent.levelName().equals(NUL)) {
            topicFilterPrefix.add(parent.levelName());
        }
        return topicFilterPrefix;
    }

    /**
     * The backing topics for the topic filter node.
     *
     * @return backing topics
     */
    abstract Set<TopicTrieNode<V>> backingTopics();

    /**
     * Seek to the child node whose level name is greater or equals to specified level name.
     *
     * @param childLevelName child level name
     */
    abstract void seekChild(String childLevelName);

    /**
     * Seek to the child node whose level name is less than or equals to specified level name.
     *
     * @param childLevelName child level name
     */
    abstract void seekPrevChild(String childLevelName);

    /**
     * Seek to first child node.
     */
    abstract void seekToFirstChild();

    /**
     * Seek to last child node.
     */
    abstract void seekToLastChild();

    /**
     * Check if current iteration state point to valid child node.
     *
     * @return true if current iteration state point to valid child node
     */
    abstract boolean atValidChild();

    /**
     * Move iteration state to next child node.
     */
    abstract void nextChild();

    /**
     * Move iteration state to previous child node.
     */
    abstract void prevChild();

    /**
     * Get child node with initialized iteration state.
     *
     * @return child node
     */
    abstract TopicFilterTrieNode<V> childNode();
}
