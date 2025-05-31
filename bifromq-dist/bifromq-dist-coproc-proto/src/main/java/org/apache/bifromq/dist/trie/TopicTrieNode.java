/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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
import static org.apache.bifromq.util.TopicConst.SYS_PREFIX;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * TopicTrieNode represent a TopicLevel in the topic trie.
 *
 * @param <V> the value associated with the topic
 */
public final class TopicTrieNode<V> {
    public static <V> Builder<V> builder(boolean isGlobal) {
        return new Builder<>(isGlobal);
    }

    private final String levelName;
    private final boolean wildcardMatchable;
    private final NavigableMap<String, TopicTrieNode<V>> children = new TreeMap<>();
    private final Set<V> values = new HashSet<>();
    private List<String> topic;

    private TopicTrieNode() {
        this(NUL, false);
    }

    private TopicTrieNode(String levelName, boolean wildcardMatchable) {
        this.levelName = levelName;
        this.wildcardMatchable = wildcardMatchable;
    }

    /**
     * Topic Level Name defined in MQTT Spec.
     *
     * @return the level name
     */
    String levelName() {
        return levelName;
    }

    /**
     * Check if the topic node represents a user topic.
     *
     * @return true if the topic node represents a user topic
     */
    boolean isUserTopic() {
        return !values.isEmpty();
    }

    List<String> topic() {
        return topic;
    }

    /**
     * Get the values associated with the topic.
     *
     * @return the values associated with the topic
     */
    Set<V> values() {
        return Collections.unmodifiableSet(values);
    }

    /**
     * Check if the topic node is wildcard matchable.
     *
     * @return true if the topic node is wildcard matchable
     */
    boolean wildcardMatchable() {
        return wildcardMatchable;
    }

    NavigableMap<String, TopicTrieNode<V>> children() {
        return Collections.unmodifiableNavigableMap(children);
    }

    TopicTrieNode<V> child(String levelName) {
        return children.get(levelName);
    }

    /**
     * The builder for building a TopicTrieNode.
     *
     * @param <V> the value associated with the topic
     */
    public static class Builder<V> {
        private final TopicTrieNode<V> root;
        private final boolean isGlobal;

        /**
         * Create a builder for building a global topic trie.
         *
         * @param isGlobal if the topic trie is global which the first level represents the tenantId
         */
        private Builder(boolean isGlobal) {
            this.root = new TopicTrieNode<>();
            this.isGlobal = isGlobal;
        }

        public TopicTrieNode<V> build() {
            return root;
        }

        /**
         * Add a topic to the topic trie.
         *
         * @param topicLevels the topic levels of the topic
         * @param value       the value associated with the topic
         * @return the builder
         */
        public Builder<V> addTopic(List<String> topicLevels, V value) {
            assert isGlobal ? topicLevels.size() > 1 : !topicLevels.isEmpty();
            addChild(root, 0, topicLevels, value);
            return this;
        }

        private void addChild(TopicTrieNode<V> node, int level, List<String> topicLevels, V value) {
            if (topicLevels.isEmpty()) {
                return;
            }

            String levelName = topicLevels.get(level);
            // if building a global topic trie, the first level represent the tenantId in global topic filter
            // the second level represent the first-level topic name in user's topic filter
            // skip using wildcard represent system topic which starts with '$'
            boolean wildcardMatchable = isGlobal
                ? level > 1 || level == 1 && !levelName.startsWith(SYS_PREFIX)
                : level > 0 || !levelName.startsWith(SYS_PREFIX);
            TopicTrieNode<V> child =
                node.children.computeIfAbsent(levelName, k -> new TopicTrieNode<>(levelName, wildcardMatchable));
            if (level == topicLevels.size() - 1) {
                child.topic = topicLevels;
                child.values.add(value);
            } else {
                addChild(child, level + 1, topicLevels, value);
            }
        }
    }
}
