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

import static org.apache.bifromq.util.TopicConst.MULTI_WILDCARD;
import static org.apache.bifromq.util.TopicConst.SINGLE_WILDCARD;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Normal level topic filter trie node.
 *
 * @param <V> value type
 */
final class NTopicFilterTrieNode<V> extends TopicFilterTrieNode<V> {
    private static final ConcurrentLinkedDeque<Long> KEYS = new ConcurrentLinkedDeque<>();
    private static final AtomicLong SEQ = new AtomicLong();
    private static volatile Ticker TICKER = Ticker.systemTicker();
    private static final Cache<Long, NTopicFilterTrieNode<?>> POOL = Caffeine.newBuilder()
        .expireAfterAccess(EXPIRE_AFTER)
        .recordStats()
        .scheduler(Scheduler.systemScheduler())
        .ticker(() -> TICKER.read())
        .removalListener((Long key, NTopicFilterTrieNode<?> value, RemovalCause cause) -> {
            KEYS.remove(key);
            if (cause == RemovalCause.EXPIRED || cause == RemovalCause.SIZE) {
                value.recycle();
            }
        })
        .build();

    private final NavigableSet<String> subLevelNames = new TreeSet<>();
    private final NavigableMap<String, Set<TopicTrieNode<V>>> subTopicTrieNodes = new TreeMap<>();
    private final Set<TopicTrieNode<V>> subWildcardMatchableTopicTrieNodes = new HashSet<>();
    private final Set<TopicTrieNode<V>> backingTopics = new HashSet<>();
    private String levelName;
    // point to the sub node during iteration
    private String subLevelName;

    NTopicFilterTrieNode() {
    }

    static <V> NTopicFilterTrieNode<V> borrow(TopicFilterTrieNode<V> parent,
                                              String levelName,
                                              Set<TopicTrieNode<V>> siblingTopicTrieNodes) {
        while (true) {
            Long key = KEYS.pollFirst();
            if (key == null) {
                break;
            }
            @SuppressWarnings("unchecked")
            NTopicFilterTrieNode<V> pooled = (NTopicFilterTrieNode<V>) POOL.asMap().remove(key);
            if (pooled != null) {
                return pooled.init(parent, levelName, siblingTopicTrieNodes);
            }
        }
        NTopicFilterTrieNode<V> node = new NTopicFilterTrieNode<>();
        return node.init(parent, levelName, siblingTopicTrieNodes);
    }

    static void release(NTopicFilterTrieNode<?> node) {
        node.recycle();
        long key = SEQ.incrementAndGet();
        KEYS.offerLast(key);
        POOL.put(key, node);
    }

    // test hooks (package-private)
    static void poolClear() {
        POOL.invalidateAll();
        POOL.cleanUp();
        KEYS.clear();
    }

    static void poolCleanUp() {
        POOL.cleanUp();
    }

    static int poolApproxSize() {
        return KEYS.size();
    }

    static void setTicker(Ticker ticker) {
        TICKER = ticker != null ? ticker : Ticker.systemTicker();
    }

    NTopicFilterTrieNode<V> init(TopicFilterTrieNode<V> parent, String levelName,
                                 Set<TopicTrieNode<V>> siblingTopicTrieNodes) {
        assert levelName != null;
        assert siblingTopicTrieNodes != null;
        assert siblingTopicTrieNodes.stream().allMatch(node -> node.levelName().equals(levelName));
        this.parent = parent;
        this.levelName = levelName;
        subLevelName = null;
        subLevelNames.clear();
        subTopicTrieNodes.clear();
        subWildcardMatchableTopicTrieNodes.clear();
        backingTopics.clear();
        for (TopicTrieNode<V> sibling : siblingTopicTrieNodes) {
            if (sibling.isUserTopic()) {
                backingTopics.add(sibling);
            }
            for (Map.Entry<String, TopicTrieNode<V>> entry : sibling.children().entrySet()) {
                TopicTrieNode<V> subNode = entry.getValue();
                if (subNode.wildcardMatchable()) {
                    subWildcardMatchableTopicTrieNodes.add(subNode);
                }
                subTopicTrieNodes.computeIfAbsent(subNode.levelName(), k -> new HashSet<>()).add(subNode);
                subLevelNames.add(subNode.levelName());
            }
        }
        // # match parent
        if (!backingTopics.isEmpty()) {
            subLevelNames.add(MULTI_WILDCARD);
        }
        if (!subWildcardMatchableTopicTrieNodes.isEmpty()) {
            subLevelNames.add(MULTI_WILDCARD);
            subLevelNames.add(SINGLE_WILDCARD);
        }
        seekChild("");
        return this;
    }

    @Override
    String levelName() {
        return levelName;
    }

    @Override
    Set<TopicTrieNode<V>> backingTopics() {
        return backingTopics;
    }

    @Override
    void seekChild(String childLevelName) {
        if (!subLevelNames.isEmpty()) {
            subLevelName = subLevelNames.ceiling(childLevelName);
        }
    }

    @Override
    void seekPrevChild(String childLevelName) {
        if (!subLevelNames.isEmpty()) {
            subLevelName = subLevelNames.floor(childLevelName);
        }
    }

    @Override
    void seekToFirstChild() {
        if (!subLevelNames.isEmpty()) {
            subLevelName = subLevelNames.first();
        }
    }

    @Override
    void seekToLastChild() {
        if (!subLevelNames.isEmpty()) {
            subLevelName = subLevelNames.last();
        }
    }

    @Override
    boolean atValidChild() {
        return subLevelName != null;
    }

    @Override
    void nextChild() {
        if (subLevelName != null) {
            subLevelName = subLevelNames.higher(subLevelName);
        }
    }

    @Override
    void prevChild() {
        if (subLevelName != null) {
            subLevelName = subLevelNames.lower(subLevelName);
        }
    }

    @Override
    TopicFilterTrieNode<V> childNode() {
        if (subLevelName == null) {
            throw new NoSuchElementException();
        }
        return switch (subLevelName) {
            case MULTI_WILDCARD -> MTopicFilterTrieNode.borrow(this, subWildcardMatchableTopicTrieNodes);
            case SINGLE_WILDCARD -> STopicFilterTrieNode.borrow(this, subWildcardMatchableTopicTrieNodes);
            default -> NTopicFilterTrieNode.borrow(this, subLevelName, subTopicTrieNodes.get(subLevelName));
        };
    }

    private void recycle() {
        parent = null;
        levelName = null;
        subLevelName = null;
        subLevelNames.clear();
        subTopicTrieNodes.clear();
        subWildcardMatchableTopicTrieNodes.clear();
        backingTopics.clear();
    }
}
