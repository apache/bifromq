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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Multi-level topic filter trie node.
 *
 * @param <V> value type
 */
final class MTopicFilterTrieNode<V> extends TopicFilterTrieNode<V> {
    private static final ConcurrentLinkedDeque<Long> KEYS = new ConcurrentLinkedDeque<>();
    private static final AtomicLong SEQ = new AtomicLong();
    private static volatile Ticker TICKER = Ticker.systemTicker();
    private static final Cache<Long, MTopicFilterTrieNode<?>> POOL = Caffeine.newBuilder()
        .expireAfterAccess(EXPIRE_AFTER)
        .recordStats()
        .scheduler(Scheduler.systemScheduler())
        .ticker(() -> TICKER.read())
        .removalListener((Long key, MTopicFilterTrieNode<?> value, RemovalCause cause) -> {
            KEYS.remove(key);
            if (cause == RemovalCause.EXPIRED || cause == RemovalCause.SIZE) {
                value.recycle();
            }
        })
        .build();

    private final Set<TopicTrieNode<V>> backingTopics = new HashSet<>();

    MTopicFilterTrieNode() {
    }

    static <V> MTopicFilterTrieNode<V> borrow(TopicFilterTrieNode<V> parent,
                                              Set<TopicTrieNode<V>> siblingTopicTrieNodes) {
        while (true) {
            Long key = KEYS.pollFirst();
            if (key == null) {
                break;
            }
            @SuppressWarnings("unchecked")
            MTopicFilterTrieNode<V> pooled = (MTopicFilterTrieNode<V>) POOL.asMap().remove(key);
            if (pooled != null) {
                return pooled.init(parent, siblingTopicTrieNodes);
            }
        }
        MTopicFilterTrieNode<V> node = new MTopicFilterTrieNode<>();
        return node.init(parent, siblingTopicTrieNodes);
    }

    static void release(MTopicFilterTrieNode<?> node) {
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

    MTopicFilterTrieNode<V> init(TopicFilterTrieNode<V> parent, Set<TopicTrieNode<V>> siblingTopicTrieNodes) {
        assert siblingTopicTrieNodes != null;
        this.parent = parent;
        backingTopics.clear();
        if (parent != null) {
            backingTopics.addAll(parent.backingTopics());
        }
        for (TopicTrieNode<V> sibling : siblingTopicTrieNodes) {
            collectTopics(sibling);
        }
        return this;
    }

    @Override
    String levelName() {
        return MULTI_WILDCARD;
    }

    @Override
    Set<TopicTrieNode<V>> backingTopics() {
        return backingTopics;
    }

    private void collectTopics(TopicTrieNode<V> node) {
        if (node.isUserTopic()) {
            backingTopics.add(node);
        }
        for (TopicTrieNode<V> child : node.children().values()) {
            collectTopics(child);
        }
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

    private void recycle() {
        parent = null;
        backingTopics.clear();
    }
}
