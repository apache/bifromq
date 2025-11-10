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

package org.apache.bifromq.mqtt.handler;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Interner;
import com.github.benmanes.caffeine.cache.RemovalCause;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Per-channel and per-topic timestamp deduplication cache.
 */
public class DedupCache {
    private static final Interner<String> TOPIC_INTERNER = Interner.newWeakInterner();
    private final Cache<String, ChannelTopicCache> channelTopicCaches;
    private final long expireAfterAccessMillis;
    private final long maxTopicsPerChannel;
    private final LongAdder totalTopics = new LongAdder();
    private final LongAdder channelSizeEvicted = new LongAdder();
    private final LongAdder topicSizeEvicted = new LongAdder();
    private final LongAdder channelExpiredEvicted = new LongAdder();
    private final LongAdder topicExpiredEvicted = new LongAdder();

    /**
     * Create a dedup cache with bounded capacity.
     *
     * @param expireAfterAccessMillis expire millis after access
     * @param maxChannels max cached channels
     * @param maxTopicsPerChannel max cached topics per channel
     */
    public DedupCache(long expireAfterAccessMillis, long maxChannels, long maxTopicsPerChannel) {
        this.expireAfterAccessMillis = expireAfterAccessMillis;
        this.maxTopicsPerChannel = maxTopicsPerChannel;
        this.channelTopicCaches = Caffeine.newBuilder()
            .expireAfterAccess(expireAfterAccessMillis, TimeUnit.MILLISECONDS)
            .maximumSize(maxChannels)
            .removalListener((String key, ChannelTopicCache channelTopicCache, RemovalCause cause) -> {
                long remaining = channelTopicCache.count.sum();
                if (remaining > 0) {
                    totalTopics.add(-remaining);
                }
                if (cause == RemovalCause.SIZE) {
                    channelSizeEvicted.increment();
                } else if (cause == RemovalCause.EXPIRED) {
                    channelExpiredEvicted.increment();
                }
            })
            .build();
    }

    /**
     * Check if duplicate under the given publisher channel and topic, update last timestamp if not duplicate.
     * Return true if duplicate, otherwise false.
     *
     * @param channelId the channel id of publisher client
     * @param topic  the topic
     * @param timestamp  the inbound timestamp of message
     */
    public boolean isDuplicate(String channelId, String topic, long timestamp) {
        String normalizedTopic = TOPIC_INTERNER.intern(topic);
        ChannelTopicCache perChannel = channelTopicCaches.get(channelId, k -> new ChannelTopicCache());
        AtomicReference<Long> lastRef = perChannel.cache.get(normalizedTopic, k -> {
            perChannel.count.increment();
            totalTopics.increment();
            return new AtomicReference<>(0L);
        });
        Long last = lastRef.get();
        if (last >= timestamp) {
            return true;
        }
        lastRef.set(timestamp);
        return false;
    }

    /**
     * Estimated number of cached channels.
     */
    public long estimatedChannels() {
        return channelTopicCaches.estimatedSize();
    }

    /**
     * Total cached topic entries across all channels.
     */
    public long totalCachedTopics() {
        return totalTopics.sum();
    }

    /**
     * Evicted channels due to over max channels.
     */
    public long channelEvictedBySize() {
        return channelSizeEvicted.sum();
    }

    /**
     * Evicted topics due to over max topics per channels.
     */
    public long topicEvictedBySize() {
        return topicSizeEvicted.sum();
    }

    /**
     * Evicted channels due to expiry.
     */
    public long channelEvictedByExpired() {
        return channelExpiredEvicted.sum();
    }

    /**
     * Evicted topics due to expiry.
     */
    public long topicEvictedByExpired() {
        return topicExpiredEvicted.sum();
    }

    private final class ChannelTopicCache {
        final Cache<String, AtomicReference<Long>> cache;
        final LongAdder count = new LongAdder();

        ChannelTopicCache() {
            this.cache = Caffeine.newBuilder()
                .expireAfterAccess(expireAfterAccessMillis, TimeUnit.MILLISECONDS)
                .maximumSize(maxTopicsPerChannel)
                .removalListener((String key, AtomicReference<Long> value, RemovalCause cause) -> {
                    Objects.requireNonNull(cause);
                    count.decrement();
                    totalTopics.decrement();
                    if (cause == RemovalCause.SIZE) {
                        topicSizeEvicted.increment();
                    } else if (cause == RemovalCause.EXPIRED) {
                        topicExpiredEvicted.increment();
                    }
                })
                .build();
        }
    }
}
