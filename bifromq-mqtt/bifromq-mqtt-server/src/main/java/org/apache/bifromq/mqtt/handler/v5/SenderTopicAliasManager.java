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

package org.apache.bifromq.mqtt.handler.v5;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Optional;
import java.util.PriorityQueue;

public class SenderTopicAliasManager {
    public record AliasCreationResult(int alias, boolean isFirstTime) {
    }

    private static final String CACHE_KEY = "ALIAS";
    private final int maxAlias;
    private final LoadingCache<String, AliasManagerState> cache;

    public SenderTopicAliasManager(int maxAlias, Duration expiryTime) {
        this.maxAlias = maxAlias;
        this.cache = Caffeine.newBuilder()
            .expireAfterAccess(expiryTime)
            .build(key -> new AliasManagerState(maxAlias));
    }

    public Optional<AliasCreationResult> tryAlias(String topic) {
        if (maxAlias == 0) {
            return Optional.empty();
        }
        AliasManagerState state = cache.get(CACHE_KEY);
        return state.tryAlias(topic);
    }

    private static class AliasManagerState {
        int nextAlias = 1;
        HashMap<String, AliasEntry> topicToAlias = new HashMap<>();
        PriorityQueue<AliasEntry> usageQueue = new PriorityQueue<>(Comparator.comparingInt(AliasEntry::usageCount));
        final int maxAlias;

        AliasManagerState(int maxAlias) {
            this.maxAlias = maxAlias;
        }

        public Optional<AliasCreationResult> tryAlias(String topic) {
            boolean isFirstTime = false;
            AliasEntry entry = topicToAlias.get(topic);
            if (entry == null) {
                isFirstTime = true;
                if (topicToAlias.size() >= maxAlias) {
                    // Reuse the alias of the least used entry
                    entry = recycleLeastUsedAlias();
                    entry = new AliasEntry(topic, entry.alias, 1);
                } else {
                    if (nextAlias > maxAlias) {
                        nextAlias = 1; // Optionally handle alias overflow
                    }
                    entry = new AliasEntry(topic, nextAlias++, 1);
                }
                topicToAlias.put(topic, entry);
                usageQueue.offer(entry);
            } else {
                // Update usage count for existing entries
                usageQueue.remove(entry);
                entry = new AliasEntry(entry.topic(), entry.alias(), entry.usageCount() + 1);
                topicToAlias.put(topic, entry);
                usageQueue.offer(entry);
            }
            return Optional.of(new AliasCreationResult(entry.alias(), isFirstTime));
        }

        private AliasEntry recycleLeastUsedAlias() {
            AliasEntry leastUsed = usageQueue.poll();
            if (leastUsed != null) {
                topicToAlias.remove(leastUsed.topic());
                // No need to add it to a recycled queue, as we will reuse it directly
            }
            return leastUsed; // Return the recycled entry for immediate reuse
        }

        public record AliasEntry(String topic, int alias, int usageCount) {
        }
    }

}
