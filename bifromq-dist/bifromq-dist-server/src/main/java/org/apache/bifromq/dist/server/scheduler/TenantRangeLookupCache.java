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

package org.apache.bifromq.dist.server.scheduler;

import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.tenantBeginKey;
import static org.apache.bifromq.util.TopicConst.NUL;
import static org.apache.bifromq.util.TopicUtil.fastJoin;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.dist.rpc.proto.Fact;
import org.apache.bifromq.dist.trie.ITopicFilterIterator;
import org.apache.bifromq.dist.trie.ThreadLocalTopicFilterIterator;
import org.apache.bifromq.dist.trie.TopicTrieNode;
import org.apache.bifromq.util.TopicUtil;

class TenantRangeLookupCache {
    private final String tenantId;
    private final LoadingCache<CacheKey, Collection<KVRangeSetting>> cache;

    TenantRangeLookupCache(String tenantId, Duration expireAfterAccess, long maximumSize) {
        this.tenantId = tenantId;
        cache = Caffeine.newBuilder()
            .expireAfterAccess(expireAfterAccess)
            .maximumSize(maximumSize)
            .build(this::lookup);
    }

    // if no range contains subscription data for the topic, empty collection returned
    Collection<KVRangeSetting> lookup(String topic, NavigableMap<Boundary, KVRangeSetting> effectiveRouter) {
        ByteString tenantStartKey = tenantBeginKey(tenantId);
        Boundary tenantBoundary = toBoundary(tenantStartKey, upperBound(tenantStartKey));
        Collection<KVRangeSetting> allCandidates = findByBoundary(tenantBoundary, effectiveRouter);
        return cache.get(new CacheKey(tenantId, topic, List.copyOf(allCandidates)));
    }

    private Collection<KVRangeSetting> lookup(CacheKey key) {
        TopicTrieNode.Builder<String> topicTrieBuilder = TopicTrieNode.builder(true);
        topicTrieBuilder.addTopic(TopicUtil.parse(tenantId, key.topic, false), key.topic);
        try (ITopicFilterIterator<String> topicFilterIterator =
                 ThreadLocalTopicFilterIterator.get(topicTrieBuilder.build())) {
            topicFilterIterator.init(topicTrieBuilder.build());
            List<KVRangeSetting> finalCandidates = new LinkedList<>();
            for (KVRangeSetting candidate : key.candidates) {
                Optional<Fact> factOpt = candidate.getFact(Fact.class);
                if (factOpt.isEmpty()) {
                    finalCandidates.add(candidate);
                    continue;
                }
                Fact fact = factOpt.get();
                if (!fact.hasFirstGlobalFilterLevels() || !fact.hasLastGlobalFilterLevels()) {
                    // range is empty
                    continue;
                }
                List<String> firstFilterLevels = fact.getFirstGlobalFilterLevels().getFilterLevelList();
                List<String> lastFilterLevels = fact.getLastGlobalFilterLevels().getFilterLevelList();
                topicFilterIterator.seek(firstFilterLevels);
                if (topicFilterIterator.isValid()) {
                    // firstTopicFilter <= nextTopicFilter
                    if (topicFilterIterator.key().equals(firstFilterLevels)
                        || fastJoin(NUL, topicFilterIterator.key()).compareTo(fastJoin(NUL, lastFilterLevels)) <= 0) {
                        // if firstTopicFilter == nextTopicFilter || nextFilterLevels <= lastFilterLevels
                        // add to finalCandidates
                        finalCandidates.add(candidate);
                    }
                } else {
                    // endTopicFilter < firstTopicFilter, stop
                    break;
                }
            }
            return finalCandidates;
        }
    }

    private record CacheKey(String tenantId, String topic, List<KVRangeSetting> candidates) {
    }
}
