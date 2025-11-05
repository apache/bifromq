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
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
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
        // Wrap candidates in an order-sensitive, immutable view without copying elements
        return cache.get(new CacheKey(tenantId, topic, new CandidatesView(allCandidates)));
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

    private record CacheKey(String tenantId, String topic, CandidatesView candidates) {
    }

    private static final class CandidatesView extends AbstractCollection<KVRangeSetting> {
        private final Collection<KVRangeSetting> delegate;
        private final int size;
        private final int hash;

        CandidatesView(Collection<KVRangeSetting> delegate) {
            this.delegate = delegate;
            this.size = delegate.size();
            int h = 1;
            for (KVRangeSetting e : delegate) {
                h = 31 * h + (e == null ? 0 : e.hashCode());
            }
            this.hash = h;
        }

        @Override
        public Iterator<KVRangeSetting> iterator() {
            // read-only iteration
            final Iterator<KVRangeSetting> it = delegate.iterator();
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public KVRangeSetting next() {
                    return it.next();
                }
            };
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Collection<?> other)) {
                return false;
            }
            if (other.size() != size) {
                return false;
            }
            Iterator<?> it1 = this.iterator();
            Iterator<?> it2 = other.iterator();
            while (it1.hasNext() && it2.hasNext()) {
                Object o1 = it1.next();
                Object o2 = it2.next();
                if (!(Objects.equals(o1, o2))) {
                    return false;
                }
            }
            return !(it1.hasNext() || it2.hasNext());
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
