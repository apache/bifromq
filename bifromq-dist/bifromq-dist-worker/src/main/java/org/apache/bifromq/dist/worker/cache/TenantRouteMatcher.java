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

package org.apache.bifromq.dist.worker.cache;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.compare;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.intersect;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.isNULLRange;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.buildMatchRoute;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.tenantBeginKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.tenantRouteStartKey;
import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Timer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVReader;
import org.apache.bifromq.dist.trie.TopicFilterIterator;
import org.apache.bifromq.dist.trie.TopicTrieNode;
import org.apache.bifromq.dist.worker.schema.Matching;
import org.apache.bifromq.dist.worker.schema.NormalMatching;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.distservice.GroupFanoutThrottled;
import org.apache.bifromq.plugin.eventcollector.distservice.PersistentFanoutThrottled;
import org.apache.bifromq.util.TopicUtil;

class TenantRouteMatcher implements ITenantRouteMatcher {
    private final String tenantId;
    private final Timer timer;
    private final Supplier<IKVReader> kvReaderSupplier;
    private final IEventCollector eventCollector;

    public TenantRouteMatcher(String tenantId,
                              Supplier<IKVReader> kvReaderSupplier,
                              IEventCollector eventCollector,
                              Timer timer) {
        this.tenantId = tenantId;
        this.timer = timer;
        this.kvReaderSupplier = kvReaderSupplier;
        this.eventCollector = eventCollector;
    }

    @Override
    public Map<String, IMatchedRoutes> matchAll(Set<String> topics,
                                                int maxPersistentFanoutCount,
                                                int maxGroupFanoutCount) {
        final Timer.Sample sample = Timer.start();
        Map<String, IMatchedRoutes> matchedRoutes = new HashMap<>();
        Set<String> pFanoutTrimedTopic = new HashSet<>();
        Set<String> gFanoutTrimedTopic = new HashSet<>();
        TopicTrieNode.Builder<String> topicTrieBuilder = TopicTrieNode.builder(false);
        topics.forEach(topic -> {
            topicTrieBuilder.addTopic(TopicUtil.parse(topic, false), topic);
            matchedRoutes.put(topic, new MatchResult(maxPersistentFanoutCount, maxGroupFanoutCount));
        });

        IKVReader rangeReader = kvReaderSupplier.get();
        rangeReader.refresh();

        ByteString tenantStartKey = tenantBeginKey(tenantId);
        Boundary tenantBoundary =
            intersect(toBoundary(tenantStartKey, upperBound(tenantStartKey)), rangeReader.boundary());
        if (isNULLRange(tenantBoundary)) {
            return matchedRoutes;
        }

        TopicFilterIterator<String> expansionSetItr = new TopicFilterIterator<>(topicTrieBuilder.build());
        Map<List<String>, Set<String>> matchedTopicFilters = new HashMap<>();
        IKVIterator itr = rangeReader.iterator();
        // track seek
        itr.seek(tenantBoundary.getStartKey());
        int probe = 0;
        while (itr.isValid() && compare(itr.key(), tenantBoundary.getEndKey()) < 0) {
            // track itr.key()
            Matching matching = buildMatchRoute(itr.key(), itr.value());
            // key: topic
            Set<String> matchedTopics = matchedTopicFilters.get(matching.matcher.getFilterLevelList());
            if (matchedTopics == null) {
                List<String> seekTopicFilter = matching.matcher.getFilterLevelList();
                expansionSetItr.seek(seekTopicFilter);
                if (expansionSetItr.isValid()) {
                    List<String> topicFilterToMatch = expansionSetItr.key();
                    if (topicFilterToMatch.equals(seekTopicFilter)) {
                        Set<String> backingTopics = new HashSet<>();
                        for (Set<String> topicSet : expansionSetItr.value().values()) {
                            for (String topic : topicSet) {
                                MatchResult matchResult = (MatchResult) matchedRoutes.computeIfAbsent(topic,
                                    k -> new MatchResult(maxPersistentFanoutCount, maxGroupFanoutCount));
                                switch (matching.type()) {
                                    case Normal -> {
                                        NormalMatching normalRoute = (NormalMatching) matching;
                                        if (normalRoute.subBrokerId() == 1) {
                                            if (matchResult.persistentFanoutCount.get() < maxPersistentFanoutCount) {
                                                matchResult.persistentFanoutCount.incrementAndGet();
                                                matchResult.routes.add(matching);
                                            } else {
                                                pFanoutTrimedTopic.add(topic);
                                            }
                                        } else {
                                            matchResult.routes.add(matching);
                                        }
                                    }
                                    case Group -> {
                                        if (matchResult.groupFanoutCount.get() < maxGroupFanoutCount) {
                                            matchResult.groupFanoutCount.incrementAndGet();
                                            matchResult.routes.add(matching);
                                        } else {
                                            gFanoutTrimedTopic.add(topic);
                                        }
                                    }
                                    default -> {
                                        // never happen
                                    }
                                }
                                backingTopics.add(topic);
                            }
                        }
                        matchedTopicFilters.put(matching.matcher.getFilterLevelList(), backingTopics);
                        itr.next();
                        probe = 0;
                    } else {
                        // next() is much cheaper than seek(), we probe following 20 entries
                        if (probe++ < 20) {
                            // probe next
                            itr.next();
                        } else {
                            // seek to match next topic filter
                            ByteString nextMatch = tenantRouteStartKey(tenantId, topicFilterToMatch);
                            itr.seek(nextMatch);
                        }
                    }
                } else {
                    break; // no more topic filter to match, stop here
                }
            } else {
                itr.next();
                for (String topic : matchedTopics) {
                    MatchResult matchResult = (MatchResult) matchedRoutes.computeIfAbsent(topic,
                        k -> new MatchResult(maxPersistentFanoutCount, maxGroupFanoutCount));
                    switch (matching.type()) {
                        case Normal -> {
                            NormalMatching normalRoute = (NormalMatching) matching;
                            if (normalRoute.subBrokerId() == 1) {
                                if (matchResult.persistentFanoutCount.get() < maxPersistentFanoutCount) {
                                    matchResult.persistentFanoutCount.incrementAndGet();
                                    matchResult.routes.add(matching);
                                } else {
                                    pFanoutTrimedTopic.add(topic);
                                }
                            } else {
                                matchResult.routes.add(matching);
                            }
                        }
                        case Group -> {
                            if (matchResult.groupFanoutCount.get() < maxGroupFanoutCount) {
                                matchResult.groupFanoutCount.incrementAndGet();
                                matchResult.routes.add(matching);
                            } else {
                                gFanoutTrimedTopic.add(topic);
                            }
                        }
                        default -> {
                            // never happen
                        }
                    }
                }
            }
        }
        sample.stop(timer);
        pFanoutTrimedTopic.forEach(topic -> eventCollector.report(getLocal(PersistentFanoutThrottled.class)
            .tenantId(tenantId)
            .topic(topic)
            .maxCount(maxPersistentFanoutCount)
        ));
        gFanoutTrimedTopic.forEach(topic -> eventCollector.report(getLocal(GroupFanoutThrottled.class)
            .tenantId(tenantId)
            .topic(topic)
            .maxCount(maxGroupFanoutCount)
        ));
        return matchedRoutes;
    }

    private static class MatchResult implements IMatchedRoutes {
        private final Set<Matching> routes = new HashSet<>();
        private final int maxPersistentFanoutCount;
        private final int maxGroupFanoutCount;
        private final AtomicInteger persistentFanoutCount = new AtomicInteger(0);
        private final AtomicInteger groupFanoutCount = new AtomicInteger(0);

        private MatchResult(int maxPersistentFanoutCount, int maxGroupFanoutCount) {
            this.maxPersistentFanoutCount = maxPersistentFanoutCount;
            this.maxGroupFanoutCount = maxGroupFanoutCount;
        }

        @Override
        public int maxPersistentFanout() {
            return maxPersistentFanoutCount;
        }

        @Override
        public int maxGroupFanout() {
            return maxGroupFanoutCount;
        }

        @Override
        public int persistentFanout() {
            return persistentFanoutCount.get();
        }

        @Override
        public int groupFanout() {
            return groupFanoutCount.get();
        }

        @Override
        public Set<Matching> routes() {
            return routes;
        }
    }
}
