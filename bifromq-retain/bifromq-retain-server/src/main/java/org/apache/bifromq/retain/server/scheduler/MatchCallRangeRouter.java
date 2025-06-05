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

package org.apache.bifromq.retain.server.scheduler;

import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByBoundary;
import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.compare;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.compareEndKeys;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.compareStartKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.endKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.startKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.retain.store.schema.KVSchemaUtil.filterPrefix;
import static org.apache.bifromq.retain.store.schema.KVSchemaUtil.parseLevelHash;
import static org.apache.bifromq.retain.store.schema.KVSchemaUtil.retainKeyPrefix;
import static org.apache.bifromq.retain.store.schema.KVSchemaUtil.tenantBeginKey;
import static org.apache.bifromq.util.TopicUtil.isMultiWildcardTopicFilter;
import static org.apache.bifromq.util.TopicUtil.isNormalTopicFilter;
import static org.apache.bifromq.util.TopicUtil.isWildcardTopicFilter;
import static org.apache.bifromq.util.TopicUtil.parse;

import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.retain.store.schema.KVSchemaUtil;
import org.apache.bifromq.retain.store.schema.LevelHash;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;

class MatchCallRangeRouter {
    public static Map<KVRangeSetting, Set<String>> rangeLookup(String tenantId, Set<String> topicFilters,
                                                               NavigableMap<Boundary, KVRangeSetting> effectiveRouter) {
        Map<KVRangeSetting, Set<String>> topicFiltersByRange = new HashMap<>();
        for (String topicFilter : topicFilters) {
            // not shared subscription
            assert isNormalTopicFilter(topicFilter);
            if (isWildcardTopicFilter(topicFilter)) {
                boolean isFixedLevelMatch = !isMultiWildcardTopicFilter(topicFilter);
                List<String> filterLevels = parse(topicFilter, false);
                List<String> filterPrefix = filterPrefix(filterLevels);
                short levels = (short) (isFixedLevelMatch ? filterLevels.size() : filterLevels.size() - 1);
                Collection<KVRangeSetting> rangeSettingList;
                if (isFixedLevelMatch) {
                    ByteString startKey = retainKeyPrefix(tenantId, levels, filterPrefix);
                    Boundary topicBoundary = toBoundary(startKey, upperBound(startKey));
                    rangeSettingList = findByBoundary(topicBoundary, effectiveRouter);
                } else {
                    // topic filter end with "#"
                    if (filterPrefix.isEmpty()) {
                        // topic filters start with wildcard
                        Boundary topicBoundary = Boundary.newBuilder()
                            .setStartKey(retainKeyPrefix(tenantId, levels, filterPrefix))
                            .setEndKey(upperBound(tenantBeginKey(tenantId)))
                            .build();
                        rangeSettingList = findByBoundary(topicBoundary, effectiveRouter);
                    } else {
                        rangeSettingList = findCandidates(tenantId, levels, filterPrefix, effectiveRouter);
                    }
                }
                for (KVRangeSetting rangeSetting : rangeSettingList) {
                    topicFiltersByRange.computeIfAbsent(rangeSetting, k -> new HashSet<>()).add(topicFilter);
                }
            } else {
                ByteString retainKey = KVSchemaUtil.retainMessageKey(tenantId, topicFilter);
                Optional<KVRangeSetting> rangeSetting = findByKey(retainKey, effectiveRouter);
                assert rangeSetting.isPresent();
                topicFiltersByRange.computeIfAbsent(rangeSetting.get(), k -> new HashSet<>()).add(topicFilter);
            }
        }
        return topicFiltersByRange;
    }

    // find the candidates that overlap with each [retainKeyPrefixStart, retainKeyPrefixStop)
    // retainKeyPrefixStart defined retainKeyPrefix with levels vary from filterPrefix.size() to maxLevel(0xFF)
    // retainKeyPrefixStop defined upperBound of retainKeyPrefixStart with levels vary from filterPrefix.size() to maxLevel(0xFF)
    // retainKeyPrefixStart is the retainKeyPrefixStart when levels = filterPrefix.size()
    // retainKeyPrefixEnd is the upper bound of tenant range
    private static List<KVRangeSetting> findCandidates(String tenantId,
                                                       short levels,
                                                       List<String> filterPrefix,
                                                       NavigableMap<Boundary, KVRangeSetting> effectiveRouter) {
        ByteString retainKeyPrefixBegin = retainKeyPrefix(tenantId, levels, filterPrefix);
        ByteString levelHash = LevelHash.hash(filterPrefix);
        ByteString retainKeyPrefixEnd = upperBound(tenantBeginKey(tenantId));
        Boundary topicBoundary = Boundary.newBuilder()
            .setStartKey(retainKeyPrefixBegin)
            .setEndKey(retainKeyPrefixEnd)
            .build();
        Collection<KVRangeSetting> allCandidates = findByBoundary(topicBoundary, effectiveRouter);
        List<KVRangeSetting> candidates = new ArrayList<>(allCandidates.size());
        for (KVRangeSetting rangeSetting : allCandidates) {
            Boundary candidateBoundary = rangeSetting.boundary;
            ByteString candidateStartKey = startKey(candidateBoundary);
            ByteString candidateEndKey = endKey(candidateBoundary);
            if (compareStartKey(candidateStartKey, retainKeyPrefixBegin) > 0
                && compare(upperBound(levelHash), parseLevelHash(candidateStartKey)) <= 0) {
                // skip: retainKeyPrefixBegin < (some)retainKeyPrefixStop <= [candidateStartKey,candidateEndKey)
                continue;
            }
            if (compareEndKeys(candidateEndKey, retainKeyPrefixEnd) <= 0
                && compare(parseLevelHash(candidateEndKey), levelHash) <= 0) {
                // skip: retainKeyPrefixBegin < [candidateStartKey,candidateEndKey) <= (some)retainKeyPrefixStart
                continue;
            }
            candidates.add(rangeSetting);
        }
        return candidates;
    }
}
