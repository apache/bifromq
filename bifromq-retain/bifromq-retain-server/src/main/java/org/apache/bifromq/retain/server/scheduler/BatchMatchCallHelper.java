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

import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.retain.rpc.proto.MatchResult;
import org.apache.bifromq.type.TopicMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchMatchCallHelper {
    static CompletableFuture<Map<String, MatchResult>> parallelMatch(long reqId, long now,
                                                                     Map<KVRangeSetting, Set<String>> rangeAssignment,
                                                                     IRetainMatcher matcher) {
        List<CompletableFuture<Map<String, MatchResult>>> futures = new ArrayList<>(rangeAssignment.size());

        for (KVRangeSetting rangeSetting : rangeAssignment.keySet()) {
            Set<String> topicFilters = rangeAssignment.get(rangeSetting);
            Map<String, Integer> topicFiltersMap = topicFilters.stream().collect(Collectors.toMap(k -> k, v -> 1));
            futures.add(matcher.match(reqId, now, topicFiltersMap, rangeSetting));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .thenApply(v -> futures.stream()
                .map(CompletableFuture::join)
                .reduce(new HashMap<>(), (acc, m) -> {
                    acc.putAll(m);
                    return acc;
                }));
    }

    static CompletableFuture<Map<String, MatchResult>> serialMatch(long reqId, long now,
                                                                   Map<KVRangeSetting, Set<String>> rangeAssignment,
                                                                   int limit, IRetainMatcher matcher) {
        Map<String, List<TopicMessage>> aggregatedMap = new HashMap<>();
        for (Set<String> topicFilters : rangeAssignment.values()) {
            topicFilters.forEach(tf -> aggregatedMap.putIfAbsent(tf, new LinkedList<>()));
        }
        CompletableFuture<Map<String, List<TopicMessage>>> future = CompletableFuture.completedFuture(aggregatedMap);

        // Process each KVRangeSetting serially.
        for (KVRangeSetting rangeSetting : rangeAssignment.keySet()) {
            future = future.thenCompose(aggregated -> {
                // For each topic filter, if its aggregated result already contains at least 'limit' messages,
                // remove it from the remaining filters.
                Map<String, Integer> remainingFilters = new HashMap<>();
                for (String tf : rangeAssignment.get(rangeSetting)) {
                    List<TopicMessage> retainMsgList = aggregated.get(tf);
                    if (retainMsgList.size() < limit) {
                        remainingFilters.put(tf, limit - retainMsgList.size());
                    }
                }

                // If no topic filters remain for this range.
                if (remainingFilters.isEmpty()) {
                    return CompletableFuture.completedFuture(aggregated);
                }

                // Issue the match query for the remaining topic filters, using the provided limit.
                return matcher.match(reqId, now, remainingFilters, rangeSetting).thenApply(result -> {
                    // For each topic filter in the result, combine the new matches with any previously
                    // aggregated matches, ensuring that the total does not exceed the limit.
                    for (Map.Entry<String, MatchResult> entry : result.entrySet()) {
                        String topicFilter = entry.getKey();
                        MatchResult newResult = entry.getValue();
                        aggregated.computeIfPresent(topicFilter, (k, v) -> {
                            for (int i = 0; i < newResult.getMessagesCount(); i++) {
                                if (v.size() < limit) {
                                    v.add(newResult.getMessages(i));
                                }
                            }
                            return v;
                        });
                    }
                    return aggregated;
                });
            });
        }
        return future.thenApply(v -> {
            Map<String, MatchResult> result = new HashMap<>();
            v.forEach((k, v1) -> result.put(k, MatchResult.newBuilder().addAllMessages(v1).build()));
            return result;
        });
    }

    interface IRetainMatcher {
        CompletableFuture<Map<String, MatchResult>> match(long reqId, long now, Map<String, Integer> topicFilters,
                                                          KVRangeSetting rangeSetting);
    }
}
