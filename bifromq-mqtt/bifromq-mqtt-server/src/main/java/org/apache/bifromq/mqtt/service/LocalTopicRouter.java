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

package org.apache.bifromq.mqtt.service;

import static org.apache.bifromq.mqtt.inbox.util.DelivererKeyUtil.toDelivererKey;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.dist.client.MatchResult;
import org.apache.bifromq.dist.client.UnmatchResult;
import org.apache.bifromq.sysprops.props.DeliverersPerMqttServer;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.util.TopicUtil;

public class LocalTopicRouter implements ILocalTopicRouter {
    private static final int TOPIC_FILTER_BUCKET_NUM = DeliverersPerMqttServer.INSTANCE.get();
    private final String serverId;
    private final IDistClient distClient;
    private final ConcurrentMap<TopicFilter, CompletableFuture<LocalRoutes>> routeMap = new ConcurrentHashMap<>();

    public LocalTopicRouter(String serverId, IDistClient distClient) {
        this.serverId = serverId;
        this.distClient = distClient;
    }

    @Override
    public CompletableFuture<MatchResult> addTopicRoute(long reqId,
                                                        String tenantId,
                                                        String topicFilter,
                                                        long incarnation,
                                                        String channelId) {
        assert !TopicUtil.isSharedSubscription(topicFilter);
        int bucketId = topicFilterBucketId(channelId);
        CompletableFuture<LocalRoutes> toReturn =
            routeMap.compute(new TopicFilter(tenantId, topicFilter, bucketId), (k, v) -> {
                if (v == null || v.isCompletedExceptionally()) {
                    LocalRoutes localRoutes = new LocalRoutes(k.bucketId);
                    return distClient.addRoute(reqId,
                            k.tenantId,
                            TopicUtil.from(k.topicFilter),
                            localRoutes.localReceiverId(),
                            toDelivererKey(k.tenantId, localRoutes.localReceiverId(), serverId),
                            0,
                            localRoutes.incarnation())
                        .thenApply(matchResult -> {
                            if (matchResult == MatchResult.OK) {
                                localRoutes.routesInfo.put(channelId, incarnation);
                                return localRoutes;
                            }
                            throw new AddRouteException(matchResult);
                        });
                } else {
                    CompletableFuture<LocalRoutes> updated = new CompletableFuture<>();
                    v.whenComplete((routeList, e) -> {
                        if (e != null) {
                            updated.completeExceptionally(e);
                        } else {
                            routeList.routesInfo.put(channelId, incarnation);
                            updated.complete(routeList);
                        }
                    });
                    return updated;
                }
            });
        return toReturn
            .handle((routeList, e) -> {
                if (e != null) {
                    routeMap.remove(new TopicFilter(tenantId, topicFilter, bucketId), toReturn);
                    if (e instanceof AddRouteException) {
                        return ((AddRouteException) e).matchResult;
                    }
                    return MatchResult.ERROR;
                } else {
                    return MatchResult.OK;
                }
            });
    }

    @Override
    public CompletableFuture<UnmatchResult> removeTopicRoute(long reqId,
                                                             String tenantId,
                                                             String topicFilter,
                                                             long incarnation,
                                                             String channelId) {
        assert !TopicUtil.isSharedSubscription(topicFilter);
        int bucketId = topicFilterBucketId(channelId);
        CompletableFuture<LocalRoutes> toReturn =
            routeMap.computeIfPresent(new TopicFilter(tenantId, topicFilter, bucketId), (k, v) -> {
                CompletableFuture<LocalRoutes> updated = new CompletableFuture<>();
                v.whenComplete((localRoutes, e) -> {
                    if (e != null) {
                        updated.completeExceptionally(e);
                    } else {
                        localRoutes.routesInfo.remove(channelId, incarnation);
                        if (localRoutes.routesInfo.isEmpty()) {
                            distClient.removeRoute(reqId,
                                    k.tenantId,
                                    TopicUtil.from(k.topicFilter),
                                    localRoutes.localReceiverId(),
                                    toDelivererKey(k.tenantId, localRoutes.localReceiverId(), serverId),
                                    0,
                                    localRoutes.incarnation())
                                .whenComplete((unmatchResult, t) -> {
                                    if (t != null) {
                                        updated.completeExceptionally(t);
                                    } else {
                                        // we use exception to return the dist unmatch call result
                                        updated.completeExceptionally(new RemoveRouteException(unmatchResult));
                                    }
                                });
                        } else {
                            updated.complete(localRoutes);
                        }
                    }
                });
                return updated;
            });
        if (toReturn == null) {
            // no route found
            return CompletableFuture.completedFuture(UnmatchResult.OK);
        }
        return toReturn
            .handle((r, e) -> {
                if (e != null) {
                    routeMap.remove(new TopicFilter(tenantId, topicFilter, bucketId), toReturn);
                    if (e instanceof RemoveRouteException) {
                        // we use exception to return the unmatch result
                        return ((RemoveRouteException) e).unmatchResult;
                    }
                    // if any exception occurs, we treat it as an error
                    return UnmatchResult.ERROR;
                } else {
                    return UnmatchResult.OK;
                }
            });
    }

    @Override
    public Optional<CompletableFuture<? extends ILocalRoutes>> getTopicRoutes(String tenantId, MatchInfo matchInfo) {
        int bucketId = LocalRoutes.parseBucketId(matchInfo.getReceiverId());
        CompletableFuture<? extends ILocalRoutes> routesFuture =
            routeMap.get(new TopicFilter(tenantId, matchInfo.getMatcher().getMqttTopicFilter(), bucketId));
        return Optional.ofNullable(routesFuture);
    }

    private int topicFilterBucketId(String key) {
        int bucketId = key.hashCode() % TOPIC_FILTER_BUCKET_NUM;
        if (bucketId < 0) {
            bucketId = (bucketId + TOPIC_FILTER_BUCKET_NUM) % TOPIC_FILTER_BUCKET_NUM;
        }
        return bucketId;
    }

    private record TopicFilter(String tenantId, String topicFilter, int bucketId) {
    }

    private static class LocalRoutes implements ILocalRoutes {
        private static final int BUCKET_ID_LENGTH = 5;
        private final String localReceiverId;
        private final Map<String, Long> routesInfo = new ConcurrentHashMap<>();
        private final long incarnation = System.nanoTime();

        private LocalRoutes(int bucketId) {
            this.localReceiverId =
                ILocalDistService.localize(String.format("%0" + BUCKET_ID_LENGTH + "d", bucketId) + incarnation);
        }

        public static int parseBucketId(String localReceiverId) {
            String receiverId = ILocalDistService.parseReceiverId(localReceiverId);
            return Integer.parseInt(receiverId.substring(0, BUCKET_ID_LENGTH));
        }

        public String localReceiverId() {
            return localReceiverId;
        }

        @Override
        public Map<String, Long> routesInfo() {
            return routesInfo;
        }

        @Override
        public long incarnation() {
            return incarnation;
        }
    }

    private static class AddRouteException extends RuntimeException {
        final MatchResult matchResult;

        private AddRouteException(MatchResult matchResult) {
            this.matchResult = matchResult;
        }
    }

    private static class RemoveRouteException extends RuntimeException {
        final UnmatchResult unmatchResult;

        private RemoveRouteException(UnmatchResult unmatchResult) {
            this.unmatchResult = unmatchResult;
        }
    }
}
