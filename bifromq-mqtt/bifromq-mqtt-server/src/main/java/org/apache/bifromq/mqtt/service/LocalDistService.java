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

package org.apache.bifromq.mqtt.service;

import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalTransientFanOutBytesPerSeconds;
import static org.apache.bifromq.metrics.TenantMetric.MqttTransientFanOutBytes;
import static org.apache.bifromq.mqtt.inbox.util.DelivererKeyUtil.toDelivererKey;

import org.apache.bifromq.mqtt.session.IMQTTSession;
import org.apache.bifromq.mqtt.session.IMQTTTransientSession;
import org.apache.bifromq.plugin.subbroker.CheckReply;
import org.apache.bifromq.plugin.subbroker.DeliveryPack;
import org.apache.bifromq.plugin.subbroker.DeliveryPackage;
import org.apache.bifromq.plugin.subbroker.DeliveryReply;
import org.apache.bifromq.plugin.subbroker.DeliveryRequest;
import org.apache.bifromq.plugin.subbroker.DeliveryResult;
import org.apache.bifromq.plugin.subbroker.DeliveryResults;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.dist.client.MatchResult;
import org.apache.bifromq.dist.client.UnmatchResult;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.type.TopicMessagePack;
import org.apache.bifromq.util.SizeUtil;
import org.apache.bifromq.util.TopicUtil;

@Slf4j
public class LocalDistService implements ILocalDistService {
    private final String serverId;
    private final IDistClient distClient;
    private final ILocalTopicRouter localTopicRouter;
    private final IResourceThrottler resourceThrottler;
    private final ILocalSessionRegistry sessionRegistry;


    public LocalDistService(String serverId, ILocalSessionRegistry sessionRegistry, ILocalTopicRouter localTopicRouter,
                            IDistClient distClient, IResourceThrottler resourceThrottler) {
        this.serverId = serverId;
        this.sessionRegistry = sessionRegistry;
        this.localTopicRouter = localTopicRouter;
        this.distClient = distClient;
        this.resourceThrottler = resourceThrottler;
    }

    @Override
    public CompletableFuture<MatchResult> match(long reqId, String topicFilter, long incarnation,
                                                IMQTTTransientSession session) {
        String tenantId = session.clientInfo().getTenantId();
        if (TopicUtil.isSharedSubscription(topicFilter)) {
            return distClient.addRoute(reqId, tenantId, TopicUtil.from(topicFilter),
                ILocalDistService.globalize(session.channelId()),
                toDelivererKey(tenantId, ILocalDistService.globalize(session.channelId()), serverId), 0, incarnation);
        } else {
            return localTopicRouter.addTopicRoute(reqId, tenantId, topicFilter, incarnation, session.channelId());
        }
    }

    @Override
    public CompletableFuture<UnmatchResult> unmatch(long reqId, String topicFilter, long incarnation,
                                                    IMQTTTransientSession session) {
        String tenantId = session.clientInfo().getTenantId();
        if (TopicUtil.isSharedSubscription(topicFilter)) {
            return distClient.removeRoute(reqId, tenantId, TopicUtil.from(topicFilter),
                ILocalDistService.globalize(session.channelId()),
                toDelivererKey(tenantId, ILocalDistService.globalize(session.channelId()), serverId), 0, incarnation);
        } else {
            return localTopicRouter.removeTopicRoute(reqId, tenantId, topicFilter, incarnation, session.channelId());
        }
    }

    @Override
    public CompletableFuture<DeliveryReply> dist(DeliveryRequest request) {
        DeliveryReply.Builder replyBuilder = DeliveryReply.newBuilder().setCode(DeliveryReply.Code.OK);
        for (Map.Entry<String, DeliveryPackage> packageEntry : request.getPackageMap().entrySet()) {
            String tenantId = packageEntry.getKey();
            DeliveryResults.Builder resultsBuilder = DeliveryResults.newBuilder();
            ITenantMeter tenantMeter = ITenantMeter.get(tenantId);
            boolean isFanOutThrottled = !resourceThrottler.hasResource(tenantId, TotalTransientFanOutBytesPerSeconds);
            Set<MatchInfo> ok = new HashSet<>();
            Set<MatchInfo> skip = new HashSet<>();
            Set<MatchInfo> noSub = new HashSet<>();
            long totalFanOutBytes = 0L;
            for (DeliveryPack writePack : packageEntry.getValue().getPackList()) {
                TopicMessagePack topicMsgPack = writePack.getMessagePack();
                Map<IMQTTTransientSession, Map<IMQTTTransientSession.MatchedTopicFilter, MatchInfo>> matchedSessions =
                    new HashMap<>();
                for (MatchInfo matchInfo : writePack.getMatchInfoList()) {
                    if (ILocalDistService.isGlobal(matchInfo.getReceiverId())) {
                        IMQTTSession session =
                            sessionRegistry.get(ILocalDistService.parseReceiverId(matchInfo.getReceiverId()));
                        if (session instanceof IMQTTTransientSession) {
                            if (isFanOutThrottled && !matchedSessions.isEmpty()) {
                                skip.add(matchInfo);
                            } else {
                                matchedSessions.computeIfAbsent((IMQTTTransientSession) session, k -> new HashMap<>())
                                    .put(new IMQTTTransientSession.MatchedTopicFilter(
                                            matchInfo.getMatcher().getMqttTopicFilter(), matchInfo.getIncarnation()),
                                        matchInfo);
                            }
                        } else {
                            // no session found for shared subscription
                            noSub.add(matchInfo);
                        }
                    } else {
                        Optional<CompletableFuture<? extends ILocalTopicRouter.ILocalRoutes>> routesFutureOpt =
                            localTopicRouter.getTopicRoutes(tenantId, matchInfo);
                        if (routesFutureOpt.isEmpty()) {
                            noSub.add(matchInfo);
                            continue;
                        }
                        CompletableFuture<? extends ILocalTopicRouter.ILocalRoutes> routesFuture =
                            routesFutureOpt.get();
                        if (!routesFuture.isDone() || routesFuture.isCompletedExceptionally()) {
                            // skip the matchInfo if the route is not ready
                            skip.add(matchInfo);
                            continue;
                        }
                        ILocalTopicRouter.ILocalRoutes localRoutes = routesFuture.join();
                        if (!localRoutes.localReceiverId().equals(matchInfo.getReceiverId())) {
                            noSub.add(matchInfo);
                            continue;
                        }
                        for (Map.Entry<String, Long> route : localRoutes.routesInfo().entrySet()) {
                            String sessionId = route.getKey();
                            long incarnation = route.getValue();
                            // at least one session should publish the message
                            IMQTTSession session = sessionRegistry.get(sessionId);
                            if (session instanceof IMQTTTransientSession) {
                                if (isFanOutThrottled && !matchedSessions.isEmpty()) {
                                    skip.add(matchInfo);
                                } else {
                                    matchedSessions.computeIfAbsent((IMQTTTransientSession) session,
                                        k -> new HashMap<>()).put(new IMQTTTransientSession.MatchedTopicFilter(
                                        matchInfo.getMatcher().getMqttTopicFilter(), incarnation), matchInfo);
                                }
                            }
                        }
                    }
                }
                long msgPackSize = SizeUtil.estSizeOf(topicMsgPack);
                int fanoutScale = 0;
                for (Map.Entry<IMQTTTransientSession, Map<IMQTTTransientSession.MatchedTopicFilter, MatchInfo>> entry : matchedSessions.entrySet()) {
                    IMQTTTransientSession session = entry.getKey();
                    Map<IMQTTTransientSession.MatchedTopicFilter, MatchInfo> matchedTopics = entry.getValue();
                    Set<IMQTTTransientSession.MatchedTopicFilter> obsoleted =
                        session.publish(topicMsgPack, matchedTopics.keySet());
                    for (IMQTTTransientSession.MatchedTopicFilter matchedTopic : matchedTopics.keySet()) {
                        MatchInfo matchInfo = matchedTopics.get(matchedTopic);
                        if (obsoleted.contains(matchedTopic)) {
                            noSub.add(matchInfo);
                        } else {
                            ok.add(matchInfo);
                            fanoutScale++;
                        }
                    }
                }
                totalFanOutBytes += msgPackSize * fanoutScale;
            }
            tenantMeter.recordSummary(MqttTransientFanOutBytes, totalFanOutBytes);
            // don't include duplicated matchInfo in the result
            // treat skip as ok
            Sets.difference(Sets.union(ok, skip), noSub).forEach(matchInfo -> resultsBuilder.addResult(
                DeliveryResult.newBuilder().setMatchInfo(matchInfo).setCode(DeliveryResult.Code.OK).build()));
            noSub.forEach(matchInfo -> resultsBuilder.addResult(
                DeliveryResult.newBuilder().setMatchInfo(matchInfo).setCode(DeliveryResult.Code.NO_SUB).build()));
            replyBuilder.putResult(tenantId, resultsBuilder.build());
        }
        return CompletableFuture.completedFuture(replyBuilder.build());
    }

    @Override
    public CheckReply.Code checkMatchInfo(String tenantId, MatchInfo matchInfo) {
        if (ILocalDistService.isGlobal(matchInfo.getReceiverId())) {
            IMQTTSession session = sessionRegistry.get(ILocalDistService.parseReceiverId(matchInfo.getReceiverId()));
            if (session == null) {
                return CheckReply.Code.NO_RECEIVER;
            }
            if (session instanceof IMQTTTransientSession transientSession) {
                return transientSession.hasSubscribed(matchInfo.getMatcher().getMqttTopicFilter())
                    ? CheckReply.Code.OK : CheckReply.Code.NO_SUB;
            } else {
                // should not be here
                return CheckReply.Code.ERROR;
            }
        } else {
            Optional<CompletableFuture<? extends ILocalTopicRouter.ILocalRoutes>> routesFutureOpt =
                localTopicRouter.getTopicRoutes(tenantId, matchInfo);
            if (routesFutureOpt.isEmpty()) {
                return CheckReply.Code.NO_RECEIVER;
            }
            CompletableFuture<? extends ILocalTopicRouter.ILocalRoutes> routesFuture = routesFutureOpt.get();
            if (!routesFuture.isDone() || routesFuture.isCompletedExceptionally()) {
                return CheckReply.Code.OK;
            }
            ILocalTopicRouter.ILocalRoutes localRoutes = routesFuture.join();
            if (!localRoutes.localReceiverId().equals(matchInfo.getReceiverId())) {
                return CheckReply.Code.NO_RECEIVER;
            }
            if (localRoutes.routesInfo().isEmpty()) {
                return CheckReply.Code.NO_RECEIVER;
            }
            return CheckReply.Code.OK;
        }
    }
}
