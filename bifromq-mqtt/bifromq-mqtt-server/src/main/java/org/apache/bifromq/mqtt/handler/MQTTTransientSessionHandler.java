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

import static org.apache.bifromq.metrics.TenantMetric.MqttQoS0InternalLatency;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS1InternalLatency;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS2InternalLatency;
import static org.apache.bifromq.metrics.TenantMetric.MqttTransientSubCount;
import static org.apache.bifromq.metrics.TenantMetric.MqttTransientSubLatency;
import static org.apache.bifromq.metrics.TenantMetric.MqttTransientUnsubCount;
import static org.apache.bifromq.metrics.TenantMetric.MqttTransientUnsubLatency;
import static org.apache.bifromq.mqtt.handler.IMQTTProtocolHelper.SubResult.EXCEED_LIMIT;
import static org.apache.bifromq.mqtt.handler.IMQTTProtocolHelper.SubResult.EXISTS;
import static org.apache.bifromq.mqtt.handler.IMQTTProtocolHelper.SubResult.OK;
import static org.apache.bifromq.mqtt.handler.IMQTTProtocolHelper.UnsubResult.ERROR;
import static org.apache.bifromq.mqtt.inbox.util.DelivererKeyUtil.toDelivererKey;
import static org.apache.bifromq.mqtt.service.ILocalDistService.globalize;
import static org.apache.bifromq.mqtt.utils.AuthUtil.buildSubAction;
import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalTransientSubscribePerSecond;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalTransientSubscriptions;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalTransientUnsubscribePerSecond;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Sets;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.dist.client.MatchResult;
import org.apache.bifromq.dist.client.UnmatchResult;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.mqtt.handler.condition.Condition;
import org.apache.bifromq.mqtt.handler.record.ProtocolResponse;
import org.apache.bifromq.mqtt.session.IMQTTTransientSession;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.eventcollector.OutOfTenantResource;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ByClient;
import org.apache.bifromq.plugin.eventcollector.session.MQTTSessionStart;
import org.apache.bifromq.plugin.eventcollector.session.MQTTSessionStop;
import org.apache.bifromq.retain.rpc.proto.MatchReply;
import org.apache.bifromq.retain.rpc.proto.MatchRequest;
import org.apache.bifromq.sysprops.props.DataPlaneMaxBurstLatencyMillis;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicFilterOption;
import org.apache.bifromq.type.TopicMessagePack;
import org.apache.bifromq.util.TopicUtil;

@Slf4j
public abstract class MQTTTransientSessionHandler extends MQTTSessionHandler implements IMQTTTransientSession {
    // the topicFilters could be accessed concurrently
    private final Map<String, TopicFilterOption> topicFilters = new ConcurrentHashMap<>();
    private final NavigableMap<Long, RoutedMessage> inbox = new TreeMap<>();
    private final Cache<String, AtomicReference<Long>> latestMsgTsByMQTTPublisher = Caffeine.newBuilder()
        // we simply use 2 times of the burst latency as the expiration time
        // which is a rough estimation of stabilizing time of internal resource scheduling
        // during which the internal retry mechanism takes effect.
        .expireAfterAccess(2 * DataPlaneMaxBurstLatencyMillis.INSTANCE.get(), TimeUnit.MILLISECONDS)
        .build();
    private long nextSendSeq = 0;
    private long msgSeqNo = 0;
    private AtomicLong subNumGauge;

    protected MQTTTransientSessionHandler(TenantSettings settings,
                                          ITenantMeter tenantMeter,
                                          Condition oomCondition,
                                          String userSessionId,
                                          int keepAliveTimeSeconds,
                                          ClientInfo clientInfo,
                                          LWT willMessage, // nullable
                                          ChannelHandlerContext ctx) {
        super(settings, tenantMeter, oomCondition, userSessionId, keepAliveTimeSeconds, clientInfo, willMessage, ctx);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        super.handlerAdded(ctx);
        subNumGauge = sessionCtx.getTransientSubNumGauge(clientInfo.getTenantId());
        onInitialized();
        resumeChannelRead();
        // Transient session lifetime is bounded by the channel lifetime
        eventCollector.report(getLocal(MQTTSessionStart.class).sessionId(userSessionId).clientInfo(clientInfo));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        super.channelInactive(ctx);
        if (!topicFilters.isEmpty()) {
            topicFilters.forEach((topicFilter, option) -> addBgTask(unsubTopicFilter(System.nanoTime(), topicFilter)));
        }
        int remainInboxSize = inbox.values().stream().reduce(0, (acc, msg) -> acc + msg.estBytes(), Integer::sum);
        memUsage.addAndGet(-remainInboxSize);
        // Transient session lifetime is bounded by the channel lifetime
        eventCollector.report(getLocal(MQTTSessionStop.class).sessionId(userSessionId).clientInfo(clientInfo));
        ctx.fireChannelInactive();
    }

    @Override
    protected ProtocolResponse handleDisconnect(MqttMessage message) {
        Optional<Integer> requestSEI = helper().sessionExpiryIntervalOnDisconnect(message);
        if (requestSEI.isPresent() && requestSEI.get() > 0) {
            return helper().respondDisconnectProtocolError();
        }
        if (helper().isNormalDisconnect(message)) {
            discardLWT();
        }
        return ProtocolResponse.goAwayNow(getLocal(ByClient.class).clientInfo(clientInfo));
    }

    @Override
    protected final void onConfirm(long seq) {
        RoutedMessage msg = inbox.remove(seq);
        if (msg != null) {
            memUsage.addAndGet(-msg.estBytes());
        }
    }

    @Override
    protected final CompletableFuture<IMQTTProtocolHelper.SubResult> subTopicFilter(long reqId,
                                                                                    String topicFilter,
                                                                                    TopicFilterOption option) {
        // check resources
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalTransientSubscriptions)) {
            eventCollector.report(getLocal(OutOfTenantResource.class)
                .reason(TotalTransientSubscriptions.name())
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(EXCEED_LIMIT);
        }
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalTransientSubscribePerSecond)) {
            eventCollector.report(getLocal(OutOfTenantResource.class)
                .reason(TotalTransientSubscribePerSecond.name())
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(EXCEED_LIMIT);
        }
        tenantMeter.recordCount(MqttTransientSubCount);
        int maxTopicFiltersPerInbox = settings.maxTopicFiltersPerInbox;
        if (topicFilters.size() >= maxTopicFiltersPerInbox) {
            return CompletableFuture.completedFuture(EXCEED_LIMIT);
        }
        final TopicFilterOption prevOption = topicFilters.put(topicFilter, option);
        if (prevOption == null) {
            subNumGauge.addAndGet(1);
            memUsage.addAndGet(topicFilter.length());
            memUsage.addAndGet(option.getSerializedSize());
        }
        Timer.Sample start = Timer.start();
        return addMatchRecord(reqId, topicFilter, option.getIncarnation())
            .thenApplyAsync(matchResult -> {
                switch (matchResult) {
                    case OK -> {
                        start.stop(tenantMeter.timer(MqttTransientSubLatency));
                        if (prevOption == null) {
                            return OK;
                        } else {
                            return EXISTS;
                        }
                    }
                    case EXCEED_LIMIT -> {
                        return IMQTTProtocolHelper.SubResult.EXCEED_LIMIT;
                    }
                    case BACK_PRESSURE_REJECTED -> {
                        return IMQTTProtocolHelper.SubResult.BACK_PRESSURE_REJECTED;
                    }
                    case TRY_LATER -> {
                        return IMQTTProtocolHelper.SubResult.TRY_LATER;
                    }
                    default -> {
                        return IMQTTProtocolHelper.SubResult.ERROR;
                    }
                }
            }, ctx.executor());
    }

    @Override
    protected CompletableFuture<MatchReply> matchRetainedMessage(long reqId,
                                                                 String topicFilter,
                                                                 TopicFilterOption option) {
        String tenantId = clientInfo().getTenantId();
        return sessionCtx.retainClient.match(MatchRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setMatchInfo(MatchInfo.newBuilder()
                .setMatcher(TopicUtil.from(topicFilter))
                // receive retain message via global channel
                .setReceiverId(globalize(channelId()))
                .setIncarnation(option.getIncarnation())
                .build())
            .setDelivererKey(toDelivererKey(tenantId, globalize(channelId()), sessionCtx.serverId))
            .setBrokerId(0)
            .setLimit(settings.retainMatchLimit)
            .build());
    }

    @Override
    protected CompletableFuture<IMQTTProtocolHelper.UnsubResult> unsubTopicFilter(long reqId, String topicFilter) {
        // check unsub rate
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalTransientUnsubscribePerSecond)) {
            eventCollector.report(getLocal(OutOfTenantResource.class)
                .reason(TotalTransientUnsubscribePerSecond.name())
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(ERROR);
        }
        tenantMeter.recordCount(MqttTransientUnsubCount);
        Timer.Sample start = Timer.start();
        TopicFilterOption option = topicFilters.remove(topicFilter);
        if (option == null) {
            return CompletableFuture.completedFuture(IMQTTProtocolHelper.UnsubResult.NO_SUB);
        } else {
            subNumGauge.addAndGet(-1);
            memUsage.addAndGet(-topicFilter.length());
            memUsage.addAndGet(-option.getSerializedSize());
        }
        return removeMatchRecord(reqId, topicFilter, option.getIncarnation())
            .handleAsync((result, e) -> {
                if (e != null) {
                    return ERROR;
                } else {
                    switch (result) {
                        case OK -> {
                            start.stop(tenantMeter.timer(MqttTransientUnsubLatency));
                            return IMQTTProtocolHelper.UnsubResult.OK;
                        }
                        case BACK_PRESSURE_REJECTED -> {
                            return IMQTTProtocolHelper.UnsubResult.BACK_PRESSURE_REJECTED;
                        }
                        case TRY_LATER -> {
                            return IMQTTProtocolHelper.UnsubResult.TRY_LATER;
                        }
                        default -> {
                            return ERROR;
                        }
                    }
                }
            }, ctx.executor());
    }

    @Override
    public boolean hasSubscribed(String topicFilter) {
        return topicFilters.containsKey(topicFilter);
    }

    @Override
    public Set<MatchedTopicFilter> publish(TopicMessagePack messagePack, Set<MatchedTopicFilter> matchedTopicFilters) {
        if (!ctx.channel().isActive()) {
            return matchedTopicFilters;
        }
        Map<MatchedTopicFilter, TopicFilterOption> validTopicFilters = new HashMap<>();
        for (MatchedTopicFilter topicFilter : matchedTopicFilters) {
            TopicFilterOption option = topicFilters.get(topicFilter.topicFilter());
            if (option != null) {
                validTopicFilters.put(topicFilter, option);
                if (option.getIncarnation() > topicFilter.incarnation()) {
                    log.debug("Receive message from previous route: topicFilter={}, inc={}, prevInc={}",
                        topicFilter, option.getIncarnation(), topicFilter.incarnation());
                }
            }
        }
        ctx.executor().execute(() -> publish(validTopicFilters, messagePack));
        return Sets.difference(matchedTopicFilters, validTopicFilters.keySet());
    }

    private void publish(Map<MatchedTopicFilter, TopicFilterOption> matchedTopicFilters,
                         TopicMessagePack topicMsgPack) {
        CompletableFuture<?>[] checkPermissionFutures = new CompletableFuture[matchedTopicFilters.size()];
        List<TopicFilterAndPermission> topicFilterAndPermissions = new ArrayList<>(matchedTopicFilters.size());
        int i = 0;
        for (Map.Entry<MatchedTopicFilter, TopicFilterOption> entry : matchedTopicFilters.entrySet()) {
            MatchedTopicFilter mtf = entry.getKey();
            TopicFilterOption opt = entry.getValue();
            CompletableFuture<CheckResult> f =
                addFgTask(authProvider.checkPermission(clientInfo(), buildSubAction(mtf.topicFilter(), opt.getQos())));
            checkPermissionFutures[i++] = f;
            topicFilterAndPermissions.add(new TopicFilterAndPermission(mtf.topicFilter(), opt, f));
        }

        CompletableFuture.allOf(checkPermissionFutures)
            .thenAccept(v -> {
                for (TopicMessagePack.PublisherPack publisherPack : topicMsgPack.getMessageList()) {
                    publish(topicMsgPack.getTopic(), publisherPack.getPublisher(), publisherPack.getMessageList(),
                        topicFilterAndPermissions);
                }
            });
    }

    private void publish(String topic,
                         ClientInfo publisher,
                         List<Message> messages,
                         List<TopicFilterAndPermission> topicFilterAndPermissions) {
        AtomicInteger totalMsgBytesSize = new AtomicInteger();
        long now = HLC.INST.get();
        for (Message message : messages) {
            // deduplicate messages based on topic and publisher
            for (TopicFilterAndPermission tfp : topicFilterAndPermissions) {
                RoutedMessage subMsg = new RoutedMessage(topic, message, publisher, tfp.topicFilter, tfp.option, now,
                    tfp.permissionCheckFuture.join().hasGranted(),
                    isDuplicateMessage(publisher, message, latestMsgTsByMQTTPublisher));
                logInternalLatency(subMsg);
                if (subMsg.qos() == QoS.AT_MOST_ONCE) {
                    sendQoS0SubMessage(subMsg);
                } else {
                    inbox.put(msgSeqNo++, subMsg);
                    totalMsgBytesSize.addAndGet(subMsg.estBytes());
                }
            }
        }
        memUsage.addAndGet(totalMsgBytesSize.get());
        send();
    }

    private void send() {
        SortedMap<Long, RoutedMessage> toBeSent = inbox.tailMap(nextSendSeq);
        if (toBeSent.isEmpty()) {
            return;
        }
        Iterator<Map.Entry<Long, RoutedMessage>> itr = toBeSent.entrySet().iterator();
        while (clientReceiveQuota() > 0 && itr.hasNext()) {
            Map.Entry<Long, RoutedMessage> entry = itr.next();
            long seq = entry.getKey();
            RoutedMessage msg = entry.getValue();
            sendConfirmableSubMessage(seq, msg);
            nextSendSeq = seq + 1;
        }
    }

    private void logInternalLatency(RoutedMessage message) {
        Timer timer = switch (message.qos()) {
            case AT_MOST_ONCE -> tenantMeter.timer(MqttQoS0InternalLatency);
            case AT_LEAST_ONCE -> tenantMeter.timer(MqttQoS1InternalLatency);
            default -> tenantMeter.timer(MqttQoS2InternalLatency);
        };
        timer.record(HLC.INST.getPhysical(message.hlc() - message.message().getTimestamp()), TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<MatchResult> addMatchRecord(long reqId, String topicFilter, long incarnation) {
        return sessionCtx.localDistService.match(reqId, topicFilter, incarnation, this);
    }

    private CompletableFuture<UnmatchResult> removeMatchRecord(long reqId, String topicFilter, long incarnation) {
        return sessionCtx.localDistService.unmatch(reqId, topicFilter, incarnation, this);
    }

    private record TopicFilterAndPermission(String topicFilter,
                                            TopicFilterOption option,
                                            CompletableFuture<CheckResult> permissionCheckFuture) {
    }
}
