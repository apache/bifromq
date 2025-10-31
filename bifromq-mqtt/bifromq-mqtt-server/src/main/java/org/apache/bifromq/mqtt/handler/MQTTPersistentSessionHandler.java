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

import static org.apache.bifromq.inbox.util.InboxServiceUtil.getDelivererKey;
import static org.apache.bifromq.inbox.util.InboxServiceUtil.receiverId;
import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentSubCount;
import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentSubLatency;
import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentUnsubCount;
import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentUnsubLatency;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS0InternalLatency;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS1InternalLatency;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS2InternalLatency;
import static org.apache.bifromq.mqtt.handler.IMQTTProtocolHelper.SubResult.EXCEED_LIMIT;
import static org.apache.bifromq.mqtt.handler.IMQTTProtocolHelper.UnsubResult.ERROR;
import static org.apache.bifromq.mqtt.utils.AuthUtil.buildSubAction;
import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalPersistentSessionSpaceBytes;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalPersistentSessions;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalPersistentSubscribePerSecond;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalPersistentSubscriptions;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalPersistentUnsubscribePerSecond;
import static org.apache.bifromq.type.QoS.AT_LEAST_ONCE;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.base.util.AsyncRetry;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.inbox.rpc.proto.CommitRequest;
import org.apache.bifromq.inbox.rpc.proto.DetachReply;
import org.apache.bifromq.inbox.rpc.proto.DetachRequest;
import org.apache.bifromq.inbox.rpc.proto.SubRequest;
import org.apache.bifromq.inbox.rpc.proto.UnsubRequest;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import org.apache.bifromq.inbox.storage.proto.InboxMessage;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.mqtt.handler.condition.Condition;
import org.apache.bifromq.mqtt.handler.record.ProtocolResponse;
import org.apache.bifromq.mqtt.session.IMQTTPersistentSession;
import org.apache.bifromq.plugin.eventcollector.OutOfTenantResource;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ByClient;
import org.apache.bifromq.retain.rpc.proto.MatchReply;
import org.apache.bifromq.retain.rpc.proto.MatchRequest;
import org.apache.bifromq.sysprops.props.DataPlaneMaxBurstLatencyMillis;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.TopicFilterOption;
import org.apache.bifromq.type.TopicMessage;
import org.apache.bifromq.util.TopicUtil;

/**
 * Abstract handler for MQTT persistent session.
 */
@Slf4j
public abstract class MQTTPersistentSessionHandler extends MQTTSessionHandler implements IMQTTPersistentSession {
    private final int sessionExpirySeconds;
    private final InboxVersion inboxVersion;
    private final NavigableMap<Long, StagingMessage> stagingBuffer = new TreeMap<>();
    private final IInboxClient inboxClient;
    private final Cache<String, AtomicReference<Long>> qoS0TimestampsByMQTTPublisher = Caffeine.newBuilder()
        .expireAfterAccess(2 * DataPlaneMaxBurstLatencyMillis.INSTANCE.get(), TimeUnit.MILLISECONDS)
        .build();
    private final Cache<String, AtomicReference<Long>> qoS12TimestampsByMQTTPublisher = Caffeine.newBuilder()
        .expireAfterAccess(2 * DataPlaneMaxBurstLatencyMillis.INSTANCE.get(), TimeUnit.MILLISECONDS)
        .build();
    private boolean qos0Confirming = false;
    private boolean inboxConfirming = false;
    private long nextSendSeq = 0;
    private long qos0ConfirmUpToSeq;
    private long inboxConfirmedUpToSeq = -1;
    private IInboxClient.IInboxReader inboxReader;
    private State state = State.INIT;
    private ScheduledFuture<?> confirmTimeout;
    private ScheduledFuture<?> hintTimeout;
    private int currentHint = 1;

    protected MQTTPersistentSessionHandler(TenantSettings settings,
                                           ITenantMeter tenantMeter,
                                           Condition oomCondition,
                                           String userSessionId,
                                           int keepAliveTimeSeconds,
                                           int sessionExpirySeconds,
                                           ClientInfo clientInfo,
                                           InboxVersion inboxVersion,
                                           LWT noDelayLWT, // nullable
                                           ChannelHandlerContext ctx) {
        super(settings, tenantMeter, oomCondition, userSessionId, keepAliveTimeSeconds, clientInfo, noDelayLWT, ctx);
        this.inboxVersion = inboxVersion;
        this.inboxClient = sessionCtx.inboxClient;
        this.sessionExpirySeconds = sessionExpirySeconds;
    }

    @Override
    protected void doOnServerShuttingDown() {
        if (state == State.ATTACHED) {
            state = State.SERVER_SHUTTING_DOWN;
        }
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        super.handlerAdded(ctx);
        if (inboxVersion.getMod() == 0) {
            // check resource
            if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalPersistentSessions)) {
                handleProtocolResponse(helper().onResourceExhaustedDisconnect(TotalPersistentSessions));
                return;
            }
            if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalPersistentSessionSpaceBytes)) {
                handleProtocolResponse(helper().onResourceExhaustedDisconnect(TotalPersistentSessionSpaceBytes));
                return;
            }
        }
        setupInboxReader();
    }

    @Override
    public void doTearDown(ChannelHandlerContext ctx) {
        if (hintTimeout != null && !hintTimeout.isCancelled()) {
            hintTimeout.cancel(false);
        }
        if (inboxReader != null) {
            inboxReader.close();
        }
        int remainInboxSize =
            stagingBuffer.values().stream().reduce(0, (acc, msg) -> acc + msg.message.estBytes(), Integer::sum);
        if (remainInboxSize > 0) {
            memUsage.addAndGet(-remainInboxSize);
        }
        switch (state) {
            case ATTACHED -> detach(DetachRequest.newBuilder()
                .setReqId(System.nanoTime())
                .setInboxId(userSessionId)
                .setVersion(inboxVersion)
                .setExpirySeconds(sessionExpirySeconds)
                .setDiscardLWT(false)
                .setClient(clientInfo)
                .setNow(HLC.INST.getPhysical())
                .build());
            case SERVER_SHUTTING_DOWN -> detach(DetachRequest.newBuilder()
                .setReqId(System.nanoTime())
                .setInboxId(userSessionId)
                .setVersion(inboxVersion)
                .setExpirySeconds(sessionExpirySeconds)
                .setDiscardLWT(true)
                .setClient(clientInfo)
                .setNow(HLC.INST.getPhysical())
                .build());
            default -> {
                // do not detach on other cases
            }
        }
    }

    @Override
    protected final ProtocolResponse handleDisconnect(MqttMessage message) {
        if (state == State.SERVER_SHUTTING_DOWN) {
            return ProtocolResponse.responseNothing();
        }
        int requestSEI = helper().sessionExpiryIntervalOnDisconnect(message).orElse(sessionExpirySeconds);
        int finalSEI = Integer.compareUnsigned(requestSEI, settings.maxSEI) < 0 ? requestSEI : settings.maxSEI;
        if (helper().isNormalDisconnect(message)) {
            discardLWT();
            if (finalSEI == 0) {
                // expire without triggering Will Message if any
                detach(DetachRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setInboxId(userSessionId)
                    .setVersion(inboxVersion)
                    .setExpirySeconds(0)
                    .setDiscardLWT(true)
                    .setClient(clientInfo)
                    .setNow(HLC.INST.getPhysical())
                    .build());
            } else {
                // update inbox with requested SEI and discard will message
                detach(DetachRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setInboxId(userSessionId)
                    .setVersion(inboxVersion)
                    .setExpirySeconds(finalSEI)
                    .setDiscardLWT(true)
                    .setClient(clientInfo)
                    .setNow(HLC.INST.getPhysical())
                    .build());
            }
        } else if (helper().isDisconnectWithLWT(message)) {
            detach(DetachRequest.newBuilder()
                .setReqId(System.nanoTime())
                .setInboxId(userSessionId)
                .setVersion(inboxVersion)
                .setExpirySeconds(finalSEI)
                .setDiscardLWT(false)
                .setClient(clientInfo)
                .setNow(HLC.INST.getPhysical())
                .build());
        }
        return ProtocolResponse.goAwayNow(getLocal(ByClient.class).clientInfo(clientInfo));
    }

    private void detach(DetachRequest request) {
        if (state != State.ATTACHED && state != State.SERVER_SHUTTING_DOWN) {
            return;
        }
        state = State.DETACH;
        addBgTask(AsyncRetry.exec(() -> inboxClient.detach(request),
            (reply, t) -> {
                if (reply != null) {
                    return reply.getCode() == DetachReply.Code.TRY_LATER;
                }
                return false;
            }, sessionCtx.retryTimeoutNanos / 5, sessionCtx.retryTimeoutNanos));
    }

    @Override
    protected final CompletableFuture<IMQTTProtocolHelper.SubResult> subTopicFilter(long reqId,
                                                                                    String topicFilter,
                                                                                    TopicFilterOption option) {
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalPersistentSubscriptions)) {
            eventCollector.report(getLocal(OutOfTenantResource.class)
                .reason(TotalPersistentSubscriptions.name())
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(EXCEED_LIMIT);
        }
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalPersistentSubscribePerSecond)) {
            eventCollector.report(getLocal(OutOfTenantResource.class)
                .reason(TotalPersistentSubscribePerSecond.name())
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(EXCEED_LIMIT);
        }
        tenantMeter.recordCount(MqttPersistentSubCount);
        Timer.Sample start = Timer.start();
        return inboxClient.sub(SubRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(clientInfo.getTenantId())
                .setInboxId(userSessionId)
                .setVersion(inboxVersion)
                .setTopicFilter(topicFilter)
                .setMaxTopicFilters(settings.maxTopicFiltersPerInbox)
                .setOption(option)
                .setNow(HLC.INST.getPhysical())
                .build())
            .thenApplyAsync(v -> {
                switch (v.getCode()) {
                    case OK -> {
                        start.stop(tenantMeter.timer(MqttPersistentSubLatency));
                        return IMQTTProtocolHelper.SubResult.OK;
                    }
                    case EXISTS -> {
                        start.stop(tenantMeter.timer(MqttPersistentSubLatency));
                        return IMQTTProtocolHelper.SubResult.EXISTS;
                    }
                    case EXCEED_LIMIT -> {
                        return IMQTTProtocolHelper.SubResult.EXCEED_LIMIT;
                    }
                    case NO_INBOX, CONFLICT -> {
                        state = State.TERMINATE;
                        handleProtocolResponse(helper().onInboxTransientError(v.getCode().name()));
                    }
                    case BACK_PRESSURE_REJECTED -> {
                        return IMQTTProtocolHelper.SubResult.BACK_PRESSURE_REJECTED;
                    }
                    case TRY_LATER -> {
                        return IMQTTProtocolHelper.SubResult.TRY_LATER;
                    }
                    case ERROR -> {
                        return IMQTTProtocolHelper.SubResult.ERROR;
                    }
                    default -> {
                        // never happens
                    }
                }
                return IMQTTProtocolHelper.SubResult.ERROR;
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
                .setReceiverId(receiverId(userSessionId, inboxVersion.getIncarnation()))
                .setIncarnation(option.getIncarnation())
                .build())
            .setDelivererKey(getDelivererKey(tenantId, userSessionId))
            .setBrokerId(inboxClient.id())
            .setLimit(settings.retainMatchLimit)
            .build());
    }

    @Override
    protected final CompletableFuture<IMQTTProtocolHelper.UnsubResult> unsubTopicFilter(long reqId,
                                                                                        String topicFilter) {
        // check unsub rate
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalPersistentUnsubscribePerSecond)) {
            eventCollector.report(getLocal(OutOfTenantResource.class)
                .reason(TotalPersistentUnsubscribePerSecond.name())
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(ERROR);
        }

        tenantMeter.recordCount(MqttPersistentUnsubCount);
        Timer.Sample start = Timer.start();
        return inboxClient.unsub(UnsubRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(clientInfo.getTenantId())
                .setInboxId(userSessionId)
                .setVersion(inboxVersion)
                .setTopicFilter(topicFilter)
                .setNow(HLC.INST.getPhysical())
                .build())
            .thenApplyAsync(v -> {
                switch (v.getCode()) {
                    case OK -> {
                        start.stop(tenantMeter.timer(MqttPersistentUnsubLatency));
                        return IMQTTProtocolHelper.UnsubResult.OK;
                    }
                    case NO_SUB -> {
                        start.stop(tenantMeter.timer(MqttPersistentUnsubLatency));
                        return IMQTTProtocolHelper.UnsubResult.NO_SUB;
                    }
                    case NO_INBOX, CONFLICT -> {
                        state = State.TERMINATE;
                        handleProtocolResponse(helper().onInboxTransientError(v.getCode().name()));
                        return IMQTTProtocolHelper.UnsubResult.ERROR;
                    }
                    case BACK_PRESSURE_REJECTED -> {
                        return IMQTTProtocolHelper.UnsubResult.BACK_PRESSURE_REJECTED;
                    }
                    case TRY_LATER -> {
                        return IMQTTProtocolHelper.UnsubResult.TRY_LATER;
                    }
                    default -> {
                        return IMQTTProtocolHelper.UnsubResult.ERROR;
                    }
                }
            }, ctx.executor());
    }

    private void setupInboxReader() {
        state = State.ATTACHED;
        if (!ctx.channel().isActive()) {
            return;
        }
        inboxReader = inboxClient.openInboxReader(clientInfo().getTenantId(), userSessionId,
            inboxVersion.getIncarnation());
        inboxReader.fetch(this::consume);
        currentHint = clientReceiveQuota();
        scheduleHintTimeout();
        inboxReader.hint(currentHint);
        // resume channel read after inbox being setup
        onInitialized();
        resumeChannelRead();
    }

    private void scheduleHintTimeout() {
        hintTimeout = ctx.executor().schedule(() -> {
            inboxReader.hint(currentHint);
            scheduleHintTimeout();
        }, ThreadLocalRandom.current().nextLong(15, 45), TimeUnit.SECONDS);
    }

    private void scheduleConfirmTimeout(long upToSeq) {
        confirmTimeout = ctx.executor().schedule(() -> {
            if (upToSeq < inboxConfirmedUpToSeq) {
                confirmSendBuffer();
            }
        }, ThreadLocalRandom.current().nextLong(15, 45), TimeUnit.SECONDS);
    }

    private void confirmQoS0() {
        if (qos0Confirming) {
            return;
        }
        qos0Confirming = true;
        long upToSeq = qos0ConfirmUpToSeq;
        addBgTask(inboxClient.commit(CommitRequest.newBuilder()
            .setReqId(HLC.INST.get())
            .setTenantId(clientInfo.getTenantId())
            .setInboxId(userSessionId)
            .setVersion(inboxVersion)
            .setQos0UpToSeq(upToSeq)
            .setNow(HLC.INST.getPhysical())
            .build()))
            .thenAcceptAsync(v -> {
                switch (v.getCode()) {
                    case OK -> {
                        qos0Confirming = false;
                        if (upToSeq < qos0ConfirmUpToSeq) {
                            confirmQoS0();
                        }
                    }
                    case NO_INBOX, CONFLICT -> {
                        state = State.TERMINATE;
                        handleProtocolResponse(helper().onInboxTransientError(v.getCode().name()));
                    }
                    case BACK_PRESSURE_REJECTED -> qos0Confirming = false;
                    case TRY_LATER -> {
                        // try again with same version
                        qos0Confirming = false;
                        if (upToSeq < qos0ConfirmUpToSeq) {
                            confirmQoS0();
                        }
                    }
                    default -> {
                        // never happens
                    }
                }
            }, ctx.executor());
    }

    @Override
    protected final void onConfirm(long seq) {
        NavigableMap<Long, StagingMessage> confirmedMsgs = stagingBuffer.headMap(seq, true);
        for (StagingMessage stagingMessage : confirmedMsgs.values()) {
            RoutedMessage confirmed = stagingMessage.message;
            memUsage.addAndGet(-confirmed.estBytes());
            if (stagingMessage.batchEnd() && inboxConfirmedUpToSeq < confirmed.inboxPos()) {
                // for multiple topic filters matched message, confirm to upstream when at lease one is confirmed by client
                inboxConfirmedUpToSeq = confirmed.inboxPos();
                confirmSendBuffer();
            }
        }
        confirmedMsgs.clear();
        currentHint = clientReceiveQuota();
        inboxReader.hint(currentHint);
        ctx.executor().execute(this::drainStaging);
    }

    private void confirmSendBuffer() {
        if (confirmTimeout != null && !confirmTimeout.isCancelled()) {
            confirmTimeout.cancel(false);
        }
        if (inboxConfirming) {
            return;
        }
        inboxConfirming = true;
        long upToSeq = inboxConfirmedUpToSeq;
        addFgTask(inboxClient.commit(CommitRequest.newBuilder()
            .setReqId(HLC.INST.get())
            .setTenantId(clientInfo.getTenantId())
            .setInboxId(userSessionId)
            .setVersion(inboxVersion)
            .setSendBufferUpToSeq(upToSeq)
            .setNow(HLC.INST.getPhysical())
            .build()))
            .thenAcceptAsync(v -> {
                switch (v.getCode()) {
                    case OK -> {
                        inboxConfirming = false;
                        if (upToSeq < inboxConfirmedUpToSeq) {
                            confirmSendBuffer();
                        } else {
                            currentHint = clientReceiveQuota();
                            inboxReader.hint(currentHint);
                        }
                    }
                    case NO_INBOX, CONFLICT ->
                        handleProtocolResponse(helper().onInboxTransientError(v.getCode().name()));
                    case BACK_PRESSURE_REJECTED -> {
                        inboxConfirming = false;
                        if (upToSeq < inboxConfirmedUpToSeq) {
                            scheduleConfirmTimeout(upToSeq);
                        }
                    }
                    case TRY_LATER -> {
                        // try again with same version
                        inboxConfirming = false;
                        if (upToSeq < inboxConfirmedUpToSeq) {
                            confirmSendBuffer();
                        } else {
                            currentHint = clientReceiveQuota();
                            inboxReader.hint(currentHint);
                            scheduleHintTimeout();
                        }
                    }
                    default -> {
                        // never happens
                    }
                }
            }, ctx.executor());
    }

    private void consume(Fetched fetched) {
        ctx.executor().execute(() -> {
            if (hintTimeout != null && !hintTimeout.isCancelled()) {
                hintTimeout.cancel(false);
            }
            switch (fetched.getResult()) {
                case OK -> {
                    // deal with qos0
                    if (fetched.getQos0MsgCount() > 0) {
                        fetched.getQos0MsgList().forEach(this::pubQoS0Message);
                        flush(true);
                        // commit immediately
                        qos0ConfirmUpToSeq = fetched.getQos0Msg(fetched.getQos0MsgCount() - 1).getSeq();
                        confirmQoS0();
                    }
                    // deal with buffered message
                    if (fetched.getSendBufferMsgCount() > 0) {
                        for (int i = 0; i < fetched.getSendBufferMsgCount(); i++) {
                            InboxMessage inboxMessage = fetched.getSendBufferMsg(i);
                            this.pubBufferedMessage(inboxMessage, i + 1 == fetched.getSendBufferMsgCount());
                        }
                    }
                }
                case BACK_PRESSURE_REJECTED -> {
                    currentHint = clientReceiveQuota();
                    scheduleHintTimeout();
                }
                case TRY_LATER -> {
                    currentHint = clientReceiveQuota();
                    scheduleHintTimeout();
                    inboxReader.hint(currentHint);
                }
                case NO_INBOX, ERROR ->
                    handleProtocolResponse(helper().onInboxTransientError(fetched.getResult().name()));
                default -> {
                    // never happens
                }
            }
        });
    }

    private void pubQoS0Message(InboxMessage inboxMsg) {
        boolean isDup = isDuplicateMessage(inboxMsg.getMsg().getPublisher(), inboxMsg.getMsg().getMessage(),
            qoS0TimestampsByMQTTPublisher);
        inboxMsg.getMatchedTopicFilterMap()
            .forEach((topicFilter, option) -> pubQoS0Message(topicFilter, option, inboxMsg.getMsg(), isDup));
    }

    private void pubQoS0Message(String topicFilter, TopicFilterOption option, TopicMessage topicMsg, boolean isDup) {
        addFgTask(authProvider.checkPermission(clientInfo(), buildSubAction(topicFilter, option.getQos()))).thenAccept(
            checkResult -> {
                String topic = topicMsg.getTopic();
                Message message = topicMsg.getMessage();
                ClientInfo publisher = topicMsg.getPublisher();
                long now = HLC.INST.get();
                tenantMeter.timer(MqttQoS0InternalLatency)
                    .record(HLC.INST.getPhysical(now - message.getTimestamp()), TimeUnit.MILLISECONDS);
                sendQoS0SubMessage(new RoutedMessage(topic, message, publisher,
                    topicFilter, option, now, checkResult.hasGranted(), isDup));
            });
    }

    private void pubBufferedMessage(InboxMessage inboxMsg, boolean batchEnd) {
        boolean isDup = isDuplicateMessage(inboxMsg.getMsg().getPublisher(), inboxMsg.getMsg().getMessage(),
            qoS12TimestampsByMQTTPublisher);
        int i = 0;
        for (Map.Entry<String, TopicFilterOption> entry : inboxMsg.getMatchedTopicFilterMap().entrySet()) {
            String topicFilter = entry.getKey();
            TopicFilterOption option = entry.getValue();
            long seq = inboxMsg.getSeq();
            pubBufferedMessage(topicFilter, option, seq + i++, seq, inboxMsg.getMsg(), isDup, batchEnd);
        }
    }

    private void pubBufferedMessage(String topicFilter,
                                    TopicFilterOption option,
                                    long seq,
                                    long inboxSeq,
                                    TopicMessage topicMsg,
                                    boolean isDup,
                                    boolean batchEnd) {
        if (seq < nextSendSeq) {
            // do not buffer message that has been sent
            return;
        }
        addFgTask(authProvider.checkPermission(clientInfo(), buildSubAction(topicFilter, option.getQos())))
            .thenAccept(checkResult -> {
                String topic = topicMsg.getTopic();
                Message message = topicMsg.getMessage();
                ClientInfo publisher = topicMsg.getPublisher();
                long now = HLC.INST.get();
                RoutedMessage msg = new RoutedMessage(topic, message, publisher, topicFilter, option, now,
                    checkResult.hasGranted(), isDup, inboxSeq);
                tenantMeter.timer(msg.qos() == AT_LEAST_ONCE ? MqttQoS1InternalLatency : MqttQoS2InternalLatency)
                    .record(HLC.INST.getPhysical(now - message.getTimestamp()), TimeUnit.MILLISECONDS);
                StagingMessage prev = stagingBuffer.put(seq, new StagingMessage(msg, batchEnd));
                if (prev == null) {
                    memUsage.addAndGet(msg.estBytes());
                }
                if (ctx.executor().inEventLoop()) {
                    this.drainStaging();
                } else {
                    ctx.executor().execute(this::drainStaging);
                }
            });
    }

    private void drainStaging() {
        SortedMap<Long, StagingMessage> toBeSent = stagingBuffer.tailMap(nextSendSeq);
        if (toBeSent.isEmpty()) {
            return;
        }
        Iterator<Map.Entry<Long, StagingMessage>> itr = toBeSent.entrySet().iterator();
        while (clientReceiveQuota() > 0 && itr.hasNext()) {
            Map.Entry<Long, StagingMessage> entry = itr.next();
            long seq = entry.getKey();
            sendConfirmableSubMessage(seq, entry.getValue().message);
            nextSendSeq = seq + 1;
        }
        flush(true);
    }

    private enum State {
        INIT,
        ATTACHED,
        DETACH,
        SERVER_SHUTTING_DOWN,
        TERMINATE
    }

    private record StagingMessage(RoutedMessage message, boolean batchEnd) {

    }
}
