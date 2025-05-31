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

package org.apache.bifromq.mqtt.handler;

import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainMatchBytesPerSecond;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainMatchPerSeconds;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainMessageSpaceBytes;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainTopics;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainedBytesPerSecond;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainedMessagesPerSeconds;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalSharedSubscriptions;
import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.bifromq.inbox.storage.proto.RetainHandling.SEND_AT_SUBSCRIBE;
import static org.apache.bifromq.inbox.storage.proto.RetainHandling.SEND_AT_SUBSCRIBE_IF_NOT_YET_EXISTS;
import static org.apache.bifromq.metrics.TenantMetric.MqttConnectCount;
import static org.apache.bifromq.metrics.TenantMetric.MqttDisconnectCount;
import static org.apache.bifromq.metrics.TenantMetric.MqttIngressBytes;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS0DistBytes;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS0IngressBytes;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS1DeliverBytes;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS1DistBytes;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS1ExternalLatency;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS1IngressBytes;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS2DeliverBytes;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS2DistBytes;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS2ExternalLatency;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS2IngressBytes;
import static org.apache.bifromq.metrics.TenantMetric.MqttTransientSubLatency;
import static org.apache.bifromq.mqtt.handler.IMQTTProtocolHelper.SubResult.EXCEED_LIMIT;
import static org.apache.bifromq.mqtt.handler.MQTTSessionIdUtil.packetId;
import static org.apache.bifromq.mqtt.handler.MQTTSessionIdUtil.userSessionId;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.messageExpiryInterval;
import static org.apache.bifromq.mqtt.utils.AuthUtil.buildPubAction;
import static org.apache.bifromq.mqtt.utils.AuthUtil.buildSubAction;
import static org.apache.bifromq.mqtt.utils.AuthUtil.buildUnsubAction;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_5_VALUE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;
import static org.apache.bifromq.type.QoS.AT_LEAST_ONCE;
import static org.apache.bifromq.type.QoS.AT_MOST_ONCE;
import static org.apache.bifromq.type.QoS.EXACTLY_ONCE;
import static org.apache.bifromq.util.TopicUtil.isSharedSubscription;
import static org.apache.bifromq.util.TopicUtil.isValidTopicFilter;
import static org.apache.bifromq.util.TopicUtil.isWildcardTopicFilter;

import org.apache.bifromq.plugin.authprovider.IAuthProvider;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.clientbalancer.Redirection;
import org.apache.bifromq.plugin.eventcollector.Event;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.OutOfTenantResource;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.PingReq;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.accessctrl.PubActionDisallow;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.accessctrl.SubActionDisallow;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.accessctrl.UnsubActionDisallow;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ByClient;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ClientChannelError;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InvalidTopicFilter;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.MalformedTopicFilter;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS0DistError;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS1DistError;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS1PubAckDropped;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS2DistError;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS2PubRecDropped;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.disthandling.WillDistError;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.disthandling.WillDisted;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.DropReason;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS0Dropped;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS0Pushed;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Confirmed;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Dropped;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Pushed;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Confirmed;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Dropped;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Pushed;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Received;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.retainhandling.MatchRetainError;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.retainhandling.MsgRetained;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.retainhandling.MsgRetainedError;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.retainhandling.RetainMsgCleared;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.subhandling.SubAcked;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.subhandling.UnsubAcked;
import org.apache.bifromq.retain.rpc.proto.MatchReply;
import org.apache.bifromq.retain.rpc.proto.RetainReply;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.Sets;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.base.util.FutureTracker;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.dist.client.PubResult;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.inbox.storage.proto.TopicFilterOption;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.mqtt.handler.condition.Condition;
import org.apache.bifromq.mqtt.handler.record.ProtocolResponse;
import org.apache.bifromq.mqtt.inbox.rpc.proto.SubReply;
import org.apache.bifromq.mqtt.inbox.rpc.proto.UnsubReply;
import org.apache.bifromq.mqtt.session.IMQTTSession;
import org.apache.bifromq.mqtt.session.MQTTSessionContext;
import org.apache.bifromq.mqtt.utils.IMQTTMessageSizer;
import org.apache.bifromq.sessiondict.client.ISessionRegistration;
import org.apache.bifromq.sessiondict.rpc.proto.ServerRedirection;
import org.apache.bifromq.sysprops.props.ClientRedirectCheckIntervalSeconds;
import org.apache.bifromq.sysprops.props.SanityCheckMqttUtf8String;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.MQTTClientInfoConstants;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.UserProperties;
import org.apache.bifromq.util.UTF8Util;

/**
 * The abstract class for MQTT session handler.
 */
@Slf4j
public abstract class MQTTSessionHandler extends MQTTMessageHandler implements IMQTTSession {
    protected static final boolean SANITY_CHECK = SanityCheckMqttUtf8String.INSTANCE.get();
    private static final int REDIRECT_CHECK_INTERVAL_SECONDS = ClientRedirectCheckIntervalSeconds.INSTANCE.get();
    protected final TenantSettings settings;
    protected final String userSessionId;
    protected final int keepAliveTimeSeconds;
    protected final ClientInfo clientInfo;
    protected final AtomicLong memUsage;
    protected final ITenantMeter tenantMeter;
    protected final ChannelHandlerContext ctx;
    protected final MQTTSessionContext sessionCtx;
    protected final IAuthProvider authProvider;
    protected final IEventCollector eventCollector;
    protected final IResourceThrottler resourceThrottler;
    private final Condition oomCondition;
    private final long idleTimeoutNanos;
    private final MPSThrottler throttler;
    private final Set<CompletableFuture<?>> fgTasks = new HashSet<>();
    private final FutureTracker bgTasks = new FutureTracker();
    private final Set<Integer> inUsePacketIds = new HashSet<>();
    private final IMQTTMessageSizer sizer;
    private final LinkedHashMap<Integer, ConfirmingMessage> unconfirmedPacketIds = new LinkedHashMap<>();
    private final TreeSet<ConfirmingMessage> resendQueue;
    private final CompletableFuture<Void> onInitialized = new CompletableFuture<>();
    private LWT noDelayLWT;
    private boolean isGoAway;
    private ScheduledFuture<?> idleTimeoutTask;
    private ScheduledFuture<?> redirectTask;
    private ISessionRegistration sessionRegistration;
    private long lastActiveAtNanos;
    private ScheduledFuture<?> resendTask;
    private int receivingCount = 0;

    protected MQTTSessionHandler(TenantSettings settings,
                                 ITenantMeter tenantMeter,
                                 Condition oomCondition,
                                 String userSessionId,
                                 int keepAliveTimeSeconds,
                                 ClientInfo clientInfo,
                                 @Nullable LWT noDelayLWT,
                                 ChannelHandlerContext ctx) {
        this.sizer = clientInfo.getMetadataOrDefault(MQTT_PROTOCOL_VER_KEY, "").equals(MQTT_PROTOCOL_VER_5_VALUE)
            ? IMQTTMessageSizer.mqtt5() : IMQTTMessageSizer.mqtt3();
        this.ctx = ctx;
        this.settings = settings;
        this.oomCondition = oomCondition;
        this.userSessionId = userSessionId;
        this.keepAliveTimeSeconds = keepAliveTimeSeconds;
        this.clientInfo = clientInfo;
        this.noDelayLWT = noDelayLWT;
        this.tenantMeter = tenantMeter;
        this.throttler = new MPSThrottler(settings.maxMsgPerSec);
        this.idleTimeoutNanos = Duration.ofMillis(keepAliveTimeSeconds * 1500L).toNanos(); // x1.5
        resendQueue = new TreeSet<>(Comparator.comparingLong(this::ackTimeoutNanos));
        sessionCtx = ChannelAttrs.mqttSessionContext(ctx);
        // strong reference to avoid gc
        memUsage = sessionCtx.getSessionMemGauge(clientInfo.getTenantId());
        authProvider = sessionCtx.authProvider(ctx);
        eventCollector = sessionCtx.eventCollector;
        resourceThrottler = sessionCtx.resourceThrottler;
    }

    private int estMemSize() {
        int s = 144; // base size from JOL
        s += userSessionId.length();
        s += clientInfo.getSerializedSize();
        if (noDelayLWT != null) {
            s += noDelayLWT.getSerializedSize();
        }
        return s;
    }

    protected abstract IMQTTProtocolHelper helper();

    @Override
    public final String channelId() {
        return clientInfo.getMetadataOrDefault(MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY, "");
    }

    @Override
    public final ClientInfo clientInfo() {
        return clientInfo;
    }

    @Override
    public final CompletableFuture<Void> disconnect() {
        ctx.executor().execute(() -> handleProtocolResponse(helper().onDisconnect()));
        return bgTasks.whenComplete((v, e) -> log.trace("All bg tasks finished: client={}", clientInfo));
    }

    @Override
    public final CompletableFuture<SubReply.Result> subscribe(long reqId, String topicFilter, QoS qos) {
        return CompletableFuture.completedFuture(true)
            .thenComposeAsync(v -> checkAndSubscribe(reqId, topicFilter, TopicFilterOption
                .newBuilder()
                .setQos(qos)
                .setIncarnation(HLC.INST.get())
                .build(), UserProperties.getDefaultInstance())
                .thenApply(subResult -> {
                    switch (subResult) {
                        case OK -> {
                            return SubReply.Result.OK;
                        }
                        case EXISTS -> {
                            return SubReply.Result.ERROR;
                        }
                        case EXCEED_LIMIT -> {
                            return SubReply.Result.EXCEED_LIMIT;
                        }
                        case NOT_AUTHORIZED -> {
                            return SubReply.Result.NOT_AUTHORIZED;
                        }
                        case TOPIC_FILTER_INVALID -> {
                            return SubReply.Result.TOPIC_FILTER_INVALID;
                        }
                        case WILDCARD_NOT_SUPPORTED -> {
                            return SubReply.Result.WILDCARD_NOT_SUPPORTED;
                        }
                        case SHARED_SUBSCRIPTION_NOT_SUPPORTED -> {
                            return SubReply.Result.SHARED_SUBSCRIPTION_NOT_SUPPORTED;
                        }
                        case SUBSCRIPTION_IDENTIFIER_NOT_SUPPORTED -> {
                            return SubReply.Result.SUBSCRIPTION_IDENTIFIER_NOT_SUPPORTED;
                        }
                        case BACK_PRESSURE_REJECTED -> {
                            return SubReply.Result.BACK_PRESSURE_REJECTED;
                        }
                        case TRY_LATER -> {
                            return SubReply.Result.TRY_LATER;
                        }
                        default -> {
                            return SubReply.Result.ERROR;
                        }
                    }
                }), ctx.executor());
    }

    @Override
    public final CompletableFuture<UnsubReply.Result> unsubscribe(long reqId, String topicFilter) {
        return CompletableFuture.completedFuture(true)
            .thenComposeAsync(v -> checkAndUnsubscribe(reqId, topicFilter, UserProperties.getDefaultInstance())
                .thenApply(unsubResult -> {
                    switch (unsubResult) {
                        case OK -> {
                            return UnsubReply.Result.OK;
                        }
                        case NO_SUB -> {
                            return UnsubReply.Result.NO_SUB;
                        }
                        case NOT_AUTHORIZED -> {
                            return UnsubReply.Result.NOT_AUTHORIZED;
                        }
                        case TOPIC_FILTER_INVALID -> {
                            return UnsubReply.Result.TOPIC_FILTER_INVALID;
                        }
                        case BACK_PRESSURE_REJECTED -> {
                            return UnsubReply.Result.BACK_PRESSURE_REJECTED;
                        }
                        case TRY_LATER -> {
                            return UnsubReply.Result.TRY_LATER;
                        }
                        default -> {
                            return UnsubReply.Result.ERROR;
                        }
                    }
                }), ctx.executor());
    }

    public final CompletableFuture<Void> awaitInitialized() {
        return onInitialized;
    }

    protected final void onInitialized() {
        onInitialized.complete(null);
    }

    protected final LWT willMessage() {
        return noDelayLWT;
    }

    protected final <T> CompletableFuture<T> addFgTask(CompletableFuture<T> taskFuture) {
        if (!taskFuture.isDone()) {
            fgTasks.add(taskFuture);
            taskFuture.whenComplete((v, e) -> fgTasks.remove(taskFuture));
        }
        return taskFuture;
    }

    protected final <T> CompletableFuture<T> trackTask(CompletableFuture<T> task, boolean background) {
        if (background) {
            return addBgTask(task);
        }
        return addFgTask(task);
    }

    protected final <T> CompletableFuture<T> addBgTask(CompletableFuture<T> task) {
        return bgTasks.track(sessionCtx.trackBgTask(task));
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        super.handlerAdded(ctx);
        ChannelAttrs.trafficShaper(ctx).setReadLimit(settings.inboundBandwidth);
        ChannelAttrs.trafficShaper(ctx).setWriteLimit(settings.outboundBandwidth);
        ChannelAttrs.trafficShaper(ctx).setMaxWriteSize(settings.outboundBandwidth);
        ChannelAttrs.setMaxPayload(settings.maxPacketSize, ctx);
        sessionCtx.localSessionRegistry.add(channelId(), this);
        sessionRegistration = ChannelAttrs.mqttSessionContext(ctx).sessionDictClient
            .reg(clientInfo, (killer, redirection) -> {
                if (redirection.getType() != ServerRedirection.Type.NO_MOVE) {
                    ctx.executor().execute(() -> handleProtocolResponse(
                        helper().onRedirect(redirection.getType() == ServerRedirection.Type.PERMANENT_MOVE,
                            redirection.getServerReference())));
                } else {
                    ctx.executor().execute(() -> handleProtocolResponse(helper().onKick(killer)));
                }
            });
        lastActiveAtNanos = sessionCtx.nanoTime();
        if (idleTimeoutNanos > 0) {
            idleTimeoutTask = ctx.executor()
                .scheduleAtFixedRate(this::checkIdle, idleTimeoutNanos, idleTimeoutNanos, TimeUnit.NANOSECONDS);
        }
        scheduleRedirectCheck();
        onInitialized.whenComplete((v, e) -> tenantMeter.recordCount(MqttConnectCount));
        memUsage.addAndGet(estMemSize());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (idleTimeoutTask != null) {
            idleTimeoutTask.cancel(true);
        }
        if (redirectTask != null) {
            redirectTask.cancel(true);
        }
        if (resendTask != null) {
            resendTask.cancel(true);
        }
        if (noDelayLWT != null) {
            addBgTask(pubWillMessage(noDelayLWT));
        }
        Sets.newHashSet(fgTasks).forEach(t -> t.cancel(true));
        sessionCtx.localSessionRegistry.remove(channelId(), this);
        sessionRegistration.stop();
        tenantMeter.recordCount(MqttDisconnectCount);
        memUsage.addAndGet(-estMemSize());
        if (!isGoAway) {
            isGoAway = true;
            eventCollector.report(getLocal(ByClient.class).withoutDisconnect(true).clientInfo(clientInfo));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        super.exceptionCaught(ctx, cause);
        log.debug("ctx: {}, cause:", ctx, cause);
        // if disconnection is caused purely by channel error
        handleProtocolResponse(
            ProtocolResponse.goAwayNow(getLocal(ClientChannelError.class).clientInfo(clientInfo).cause(cause)));
    }

    @Override
    public final void channelRead(ChannelHandlerContext ctx, Object msg) {
        assert msg instanceof MqttMessage;
        MqttMessage mqttMessage = (MqttMessage) msg;
        if (mqttMessage.decoderResult().isSuccess()) {
            tenantMeter.recordSummary(MqttIngressBytes, sizer.sizeByHeader(mqttMessage.fixedHeader()));
            lastActiveAtNanos = sessionCtx.nanoTime();
            log.trace("Received mqtt message:{}", mqttMessage);
            switch (mqttMessage.fixedHeader().messageType()) {
                case CONNECT ->
                    handleProtocolResponse(helper().respondDuplicateConnect((MqttConnectMessage) mqttMessage));
                case DISCONNECT -> handleProtocolResponse(handleDisconnect(mqttMessage));
                case PINGREQ -> {
                    writeAndFlush(MqttMessage.PINGRESP);
                    if (settings.debugMode) {
                        eventCollector.report(getLocal(PingReq.class).pong(true).clientInfo(clientInfo));
                    }
                }
                case PUBLISH -> handlePubMsg((MqttPublishMessage) mqttMessage);
                case PUBREL -> handlePubRelMsg(mqttMessage);
                case PUBACK -> handlePubAckMsg((MqttPubAckMessage) mqttMessage);
                case PUBREC -> handlePubRecMsg(mqttMessage);
                case PUBCOMP -> handlePubCompMsg(mqttMessage);
                case SUBSCRIBE -> handleSubMsg((MqttSubscribeMessage) mqttMessage);
                case UNSUBSCRIBE -> handleUnsubMsg((MqttUnsubscribeMessage) mqttMessage);
                default -> handleOther(mqttMessage);
            }
        } else {
            log.debug("Received bad mqtt message: {}", mqttMessage);
            handleProtocolResponse(helper().respondDecodeError(mqttMessage));
        }
    }

    protected void handleOther(MqttMessage message) {

    }

    protected abstract ProtocolResponse handleDisconnect(MqttMessage message);

    private void handlePubMsg(MqttPublishMessage mqttMessage) {
        if (isExceedReceivingMaximum()) {
            handleProtocolResponse(helper().respondReceivingMaximumExceeded(mqttMessage));
            mqttMessage.release();
            return;
        }
        if (!throttler.pass()) {
            handleProtocolResponse(helper().respondPubRateExceeded(mqttMessage));
            mqttMessage.release();
            return;
        }
        ProtocolResponse isInvalid = helper().validatePubMessage(mqttMessage);
        if (isInvalid != null) {
            handleProtocolResponse(isInvalid);
            mqttMessage.release();
            return;
        }
        int packetId = mqttMessage.variableHeader().packetId();
        long reqId = packetId > 0 ? packetId : sessionCtx.nanoTime();
        String topic = helper().getTopic(mqttMessage);
        int ingressMsgBytes = mqttMessage.fixedHeader().remainingLength() + 1;
        CompletableFuture<Void> pubFuture = (switch (mqttMessage.fixedHeader().qosLevel()) {
            case AT_MOST_ONCE -> handleQoS0Pub(reqId, topic, mqttMessage, ingressMsgBytes);
            case AT_LEAST_ONCE -> handleQoS1Pub(reqId, topic, mqttMessage, ingressMsgBytes);
            case EXACTLY_ONCE -> handleQoS2Pub(reqId, topic, mqttMessage, ingressMsgBytes);
            default -> CompletableFuture.completedFuture(null);
        });
        pubFuture.whenComplete((v, e) -> mqttMessage.release());
    }

    private void handleSubMsg(MqttSubscribeMessage message) {
        ProtocolResponse isInvalid = helper().validateSubMessage(message);
        if (isInvalid != null) {
            handleProtocolResponse(isInvalid);
            return;
        }
        int packetId = message.variableHeader().messageId();
        if (helper().checkPacketIdUsage() && inUsePacketIds.contains(packetId)) {
            writeAndFlush(helper().respondPacketIdInUse(message));
            return;
        }
        inUsePacketIds.add(packetId);
        doSubscribe(packetId, message)
            .thenAcceptAsync(response -> {
                handleProtocolResponse(response);
                if (response.action() == ProtocolResponse.Action.Response) {
                    inUsePacketIds.remove(packetId);
                    eventCollector.report(getLocal(SubAcked.class)
                        .messageId(packetId)
                        .granted(((MqttSubAckMessage) (response.message())).payload().grantedQoSLevels())
                        .topicFilter(message.payload().topicSubscriptions().stream()
                            .map(MqttTopicSubscription::topicFilter)
                            .collect(Collectors.toList()))
                        .clientInfo(clientInfo));
                }
            }, ctx.executor());
    }

    private CompletableFuture<ProtocolResponse> doSubscribe(long reqId, MqttSubscribeMessage message) {
        List<CompletableFuture<IMQTTProtocolHelper.SubResult>> resultFutures = helper().getSubTask(message).stream()
            .map(subTask -> checkAndSubscribe(reqId, subTask.topicFilter(), subTask.option(), subTask.userProperties()))
            .toList();
        return CompletableFuture.allOf(resultFutures.toArray(CompletableFuture[]::new))
            .thenApplyAsync(v -> {
                List<IMQTTProtocolHelper.SubResult> subResults =
                    resultFutures.stream().map(CompletableFuture::join).toList();
                if (subResults.stream().anyMatch(r -> r == IMQTTProtocolHelper.SubResult.BACK_PRESSURE_REJECTED)) {
                    return helper().onSubBackPressured(message);
                }
                return helper().buildSubAckMessage(message, subResults);
            }, ctx.executor());
    }

    protected final CompletableFuture<IMQTTProtocolHelper.SubResult> checkAndSubscribe(long reqId,
                                                                                       String topicFilter,
                                                                                       TopicFilterOption option,
                                                                                       UserProperties userProps) {
        if (!UTF8Util.isWellFormed(topicFilter, SANITY_CHECK)) {
            eventCollector.report(getLocal(MalformedTopicFilter.class)
                .topicFilter(topicFilter)
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.TOPIC_FILTER_INVALID);
        }
        if (!isValidTopicFilter(topicFilter,
            settings.maxTopicLevelLength,
            settings.maxTopicLevels,
            settings.maxTopicLength)) {
            eventCollector.report(getLocal(InvalidTopicFilter.class)
                .topicFilter(topicFilter)
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.TOPIC_FILTER_INVALID);
        }
        if (isWildcardTopicFilter(topicFilter) && !settings.wildcardSubscriptionEnabled) {
            return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.WILDCARD_NOT_SUPPORTED);
        }

        if (isSharedSubscription(topicFilter) && !settings.subscriptionIdentifierEnabled) {
            return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.SHARED_SUBSCRIPTION_NOT_SUPPORTED);
        }

        return addFgTask(
            authProvider.checkPermission(clientInfo, buildSubAction(topicFilter, option.getQos(), userProps))
                .thenCompose(checkResult -> {
                    assert ctx.executor().inEventLoop();
                    if (checkResult.hasGranted()) {
                        if (isSharedSubscription(topicFilter)
                            && !resourceThrottler.hasResource(clientInfo.getTenantId(), TotalSharedSubscriptions)) {
                            eventCollector.report(getLocal(OutOfTenantResource.class)
                                .reason(TotalSharedSubscriptions.name())
                                .clientInfo(clientInfo));
                            return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.EXCEED_LIMIT);
                        }
                        Timer.Sample start = Timer.start();
                        return addFgTask(subTopicFilter(reqId, topicFilter, option))
                            .thenComposeAsync(subResult -> {
                                switch (subResult) {
                                    case OK, EXISTS -> {
                                        start.stop(tenantMeter.timer(MqttTransientSubLatency));
                                        if (!isSharedSubscription(topicFilter) && settings.retainEnabled
                                            && (option.getRetainHandling() == SEND_AT_SUBSCRIBE
                                            || (subResult == IMQTTProtocolHelper.SubResult.OK
                                            &&
                                            option.getRetainHandling() == SEND_AT_SUBSCRIBE_IF_NOT_YET_EXISTS))) {
                                            if (!resourceThrottler.hasResource(clientInfo.getTenantId(),
                                                TotalRetainMatchPerSeconds)) {
                                                eventCollector.report(getLocal(OutOfTenantResource.class)
                                                    .reason(TotalRetainMatchPerSeconds.name())
                                                    .clientInfo(clientInfo));
                                                return CompletableFuture.completedFuture(EXCEED_LIMIT);
                                            }
                                            if (!resourceThrottler.hasResource(clientInfo.getTenantId(),
                                                TotalRetainMatchBytesPerSecond)) {
                                                eventCollector.report(getLocal(OutOfTenantResource.class)
                                                    .reason(TotalRetainMatchBytesPerSecond.name())
                                                    .clientInfo(clientInfo));
                                                return CompletableFuture.completedFuture(EXCEED_LIMIT);
                                            }
                                            return addFgTask(matchRetainedMessage(reqId, topicFilter, option))
                                                .thenApply(matchReply -> {
                                                    switch (matchReply.getResult()) {
                                                        case OK -> {
                                                            return IMQTTProtocolHelper.SubResult.OK;
                                                        }
                                                        case TRY_LATER -> {
                                                            eventCollector.report(getLocal(MatchRetainError.class)
                                                                .reason(matchReply.getResult().name())
                                                                .clientInfo(clientInfo));
                                                            return IMQTTProtocolHelper.SubResult.TRY_LATER;
                                                        }
                                                        case BACK_PRESSURE_REJECTED -> {
                                                            eventCollector.report(getLocal(MatchRetainError.class)
                                                                .reason(matchReply.getResult().name())
                                                                .clientInfo(clientInfo));
                                                            return IMQTTProtocolHelper.SubResult.BACK_PRESSURE_REJECTED;
                                                        }
                                                        default -> {
                                                            eventCollector.report(getLocal(MatchRetainError.class)
                                                                .reason(matchReply.getResult().name())
                                                                .clientInfo(clientInfo));
                                                            return IMQTTProtocolHelper.SubResult.ERROR;
                                                        }
                                                    }
                                                });
                                        }
                                        return CompletableFuture.completedFuture(subResult);
                                    }
                                    case EXCEED_LIMIT -> {
                                        return CompletableFuture.completedFuture(
                                            IMQTTProtocolHelper.SubResult.EXCEED_LIMIT);
                                    }
                                    case BACK_PRESSURE_REJECTED -> {
                                        return CompletableFuture.completedFuture(
                                            IMQTTProtocolHelper.SubResult.BACK_PRESSURE_REJECTED);
                                    }
                                    case TRY_LATER -> {
                                        return CompletableFuture.completedFuture(
                                            IMQTTProtocolHelper.SubResult.TRY_LATER);
                                    }
                                    default -> {
                                        return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.ERROR);
                                    }
                                }
                            }, ctx.executor());
                    } else {
                        eventCollector.report(getLocal(SubActionDisallow.class)
                            .topicFilter(topicFilter)
                            .qos(option.getQos())
                            .clientInfo(clientInfo));
                        return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.NOT_AUTHORIZED);
                    }
                }));
    }

    protected abstract CompletableFuture<IMQTTProtocolHelper.SubResult> subTopicFilter(long reqId,
                                                                                       String topicFilter,
                                                                                       TopicFilterOption option);

    protected abstract CompletableFuture<MatchReply> matchRetainedMessage(long reqId,
                                                                          String topicFilter,
                                                                          TopicFilterOption option);

    private void handleUnsubMsg(MqttUnsubscribeMessage message) {
        ProtocolResponse goAwayOnInvalid = helper().validateUnsubMessage(message);
        if (goAwayOnInvalid != null) {
            handleProtocolResponse(goAwayOnInvalid);
            return;
        }
        int packetId = message.variableHeader().messageId();
        if (helper().checkPacketIdUsage() && inUsePacketIds.contains(packetId)) {
            writeAndFlush(helper().respondPacketIdInUse(message));
            return;
        }
        inUsePacketIds.add(packetId);
        doUnsubscribe(packetId, message)
            .thenAcceptAsync(response -> {
                inUsePacketIds.remove(packetId);
                handleProtocolResponse(response);
                if (response.action() == ProtocolResponse.Action.Response) {
                    eventCollector.report(getLocal(UnsubAcked.class)
                        .messageId(packetId)
                        .topicFilter(message.payload().topics())
                        .clientInfo(clientInfo));
                }
            }, ctx.executor());
    }

    private CompletableFuture<ProtocolResponse> doUnsubscribe(long reqId, MqttUnsubscribeMessage message) {
        UserProperties userProps = helper().getUserProps(message);
        List<CompletableFuture<IMQTTProtocolHelper.UnsubResult>> resultFutures = message.payload().topics().stream()
            .map(topicFilter -> checkAndUnsubscribe(reqId, topicFilter, userProps))
            .toList();
        return CompletableFuture.allOf(resultFutures.toArray(CompletableFuture[]::new))
            .thenApply(v -> resultFutures.stream().map(CompletableFuture::join).toList())
            .thenApply(subResults -> {
                if (subResults.stream().anyMatch(r -> r == IMQTTProtocolHelper.UnsubResult.BACK_PRESSURE_REJECTED)) {
                    return helper().onUnsubBackPressured(message);
                }
                return helper().buildUnsubAckMessage(message, subResults);
            });
    }

    protected final CompletableFuture<IMQTTProtocolHelper.UnsubResult> checkAndUnsubscribe(long reqId,
                                                                                           String topicFilter,
                                                                                           UserProperties userProps) {
        if (!isValidTopicFilter(topicFilter,
            settings.maxTopicLevelLength,
            settings.maxTopicLevels,
            settings.maxTopicLength)) {
            eventCollector.report(getLocal(InvalidTopicFilter.class)
                .topicFilter(topicFilter)
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(IMQTTProtocolHelper.UnsubResult.TOPIC_FILTER_INVALID);
        }
        return addFgTask(authProvider.checkPermission(clientInfo, buildUnsubAction(topicFilter, userProps)))
            .thenCompose(checkResult -> {
                assert ctx.executor().inEventLoop();
                if (checkResult.hasGranted()) {
                    return addFgTask(unsubTopicFilter(reqId, topicFilter));
                } else {
                    eventCollector.report(getLocal(UnsubActionDisallow.class)
                        .clientInfo(clientInfo)
                        .topicFilter(topicFilter));
                    // always reply unsub ack
                    return CompletableFuture.completedFuture(IMQTTProtocolHelper.UnsubResult.NOT_AUTHORIZED);
                }
            });
    }

    protected abstract CompletableFuture<IMQTTProtocolHelper.UnsubResult> unsubTopicFilter(long reqId,
                                                                                           String topicFilter);

    private void handlePubRelMsg(MqttMessage mqttMessage) {
        int packetId = ((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId();
        if (!inUsePacketIds.contains(packetId)) {
            writeAndFlush(helper().onPubRelReceived(mqttMessage, false));
            return;
        }
        decReceivingCount();
        inUsePacketIds.remove(packetId);
        writeAndFlush(helper().onPubRelReceived(mqttMessage, true));
    }

    private void handlePubAckMsg(MqttPubAckMessage mqttMessage) {
        int packetId = mqttMessage.variableHeader().messageId();
        if (isConfirming(packetId)) {
            SubMessage confirmed = confirm(packetId, true);
            tenantMeter.recordSummary(MqttQoS1DeliverBytes, confirmed.message().getPayload().size());
        } else {
            log.trace("No packetId to confirm released: sessionId={}, packetId={}",
                userSessionId(clientInfo), packetId);
        }
    }

    private void handlePubRecMsg(MqttMessage message) {
        int packetId = ((MqttMessageIdVariableHeader) message.variableHeader()).messageId();
        if (isConfirming(packetId)) {
            if (helper().isQoS2Received(message)) {
                handleProtocolResponse(helper().respondPubRecMsg(message, false));
                if (settings.debugMode) {
                    SubMessage received = getConfirming(packetId);
                    eventCollector.report(getLocal(QoS2Received.class)
                        .reqId(packetId)
                        .messageId(packetId)
                        .isRetain(received.isRetain())
                        .sender(received.publisher())
                        .topic(received.topic())
                        .matchedFilter(received.topicFilter())
                        .size(received.message().getPayload().size())
                        .clientInfo(clientInfo));
                }
            } else {
                confirm(packetId, true);
            }
        } else {
            handleProtocolResponse(helper().respondPubRecMsg(message, true));
        }
    }

    private void handlePubCompMsg(MqttMessage message) {
        MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
        int packetId = variableHeader.messageId();
        if (isConfirming(packetId)) {
            SubMessage confirmed = confirm(packetId, true);
            if (settings.debugMode) {
                eventCollector.report(getLocal(QoS2Confirmed.class)
                    .reqId(confirmed.message().getMessageId())
                    .messageId(packetId)
                    .isRetain(confirmed.isRetain())
                    .sender(confirmed.publisher())
                    .delivered(true)
                    .topic(confirmed.topic())
                    .matchedFilter(confirmed.topicFilter())
                    .size(confirmed.message().getPayload().size())
                    .clientInfo(clientInfo));
            }
            tenantMeter.recordSummary(MqttQoS2DeliverBytes, confirmed.message.getPayload().size());
        } else {
            log.trace("No packetId to confirm released: sessionId={}, packetId={}",
                userSessionId(clientInfo), packetId);
        }
    }

    protected int clientReceiveMaximum() {
        return helper().clientReceiveMaximum();
    }

    protected final boolean isConfirming(int packetId) {
        return unconfirmedPacketIds.containsKey(packetId);
    }

    private SubMessage getConfirming(int packetId) {
        return unconfirmedPacketIds.get(packetId).message;
    }

    protected final int clientReceiveQuota() {
        return clientReceiveMaximum() - unconfirmedPacketIds.size();
    }

    private SubMessage confirm(int packetId, boolean delivered) {
        ConfirmingMessage confirmingMsg = unconfirmedPacketIds.get(packetId);
        SubMessage msg = null;
        if (confirmingMsg != null) {
            msg = confirmingMsg.message;
            confirm(confirmingMsg, delivered);
        } else {
            log.trace("No msg to confirm: sessionId={}, packetId={}", userSessionId, packetId);
        }
        return msg;
    }

    private void confirm(ConfirmingMessage confirmingMsg, boolean delivered) {
        long now = sessionCtx.nanoTime();
        confirmingMsg.setAcked();
        resendQueue.remove(confirmingMsg);
        Iterator<Integer> packetIdItr = unconfirmedPacketIds.keySet().iterator();
        while (packetIdItr.hasNext()) {
            int packetId = packetIdItr.next();
            confirmingMsg = unconfirmedPacketIds.get(packetId);
            if (confirmingMsg.acked) {
                packetIdItr.remove();
                SubMessage confirmed = confirmingMsg.message;
                onConfirm(confirmingMsg.seq);
                switch (confirmed.qos()) {
                    case AT_LEAST_ONCE -> {
                        tenantMeter.timer(MqttQoS1ExternalLatency)
                            .record(now - confirmingMsg.timestamp, TimeUnit.NANOSECONDS);
                        if (settings.debugMode) {
                            eventCollector.report(getLocal(QoS1Confirmed.class)
                                .reqId(confirmed.message().getMessageId())
                                .messageId(packetId)
                                .isRetain(confirmed.isRetain())
                                .sender(confirmed.publisher())
                                .delivered(delivered)
                                .topic(confirmed.topic())
                                .matchedFilter(confirmed.topicFilter())
                                .size(confirmed.message().getPayload().size())
                                .clientInfo(clientInfo));
                        }
                    }
                    case EXACTLY_ONCE -> {
                        tenantMeter.timer(MqttQoS2ExternalLatency)
                            .record(now - confirmingMsg.timestamp, TimeUnit.NANOSECONDS);
                        if (!delivered && settings.debugMode) {
                            eventCollector.report(getLocal(QoS2Confirmed.class)
                                .reqId(confirmed.message().getMessageId())
                                .messageId(packetId)
                                .isRetain(confirmed.isRetain())
                                .sender(confirmed.publisher())
                                .delivered(false)
                                .topic(confirmed.topic())
                                .matchedFilter(confirmed.topicFilter())
                                .size(confirmed.message().getPayload().size())
                                .clientInfo(clientInfo));
                        }
                    }
                    default -> {
                        // do nothing
                    }
                }
            } else {
                // the seq should be confirmed one by one, stop at first unconfirmed msg
                break;
            }
        }
        if (resendTask != null && !resendTask.isDone()) {
            resendTask.cancel(true);
        }
        if (!resendQueue.isEmpty()) {
            scheduleResend();
        }
    }

    protected abstract void onConfirm(long seq);

    protected final void sendQoS0SubMessage(SubMessage msg) {
        assert msg.qos() == AT_MOST_ONCE;
        ClientInfo publisher = msg.publisher();
        String topicFilter = msg.topicFilter();
        MqttPublishMessage pubMsg = helper().buildMqttPubMessage(0, msg, false);
        int msgSize = sizer.sizeOf(pubMsg).encodedBytes();
        assert ctx.executor().inEventLoop();
        if (!msg.permissionGranted()) {
            eventCollector.report(getLocal(QoS0Dropped.class)
                .reason(DropReason.NoSubPermission)
                .isRetain(msg.isRetain())
                .sender(publisher)
                .topic(msg.topic)
                .matchedFilter(topicFilter)
                .size(msgSize)
                .clientInfo(clientInfo()));
            // unsubscribe the topic filter when no permission
            addBgTask(unsubTopicFilter(System.nanoTime(), topicFilter));
            return;
        }
        if (msg.isDup) {
            eventCollector.report(getLocal(QoS0Dropped.class)
                .reason(DropReason.Duplicated)
                .isRetain(msg.isRetain())
                .sender(publisher)
                .topic(msg.topic)
                .matchedFilter(topicFilter)
                .size(msgSize)
                .clientInfo(clientInfo()));
            return;
        }
        TopicFilterOption option = msg.option();
        if (option.getNoLocal() && clientInfo.equals(publisher)) {
            // skip local sub
            if (settings.debugMode) {
                eventCollector.report(getLocal(QoS0Dropped.class)
                    .reason(DropReason.NoLocal)
                    .isRetain(msg.isRetain())
                    .sender(publisher)
                    .topic(msg.topic)
                    .matchedFilter(topicFilter)
                    .size(msgSize)
                    .clientInfo(clientInfo()));
            }
            return;
        }
        if (messageExpiryInterval(pubMsg.variableHeader().properties()).orElse(Integer.MAX_VALUE) < 0) {
            // If the Message Expiry Interval has passed and the Server has not managed to start onward delivery
            // to a matching subscriber, then it MUST delete the copy of the message for that subscriber [MQTT-3.3.2-5]
            if (settings.debugMode) {
                eventCollector.report(getLocal(QoS0Dropped.class)
                    .reason(DropReason.Expired)
                    .isRetain(msg.isRetain())
                    .sender(publisher)
                    .topic(msg.topic)
                    .matchedFilter(topicFilter)
                    .size(msgSize)
                    .clientInfo(clientInfo()));
            }
            return;
        }
        if (oomCondition.meet()) {
            eventCollector.report(getLocal(QoS0Dropped.class)
                .reason(DropReason.ResourceExhausted)
                .isRetain(msg.isRetain())
                .sender(publisher)
                .topic(msg.topic)
                .matchedFilter(topicFilter)
                .size(msgSize)
                .clientInfo(clientInfo()));
            return;
        }
        if (!ctx.channel().isActive()) {
            eventCollector.report(getLocal(QoS0Dropped.class)
                .reason(DropReason.ChannelClosed)
                .isRetain(msg.isRetain())
                .sender(publisher)
                .topic(msg.topic)
                .matchedFilter(topicFilter)
                .size(msgSize)
                .clientInfo(clientInfo()));
            return;
        }
        if (!ctx.channel().isWritable()) {
            eventCollector.report(getLocal(QoS0Dropped.class)
                .reason(DropReason.Overflow)
                .isRetain(msg.isRetain())
                .sender(publisher)
                .topic(msg.topic)
                .matchedFilter(topicFilter)
                .size(msgSize)
                .clientInfo(clientInfo()));
            return;
        }
        memUsage.addAndGet(msgSize);
        writeAndFlush(pubMsg).addListener(f -> {
            memUsage.addAndGet(-msgSize);
            if (f.isSuccess()) {
                lastActiveAtNanos = sessionCtx.nanoTime();
                if (settings.debugMode) {
                    eventCollector.report(getLocal(QoS0Pushed.class)
                        .isRetain(msg.isRetain())
                        .sender(publisher)
                        .matchedFilter(topicFilter)
                        .topic(msg.topic)
                        .size(msgSize)
                        .clientInfo(clientInfo));
                }
            } else {
                // TODO: add cause to event
                eventCollector.report(getLocal(QoS0Dropped.class)
                    .reason(DropReason.InternalError)
                    .isRetain(msg.isRetain())
                    .sender(publisher)
                    .topic(msg.topic)
                    .matchedFilter(topicFilter)
                    .size(msgSize)
                    .clientInfo(clientInfo()));
            }
        });
    }

    protected final void sendConfirmableSubMessage(long seq, SubMessage msg) {
        assert seq > -1;
        ConfirmingMessage confirmingMessage = new ConfirmingMessage(seq, msg, sessionCtx.nanoTime());
        // make sure acktimeout moments don't conflict
        while (resendQueue.contains(confirmingMessage)) {
            confirmingMessage = new ConfirmingMessage(seq, msg, sessionCtx.nanoTime());
        }
        ConfirmingMessage prev = unconfirmedPacketIds.putIfAbsent(confirmingMessage.packetId(), confirmingMessage);
        if (prev == null) {
            resendQueue.add(confirmingMessage);
            if (resendTask == null || resendTask.isDone()) {
                scheduleResend();
            }
            writeConfirmableSubMessage(seq, msg, msg.topicFilter(), msg.publisher(), false);
        } else {
            log.warn("Bad state: sequence duplicate seq={}", seq);
        }
    }

    private void writeConfirmableSubMessage(long seq,
                                            SubMessage msg,
                                            String topicFilter,
                                            ClientInfo publisher,
                                            boolean isDup) {
        int packetId = packetId(seq);
        MqttPublishMessage pubMsg = helper().buildMqttPubMessage(packetId, msg, isDup);
        TopicFilterOption option = msg.option();
        int msgSize = sizer.sizeOf(pubMsg).encodedBytes();
        if (!msg.permissionGranted()) {
            reportDropConfirmableMsgEvent(msg, DropReason.NoSubPermission);
            ctx.executor().execute(() -> confirm(packetId, false));
            addBgTask(this.unsubTopicFilter(System.nanoTime(), topicFilter));
            return;
        }
        if (msg.isDup) {
            reportDropConfirmableMsgEvent(msg, DropReason.Duplicated);
            ctx.executor().execute(() -> confirm(packetId, false));
            return;
        }
        if (option.getNoLocal() && clientInfo.equals(publisher)) {
            // skip local sub
            if (settings.debugMode) {
                switch (msg.qos()) {
                    case AT_LEAST_ONCE -> eventCollector.report(getLocal(QoS1Dropped.class)
                        .reason(DropReason.NoLocal)
                        .reqId(pubMsg.variableHeader().packetId())
                        .isRetain(pubMsg.fixedHeader().isRetain())
                        .sender(publisher)
                        .topic(pubMsg.variableHeader().topicName())
                        .matchedFilter(topicFilter)
                        .size(msgSize)
                        .clientInfo(clientInfo));
                    case EXACTLY_ONCE -> eventCollector.report(getLocal(QoS2Dropped.class)
                        .reason(DropReason.NoLocal)
                        .reqId(pubMsg.variableHeader().packetId())
                        .isRetain(pubMsg.fixedHeader().isRetain())
                        .sender(publisher)
                        .topic(pubMsg.variableHeader().topicName())
                        .matchedFilter(topicFilter)
                        .size(msgSize)
                        .clientInfo(clientInfo));
                    default -> {
                        // do nothing
                    }
                }
            }
            ctx.executor().execute(() -> confirm(packetId, false));
            return;
        }
        if (messageExpiryInterval(pubMsg.variableHeader().properties()).orElse(Integer.MAX_VALUE) < 0) {
            //  If the Message Expiry Interval has passed and the Server has not managed to start onward delivery
            //  to a matching subscriber, then it MUST delete the copy of the message for that subscriber [MQTT-3.3.2-5]
            if (settings.debugMode) {
                reportDropConfirmableMsgEvent(msg, DropReason.Expired);
            }
            ctx.executor().execute(() -> confirm(packetId, false));
            return;
        }
        if (oomCondition.meet()) {
            reportDropConfirmableMsgEvent(msg, DropReason.ResourceExhausted);
            ctx.executor().execute(() -> confirm(packetId, false));
            return;
        }
        if (!ctx.channel().isActive()) {
            reportDropConfirmableMsgEvent(msg, DropReason.ChannelClosed);
            return;
        }
        if (!ctx.channel().isWritable()) {
            reportDropConfirmableMsgEvent(msg, DropReason.Overflow);
            ctx.executor().execute(() -> confirm(packetId, false));
            return;
        }
        memUsage.addAndGet(msgSize);
        writeAndFlush(pubMsg).addListener(f -> {
            memUsage.addAndGet(-msgSize);
            if (f.isSuccess()) {
                if (settings.debugMode) {
                    switch (pubMsg.fixedHeader().qosLevel()) {
                        case AT_LEAST_ONCE -> eventCollector.report(getLocal(QoS1Pushed.class)
                            .reqId(pubMsg.variableHeader().packetId())
                            .messageId(pubMsg.variableHeader().packetId())
                            .dup(false)
                            .isRetain(pubMsg.fixedHeader().isRetain())
                            .sender(publisher)
                            .matchedFilter(topicFilter)
                            .topic(pubMsg.variableHeader().topicName())
                            .size(msgSize)
                            .clientInfo(clientInfo));
                        case EXACTLY_ONCE -> eventCollector.report(getLocal(QoS2Pushed.class)
                            .reqId(pubMsg.variableHeader().packetId())
                            .messageId(pubMsg.variableHeader().packetId())
                            .dup(false)
                            .isRetain(pubMsg.fixedHeader().isRetain())
                            .sender(publisher)
                            .matchedFilter(topicFilter)
                            .topic(pubMsg.variableHeader().topicName())
                            .size(msgSize)
                            .clientInfo(clientInfo));
                        default -> {
                            // do nothing
                        }
                    }
                }
            } else {
                reportDropConfirmableMsgEvent(msg, DropReason.InternalError);
            }
        });
    }

    private void reportDropConfirmableMsgEvent(SubMessage subMsg, DropReason reason) {
        switch (subMsg.qos()) {
            case AT_LEAST_ONCE -> eventCollector.report(getLocal(QoS1Dropped.class)
                .reason(reason)
                .reqId(subMsg.message().getMessageId())
                .isRetain(subMsg.isRetain())
                .sender(subMsg.publisher())
                .topic(subMsg.topic())
                .matchedFilter(subMsg.topicFilter())
                .size(subMsg.message().getPayload().size())
                .clientInfo(clientInfo()));
            case EXACTLY_ONCE -> eventCollector.report(getLocal(QoS2Dropped.class)
                .reason(reason)
                .reqId(subMsg.message().getMessageId())
                .isRetain(subMsg.isRetain())
                .sender(subMsg.publisher())
                .topic(subMsg.topic())
                .matchedFilter(subMsg.topicFilter())
                .size(subMsg.message().getPayload().size())
                .clientInfo(clientInfo()));
            default -> {
                // do nothing
            }
        }
    }

    private long ackTimeoutNanos(ConfirmingMessage msg) {
        return msg.timestamp + Duration.ofSeconds(settings.resendTimeoutSeconds).multipliedBy(msg.sentCount).toNanos();
    }

    private void scheduleResend() {
        resendTask =
            ctx.executor().schedule(this::resend, ackTimeoutNanos(resendQueue.first()) - sessionCtx.nanoTime(),
                TimeUnit.NANOSECONDS);
    }

    private void resend() {
        long now = sessionCtx.nanoTime() + Duration.ofMillis(100).toNanos();
        while (!resendQueue.isEmpty() && ctx.channel().isActive()) {
            ConfirmingMessage confirmingMessage = resendQueue.first();
            if (ackTimeoutNanos(confirmingMessage) > now) {
                scheduleResend();
                break;
            }
            if (confirmingMessage.sentCount < settings.maxResendTimes + 1) {
                // reorder and write out again with dup flag set
                resendQueue.remove(confirmingMessage);
                confirmingMessage.sentCount++;
                resendQueue.add(confirmingMessage);
                writeConfirmableSubMessage(confirmingMessage.seq, confirmingMessage.message,
                    confirmingMessage.message.topicFilter(), confirmingMessage.message.publisher(), true);
            } else {
                confirm(confirmingMessage, false);
            }
        }
    }

    private boolean isExceedReceivingMaximum() {
        return receivingCount >= settings.receiveMaximum;
    }

    private void incReceivingCount() {
        receivingCount++;
    }

    private void decReceivingCount() {
        receivingCount = Math.max(receivingCount - 1, 0);
    }

    private CompletableFuture<CheckResult> checkPubPermission(String topic, Message distMessage,
                                                              UserProperties userProps) {
        return authProvider.checkPermission(clientInfo(),
            buildPubAction(topic, distMessage.getPubQoS(), distMessage.getIsRetain(), userProps));
    }

    private CompletableFuture<Void> handleQoS0Pub(long reqId,
                                                  String topic,
                                                  MqttPublishMessage message,
                                                  int ingressMsgBytes) {
        assert ctx.executor().inEventLoop();
        if (log.isTraceEnabled()) {
            log.trace("Checking authorization of pub qos0 action: reqId={}, sessionId={}, topic={}", reqId,
                userSessionId(clientInfo), topic);
        }
        Message distMessage = helper().buildDistMessage(message);
        UserProperties userProps = helper().getUserProps(message);
        return addFgTask(checkPubPermission(topic, distMessage, userProps))
            .thenCompose(checkResult -> {
                assert ctx.executor().inEventLoop();
                if (log.isTraceEnabled()) {
                    log.trace("Checked authorization of pub qos0 action: reqId={}, sessionId={}, topic={}:{}",
                        reqId, userSessionId(clientInfo), topic, checkResult.getTypeCase());
                }
                if (checkResult.getTypeCase() == CheckResult.TypeCase.GRANTED) {
                    tenantMeter.recordSummary(MqttQoS0IngressBytes, ingressMsgBytes);
                    return doPub(reqId, topic, distMessage, false, ingressMsgBytes)
                        .thenAccept(pubResult -> {
                            assert ctx.executor().inEventLoop();
                            if (log.isTraceEnabled()) {
                                log.trace("Disted qos0 msg: reqId={}, sessionId={}, topic={}",
                                    reqId, userSessionId(clientInfo), topic);
                            }
                            handleProtocolResponse(helper().onQoS0PubHandled(pubResult, message,
                                checkResult.getGranted().getUserProps()));
                        });
                }
                if (log.isTraceEnabled()) {
                    log.trace("Unauthorized qos0 topic: reqId={}, sessionId={}, topic={}",
                        reqId, userSessionId(clientInfo), topic);
                }
                eventCollector.report(getLocal(PubActionDisallow.class)
                    .isLastWill(false)
                    .topic(topic)
                    .qos(AT_MOST_ONCE)
                    .isRetain(distMessage.getIsRetain())
                    .clientInfo(clientInfo));
                handleProtocolResponse(helper().onQoS0DistDenied(topic, distMessage, checkResult));
                return CompletableFuture.completedFuture(null);
            });
    }

    private CompletableFuture<Void> handleQoS1Pub(long reqId,
                                                  String topic,
                                                  MqttPublishMessage message,
                                                  int ingressMsgBytes) {
        int packetId = message.variableHeader().packetId();
        if (inUsePacketIds.contains(packetId)) {
            handleProtocolResponse(helper().respondQoS1PacketInUse(message));
            return CompletableFuture.completedFuture(null);
        }
        inUsePacketIds.add(packetId);
        incReceivingCount();
        if (log.isTraceEnabled()) {
            log.trace("Checking authorization of pub qos1 action: reqId={}, sessionId={}, topic={}",
                reqId, userSessionId(clientInfo), topic);
        }
        Message distMessage = helper().buildDistMessage(message);
        UserProperties userProps = helper().getUserProps(message);
        return addFgTask(checkPubPermission(topic, distMessage, userProps))
            .thenCompose(checkResult -> {
                assert ctx.executor().inEventLoop();
                if (checkResult.getTypeCase() == CheckResult.TypeCase.GRANTED) {
                    tenantMeter.recordSummary(MqttQoS1IngressBytes, ingressMsgBytes);
                    return doPub(reqId, topic, distMessage, message.fixedHeader().isDup(), ingressMsgBytes)
                        .thenAccept(pubResult -> {
                            assert ctx.executor().inEventLoop();
                            if (log.isTraceEnabled()) {
                                log.trace("Disted qos1 msg: reqId={}, sessionId={}, topic={}",
                                    reqId, userSessionId(clientInfo), topic);
                            }
                            decReceivingCount();
                            inUsePacketIds.remove(packetId);
                            if (ctx.channel().isActive() && ctx.channel().isWritable()) {
                                handleProtocolResponse(helper()
                                    .onQoS1PubHandled(pubResult, message, checkResult.getGranted().getUserProps()));
                            } else {
                                eventCollector.report(getLocal(QoS1PubAckDropped.class)
                                    .reqId(reqId)
                                    .isDup(message.fixedHeader().isDup())
                                    .topic(topic)
                                    .size(message.payload().readableBytes())
                                    .clientInfo(clientInfo));
                            }
                        });
                }
                decReceivingCount();
                inUsePacketIds.remove(packetId);
                if (log.isTraceEnabled()) {
                    log.trace("Unauthorized qos1 topic: reqId={}, sessionId={}, topic={}",
                        reqId, userSessionId(clientInfo), topic);
                }
                eventCollector.report(getLocal(PubActionDisallow.class)
                    .isLastWill(false)
                    .topic(topic)
                    .qos(AT_LEAST_ONCE)
                    .isRetain(distMessage.getIsRetain())
                    .clientInfo(clientInfo));
                handleProtocolResponse(
                    helper().onQoS1DistDenied(topic, packetId, distMessage, checkResult));
                return CompletableFuture.completedFuture(null);
            });
    }

    private CompletableFuture<Void> handleQoS2Pub(long reqId,
                                                  String topic,
                                                  MqttPublishMessage message,
                                                  int ingressMsgBytes) {
        assert ctx.executor().inEventLoop();
        int packetId = message.variableHeader().packetId();
        if (inUsePacketIds.contains(packetId)) {
            handleProtocolResponse(helper().respondQoS2PacketInUse(message));
            return CompletableFuture.completedFuture(null);
        }

        incReceivingCount();
        inUsePacketIds.add(packetId);
        Message distMessage = helper().buildDistMessage(message);
        UserProperties userProps = helper().getUserProps(message);
        return addFgTask(checkPubPermission(topic, distMessage, userProps))
            .thenCompose(checkResult -> {
                assert ctx.executor().inEventLoop();
                if (checkResult.getTypeCase() == CheckResult.TypeCase.GRANTED) {
                    tenantMeter.recordSummary(MqttQoS2IngressBytes, ingressMsgBytes);
                    return doPub(reqId, topic, distMessage, message.fixedHeader().isDup(), ingressMsgBytes)
                        .thenAccept(pubResult -> {
                            assert ctx.executor().inEventLoop();
                            if (log.isTraceEnabled()) {
                                log.trace("Published qos2 msg: reqId={}, sessionId={}, topic={}",
                                    reqId, userSessionId(clientInfo), topic);
                            }
                            if (ctx.channel().isActive()) {
                                if (ctx.channel().isWritable()) {
                                    if (pubResult.distResult() == PubResult.BACK_PRESSURE_REJECTED
                                        || pubResult.distResult() == PubResult.TRY_LATER
                                        || pubResult.distResult() == PubResult.ERROR) {
                                        decReceivingCount();
                                        inUsePacketIds.remove(packetId);
                                    }
                                    handleProtocolResponse(helper()
                                        .onQoS2PubHandled(pubResult, message,
                                            checkResult.getGranted().getUserProps()));
                                } else {
                                    decReceivingCount();
                                    inUsePacketIds.remove(packetId);
                                    eventCollector.report(getLocal(QoS2PubRecDropped.class)
                                        .reqId(reqId)
                                        .isDup(message.fixedHeader().isDup())
                                        .topic(topic)
                                        .size(message.payload().readableBytes())
                                        .clientInfo(clientInfo));
                                }
                            }
                        });
                }
                decReceivingCount();
                inUsePacketIds.remove(packetId);
                if (log.isTraceEnabled()) {
                    log.trace("Unauthorized qos2 topic: reqId={}, sessionId={}, topic={}",
                        reqId, userSessionId(clientInfo), topic);
                }
                eventCollector.report(getLocal(PubActionDisallow.class)
                    .isLastWill(false)
                    .topic(topic)
                    .qos(EXACTLY_ONCE)
                    .isRetain(distMessage.getIsRetain())
                    .clientInfo(clientInfo));
                handleProtocolResponse(
                    helper().onQoS2DistDenied(topic, packetId, distMessage, checkResult));
                return CompletableFuture.completedFuture(null);
            });
    }

    private CompletableFuture<Void> pubWillMessage(LWT willMessage) {
        return authProvider.checkPermission(clientInfo(), buildPubAction(willMessage.getTopic(),
                willMessage.getMessage()
                    .getPubQoS(),
                willMessage.getMessage().getIsRetain()))
            .thenCompose(checkResult -> {
                assert ctx.executor().inEventLoop();
                if (checkResult.hasGranted()) {
                    return doPubLastWill(willMessage);
                } else {
                    sessionCtx.eventCollector.report(getLocal(PubActionDisallow.class)
                        .isLastWill(true)
                        .topic(willMessage.getTopic())
                        .qos(willMessage.getMessage().getPubQoS())
                        .isRetain(willMessage.getMessage().getIsRetain())
                        .clientInfo(clientInfo));
                    return CompletableFuture.completedFuture(null);
                }
            });
    }

    private void checkIdle() {
        if (sessionCtx.nanoTime() - lastActiveAtNanos > idleTimeoutNanos) {
            idleTimeoutTask.cancel(true);
            handleProtocolResponse(helper().onIdleTimeout(keepAliveTimeSeconds));
        }
    }

    private void scheduleRedirectCheck() {
        long delay = ThreadLocalRandom.current().nextInt(REDIRECT_CHECK_INTERVAL_SECONDS);
        redirectTask = ctx.executor()
            .scheduleAtFixedRate(this::checkRedirect, delay, REDIRECT_CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private void checkRedirect() {
        Optional<Redirection> redirection = sessionCtx.clientBalancer.needRedirect(clientInfo);
        redirection.ifPresent(value -> {
            if (redirectTask != null) {
                redirectTask.cancel(true);
            }
            handleProtocolResponse(helper().onRedirect(value.permanentMove(), value.serverReference().orElse(null)));
        });
    }

    protected final void discardLWT() {
        noDelayLWT = null;
    }

    protected final void resumeChannelRead() {
        if (isGoAway) {
            return;
        }
        // resume reading
        ctx.channel().config().setAutoRead(true);
        ctx.read();
    }

    protected void handleProtocolResponse(ProtocolResponse response) {
        assert ctx.executor().inEventLoop();
        if (isGoAway) {
            return;
        }
        for (Event<?> reason : response.reasons()) {
            sessionCtx.eventCollector.report(reason);
        }
        switch (response.action()) {
            case NoResponse -> {
                assert response.message() == null;
            }
            case Response -> writeAndFlush(response.message());
            case GoAway, GoAwayNow -> {
                isGoAway = true;
                ctx.channel().config().setAutoRead(false);
                if (response.action() == ProtocolResponse.Action.GoAwayNow) {
                    ctx.close();
                } else {
                    ctx.executor().schedule(() -> ctx.close(),
                        ThreadLocalRandom.current().nextInt(100, 3000), TimeUnit.MILLISECONDS);
                }
            }
            case ResponseAndGoAway, ResponseAndGoAwayNow -> {
                isGoAway = true;
                // disable auto read
                ctx.channel().config().setAutoRead(false);
                Runnable farewell = () -> {
                    if (response.message() != null) {
                        writeAndFlush(response.message()).addListener(ChannelFutureListener.CLOSE);
                    } else {
                        ctx.close();
                    }
                };
                if (response.action() == ProtocolResponse.Action.ResponseAndGoAwayNow) {
                    farewell.run();
                } else {
                    ctx.executor()
                        .schedule(farewell, ThreadLocalRandom.current().nextInt(100, 3000), TimeUnit.MILLISECONDS);
                }
            }
            default -> {
                // do nothing
            }
        }
    }

    protected final boolean isDuplicateMessage(ClientInfo publisher, Message message,
                                               Cache<String, AtomicReference<Long>> latestMsgTsByPublisher) {
        if (message.getIsRetained()) {
            return false;
        }
        String mqttPublisherKey = publisher.getMetadataMap().get(MQTT_CHANNEL_ID_KEY);
        if (mqttPublisherKey == null) {
            // don't deduplicate message published from HTTP API
            return false;
        }
        AtomicReference<Long> lastPubRef = latestMsgTsByPublisher.get(mqttPublisherKey, k -> new AtomicReference<>(0L));
        if (lastPubRef.get() >= message.getTimestamp()) {
            return true;
        }
        lastPubRef.set(message.getTimestamp());
        return false;
    }

    private CompletableFuture<Void> doPubLastWill(LWT willMessage) {
        Message message = willMessage.getMessage().toBuilder()
            .setTimestamp(HLC.INST.get())
            .build();
        long reqId = sessionCtx.nanoTime();
        int size = message.getPayload().size() + willMessage.getTopic().length();
        return doPub(reqId, willMessage.getTopic(), message, true, true)
            .handle((v, e) -> {
                assert ctx.executor().inEventLoop();
                if (e != null) {
                    eventCollector.report(getLocal(WillDistError.class)
                        .clientInfo(clientInfo)
                        .reqId(reqId)
                        .topic(willMessage.getTopic())
                        .qos(willMessage.getMessage().getPubQoS())
                        .size(willMessage.getMessage().getPayload().size()));
                } else {
                    switch (v.distResult()) {
                        case OK, NO_MATCH -> {
                            switch (message.getPubQoS()) {
                                case AT_MOST_ONCE -> tenantMeter.recordSummary(MqttQoS0DistBytes, size);
                                case AT_LEAST_ONCE -> tenantMeter.recordSummary(MqttQoS1DistBytes, size);
                                case EXACTLY_ONCE -> tenantMeter.recordSummary(MqttQoS2DistBytes, size);
                                default -> {
                                    // do nothing
                                }
                            }
                            eventCollector.report(getLocal(WillDisted.class)
                                .clientInfo(clientInfo)
                                .reqId(reqId)
                                .topic(willMessage.getTopic())
                                .qos(willMessage.getMessage().getPubQoS())
                                .size(willMessage.getMessage().getPayload().size()));
                        }
                        default -> eventCollector.report(getLocal(WillDistError.class)
                            .clientInfo(clientInfo)
                            .reqId(reqId)
                            .topic(willMessage.getTopic())
                            .qos(willMessage.getMessage().getPubQoS())
                            .size(willMessage.getMessage().getPayload().size()));
                    }
                }
                return null;
            });
    }

    private CompletableFuture<IMQTTProtocolHelper.PubResult> doPub(long reqId,
                                                                   String topic,
                                                                   Message message,
                                                                   boolean isDup,
                                                                   int ingressMsgSize) {
        return doPub(reqId, topic, message, false, false)
            .thenApply(v -> {
                assert ctx.executor().inEventLoop();
                switch (v.distResult()) {
                    case OK, NO_MATCH -> {
                        if (log.isTraceEnabled()) {
                            log.trace("Msg published: reqId={}, sessionId={}, topic={}, qos={}, size={}",
                                reqId, userSessionId, topic, message.getPubQoS(), message.getPayload().size());
                        }
                        switch (message.getPubQoS()) {
                            case AT_MOST_ONCE -> tenantMeter.recordSummary(MqttQoS0DistBytes, ingressMsgSize);
                            case AT_LEAST_ONCE -> tenantMeter.recordSummary(MqttQoS1DistBytes, ingressMsgSize);
                            case EXACTLY_ONCE -> tenantMeter.recordSummary(MqttQoS2DistBytes, ingressMsgSize);
                            default -> {
                                // do nothing
                            }
                        }
                    }
                    default -> {
                        switch (message.getPubQoS()) {
                            case AT_MOST_ONCE -> eventCollector.report(getLocal(QoS0DistError.class)
                                .reqId(reqId)
                                .topic(topic)
                                .size(ingressMsgSize)
                                .reason(v.distResult().name())
                                .clientInfo(clientInfo));
                            case AT_LEAST_ONCE -> eventCollector.report(getLocal(QoS1DistError.class)
                                .reqId(reqId)
                                .topic(topic)
                                .isDup(isDup)
                                .size(ingressMsgSize)
                                .reason(v.distResult().name())
                                .clientInfo(clientInfo));
                            case EXACTLY_ONCE -> eventCollector.report(getLocal(QoS2DistError.class)
                                .reqId(reqId)
                                .topic(topic)
                                .isDup(isDup)
                                .size(ingressMsgSize)
                                .reason(v.distResult().name())
                                .clientInfo(clientInfo));
                            default -> {
                                // do nothing
                            }
                        }
                    }
                }
                return v;
            });
    }

    private CompletableFuture<IMQTTProtocolHelper.PubResult> doPub(long reqId,
                                                                   String topic,
                                                                   Message message,
                                                                   boolean isLWT,
                                                                   boolean background) {
        if (log.isTraceEnabled()) {
            log.trace("Disting msg: req={}, topic={}, qos={}, size={}",
                reqId, topic, message.getPubQoS(), message.getPayload().size());
        }

        CompletableFuture<PubResult> distTask =
            trackTask(sessionCtx.distClient.pub(reqId, topic, message, clientInfo), background);
        if (!message.getIsRetain()) {
            return distTask
                .thenApplyAsync(v -> new IMQTTProtocolHelper.PubResult(v, RetainReply.Result.RETAINED), ctx.executor());
        } else {
            CompletableFuture<RetainReply.Result> retainTask =
                trackTask(retainMessage(reqId, topic, message, isLWT), background);
            return allOf(retainTask, distTask).thenApplyAsync(
                v -> new IMQTTProtocolHelper.PubResult(distTask.join(), retainTask.join()), ctx.executor());
        }
    }

    private CompletableFuture<RetainReply.Result> retainMessage(long reqId, String topic, Message message,
                                                                boolean isLWT) {
        if (!settings.retainEnabled) {
            eventCollector.report(getLocal(MsgRetainedError.class)
                .reqId(reqId)
                .topic(topic)
                .qos(message.getPubQoS())
                .payload(message.getPayload().asReadOnlyByteBuffer())
                .size(message.getPayload().size())
                .reason("Retain Disabled")
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(RetainReply.Result.RETAINED);
        }
        if (log.isTraceEnabled()) {
            log.trace("Retaining message: reqId={}, qos={}, topic={}, size={}",
                reqId, message.getPubQoS(), topic, message.getPayload().size());
        }
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalRetainMessageSpaceBytes)) {
            eventCollector.report(getLocal(OutOfTenantResource.class)
                .reason(TotalRetainMessageSpaceBytes.name())
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(RetainReply.Result.EXCEED_LIMIT);
        }
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalRetainTopics)) {
            eventCollector.report(getLocal(OutOfTenantResource.class)
                .reason(TotalRetainTopics.name())
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(RetainReply.Result.EXCEED_LIMIT);
        }
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalRetainedMessagesPerSeconds)) {
            eventCollector.report(getLocal(OutOfTenantResource.class)
                .reason(TotalRetainedMessagesPerSeconds.name())
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(RetainReply.Result.EXCEED_LIMIT);
        }
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalRetainedBytesPerSecond)) {
            eventCollector.report(getLocal(OutOfTenantResource.class)
                .reason(TotalRetainedBytesPerSecond.name())
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(RetainReply.Result.EXCEED_LIMIT);
        }
        return sessionCtx.retainClient.retain(
                reqId,
                topic,
                message.getPubQoS(),
                message.getPayload(),
                message.getExpiryInterval(),
                clientInfo)
            .thenApplyAsync(v -> {
                if (log.isTraceEnabled()) {
                    log.trace("Message retained: reqId={}, result={}", v.getReqId(), v.getResult());
                }
                switch (v.getResult()) {
                    case RETAINED -> eventCollector.report(getLocal(MsgRetained.class)
                        .reqId(v.getReqId())
                        .topic(topic)
                        .isLastWill(isLWT)
                        .qos(message.getPubQoS())
                        .size(message.getPayload().size())
                        .clientInfo(clientInfo));
                    case CLEARED -> eventCollector.report(getLocal(RetainMsgCleared.class)
                        .reqId(v.getReqId())
                        .isLastWill(false)
                        .clientInfo(clientInfo)
                        .topic(topic));
                    case EXCEED_LIMIT -> eventCollector.report(getLocal(MsgRetainedError.class)
                        .reqId(v.getReqId())
                        .clientInfo(clientInfo)
                        .topic(topic)
                        .isLastWill(isLWT)
                        .qos(message.getPubQoS())
                        .payload(message.getPayload().asReadOnlyByteBuffer())
                        .size(message.getPayload().size())
                        .reason("Exceed Limit"));
                    case BACK_PRESSURE_REJECTED -> eventCollector.report(getLocal(MsgRetainedError.class)
                        .reqId(v.getReqId())
                        .clientInfo(clientInfo)
                        .topic(topic)
                        .isLastWill(isLWT)
                        .qos(message.getPubQoS())
                        .payload(message.getPayload().asReadOnlyByteBuffer())
                        .size(message.getPayload().size())
                        .reason("Server Busy"));
                    default -> eventCollector.report(getLocal(MsgRetainedError.class)
                        .reqId(v.getReqId())
                        .clientInfo(clientInfo)
                        .topic(topic)
                        .isLastWill(isLWT)
                        .qos(message.getPubQoS())
                        .payload(message.getPayload().asReadOnlyByteBuffer())
                        .size(message.getPayload().size())
                        .reason("Internal Error"));
                }
                return v.getResult();
            }, ctx.executor());
    }

    @Accessors(fluent = true)
    @Getter
    public static class SubMessage {
        private final String topic;
        private final Message message;
        private final ClientInfo publisher;
        private final String topicFilter;
        private final TopicFilterOption option;
        private final int bytesSize;
        private final boolean permissionGranted;
        private final boolean isDup; // if duplicated because of internal retry, should be dropped before send
        private final long inboxPos; // used in persistent session, the position in inbox

        public SubMessage(String topic,
                          Message message,
                          ClientInfo publisher,
                          String topicFilter,
                          TopicFilterOption option,
                          boolean permissionGranted,
                          boolean isDup) {
            this(topic, message, publisher, topicFilter, option, permissionGranted, isDup, 0);
        }

        public SubMessage(String topic,
                          Message message,
                          ClientInfo publisher,
                          String topicFilter,
                          TopicFilterOption option,
                          boolean permissionGranted,
                          boolean isDup,
                          long inboxPos) {
            this.topic = topic;
            this.message = message;
            this.publisher = publisher;
            this.topicFilter = topicFilter;
            this.option = option;
            this.permissionGranted = permissionGranted;
            this.isDup = isDup;
            this.bytesSize = topic.length() + topicFilter.length() + message.getPayload().size();
            this.inboxPos = inboxPos;
        }

        public boolean isRetain() {
            return message.getIsRetained() || option.getRetainAsPublished() && message.getIsRetain();
        }

        public QoS qos() {
            return QoS.forNumber(Math.min(message.getPubQoS().getNumber(), option.getQos().getNumber()));
        }

        public int estBytes() {
            return bytesSize;
        }
    }

    private static class ConfirmingMessage {
        final long seq;
        final SubMessage message;
        final long timestamp; // timestamp of first sent
        int sentCount = 1;
        boolean acked = false;

        private ConfirmingMessage(long seq, SubMessage message, long timestamp) {
            this.seq = seq;
            this.message = message;
            this.timestamp = timestamp;
        }

        int packetId() {
            return MQTTSessionIdUtil.packetId(seq);
        }

        void setAcked() {
            acked = true;
        }
    }
}
