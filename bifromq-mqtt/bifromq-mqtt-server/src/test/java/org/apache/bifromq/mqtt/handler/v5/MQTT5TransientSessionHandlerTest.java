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

package org.apache.bifromq.mqtt.handler.v5;


import static org.apache.bifromq.mqtt.handler.MQTTSessionIdUtil.userSessionId;
import static org.apache.bifromq.mqtt.handler.v5.reason.MQTT5UnsubAckReasonCode.NoSubscriptionExisted;
import static org.apache.bifromq.mqtt.handler.v5.reason.MQTT5UnsubAckReasonCode.NotAuthorized;
import static org.apache.bifromq.mqtt.handler.v5.reason.MQTT5UnsubAckReasonCode.Success;
import static org.apache.bifromq.mqtt.handler.v5.reason.MQTT5UnsubAckReasonCode.UnspecifiedError;
import static org.apache.bifromq.plugin.eventcollector.EventType.EXCEED_PUB_RATE;
import static org.apache.bifromq.plugin.eventcollector.EventType.EXCEED_RECEIVING_LIMIT;
import static org.apache.bifromq.plugin.eventcollector.EventType.INVALID_TOPIC;
import static org.apache.bifromq.plugin.eventcollector.EventType.INVALID_TOPIC_FILTER;
import static org.apache.bifromq.plugin.eventcollector.EventType.MALFORMED_TOPIC;
import static org.apache.bifromq.plugin.eventcollector.EventType.MQTT_SESSION_START;
import static org.apache.bifromq.plugin.eventcollector.EventType.MQTT_SESSION_STOP;
import static org.apache.bifromq.plugin.eventcollector.EventType.MSG_RETAINED;
import static org.apache.bifromq.plugin.eventcollector.EventType.MSG_RETAINED_ERROR;
import static org.apache.bifromq.plugin.eventcollector.EventType.PROTOCOL_VIOLATION;
import static org.apache.bifromq.plugin.eventcollector.EventType.PUB_ACKED;
import static org.apache.bifromq.plugin.eventcollector.EventType.PUB_RECED;
import static org.apache.bifromq.plugin.eventcollector.EventType.PUB_REC_DROPPED;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS0_DIST_ERROR;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS0_DROPPED;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS0_PUSHED;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS1_CONFIRMED;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS1_DIST_ERROR;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS1_PUSHED;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS2_CONFIRMED;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS2_DIST_ERROR;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS2_PUSHED;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS2_RECEIVED;
import static org.apache.bifromq.plugin.eventcollector.EventType.RETAIN_MSG_CLEARED;
import static org.apache.bifromq.plugin.eventcollector.EventType.SERVER_BUSY;
import static org.apache.bifromq.plugin.eventcollector.EventType.SUB_ACKED;
import static org.apache.bifromq.plugin.eventcollector.EventType.UNSUB_ACKED;
import static org.apache.bifromq.plugin.eventcollector.EventType.UNSUB_ACTION_DISALLOW;
import static org.apache.bifromq.plugin.settingprovider.Setting.MsgPubPerSec;
import static org.apache.bifromq.plugin.settingprovider.Setting.ReceivingMaximum;
import static org.apache.bifromq.plugin.settingprovider.Setting.RetainEnabled;
import static org.apache.bifromq.retain.rpc.proto.RetainReply.Result.CLEARED;
import static org.apache.bifromq.retain.rpc.proto.RetainReply.Result.ERROR;
import static org.apache.bifromq.retain.rpc.proto.RetainReply.Result.RETAINED;
import static io.netty.handler.codec.mqtt.MqttMessageType.DISCONNECT;
import static io.netty.handler.codec.mqtt.MqttMessageType.PUBACK;
import static io.netty.handler.codec.mqtt.MqttMessageType.PUBREC;
import static io.netty.handler.codec.mqtt.MqttMessageType.PUBREL;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.TOPIC_ALIAS;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.TOPIC_ALIAS_MAXIMUM;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.dist.client.PubResult;
import org.apache.bifromq.mqtt.handler.BaseSessionHandlerTest;
import org.apache.bifromq.mqtt.handler.ChannelAttrs;
import org.apache.bifromq.mqtt.handler.TenantSettings;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5DisconnectReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5PubAckReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5PubRecReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5SubAckReasonCode;
import org.apache.bifromq.mqtt.session.IMQTTTransientSession;
import org.apache.bifromq.mqtt.utils.MQTTMessageUtils;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Granted;
import org.apache.bifromq.plugin.authprovider.type.MQTTAction;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ExceedReceivingLimit;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.MQTTClientInfoConstants;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class MQTT5TransientSessionHandlerTest extends BaseSessionHandlerTest {

    private MQTT5TransientSessionHandler transientSessionHandler;
    private boolean shouldCleanSubs = false;

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) {
        super.setup(method);
        ChannelDuplexHandler sessionHandlerAdder = buildChannelHandler();
        mockSessionReg();
        channel = new EmbeddedChannel(true, true, new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ch.attr(ChannelAttrs.MQTT_SESSION_CTX).set(sessionContext);
                ch.attr(ChannelAttrs.PEER_ADDR).set(new InetSocketAddress(remoteIp, remotePort));
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new ChannelTrafficShapingHandler(512 * 1024, 512 * 1024));
                pipeline.addLast(MqttDecoder.class.getName(), new MqttDecoder(256 * 1024));
                pipeline.addLast(sessionHandlerAdder);
            }
        });
        //channel.freezeTime();
        transientSessionHandler = (MQTT5TransientSessionHandler) channel.pipeline().last();
    }

    @SneakyThrows
    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) {
        if (shouldCleanSubs) {
            when(distClient.removeRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(),
                anyLong())).thenReturn(CompletableFuture.completedFuture(null));
            channel.close();
            verify(localDistService, atLeast(1)).unmatch(anyLong(), anyString(), anyLong(), any());
        } else {
            channel.close();
        }
        shouldCleanSubs = false;
        super.tearDown(method);
    }

    @Override
    protected ChannelDuplexHandler buildChannelHandler() {
        MqttProperties mqttProperties = new MqttProperties();
        mqttProperties.add(new MqttProperties.IntegerProperty(TOPIC_ALIAS_MAXIMUM.value(), 10));
        return new ChannelDuplexHandler() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                super.channelActive(ctx);
                ctx.pipeline().addLast(
                    MQTT5TransientSessionHandler.builder().settings(new TenantSettings(tenantId, settingProvider))
                        .tenantMeter(tenantMeter).oomCondition(oomCondition).connMsg(
                            MqttMessageBuilders.connect().protocolVersion(MqttVersion.MQTT_5).properties(mqttProperties)
                                .build()).userSessionId(userSessionId(clientInfo)).keepAliveTimeSeconds(120)
                        .clientInfo(clientInfo).willMessage(null).ctx(ctx).build());
                ctx.pipeline().remove(this);
            }
        };
    }

    @Test
    public void transientMixedSub() {
        mockCheckPermission(true);
        mockDistMatch(true);
        mockRetainMatch();
        String[] tfs = {"t1", "a/#/a", "t2"};
        int[] qos = {0, 1, 2};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(tfs, qos);
        channel.writeInbound(subMessage);
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, new int[] {0, MQTT5SubAckReasonCode.TopicFilterInvalid.value(), 2});
        verifyEvent(MQTT_SESSION_START, INVALID_TOPIC_FILTER, SUB_ACKED);
        shouldCleanSubs = true;
    }

    @Test
    public void transientMixedSubWithDistSubFailed() {
        mockCheckPermission(true);
        mockDistMatch("testTopic0", true);
        mockDistMatch("testTopic1", true);
        mockDistMatch("testTopic2", false);
        mockRetainMatch();
        int[] qos = {0, 1, 2};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, new int[] {0, 1, 128});
        verifyEvent(MQTT_SESSION_START, SUB_ACKED);
        shouldCleanSubs = true;
    }

    @Test
    public void transientSubExceedInboxLimit() {
        mockCheckPermission(true);
        mockDistMatch(true);
        mockRetainMatch();
        int settingLimit = 10;
        String[] tfs = new String[settingLimit];
        int[] qos = new int[10];
        for (int index = 0; index < 10; index++) {
            tfs[index] = "t/" + index;
            qos[index] = 0;
        }
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(tfs, qos);
        channel.writeInbound(subMessage);
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, new int[settingLimit]);

        subMessage = MQTTMessageUtils.qoSMqttSubMessages(new String[] {"anotherTFS"}, new int[] {0});
        channel.writeInbound(subMessage);
        subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, new int[] {MQTT5SubAckReasonCode.QuotaExceeded.value()});
        verifyEvent(MQTT_SESSION_START, SUB_ACKED, SUB_ACKED);
        shouldCleanSubs = true;
    }

    @Test
    public void transientUnSub() {
        mockCheckPermission(true);
        mockDistMatch(true);
        mockDistUnMatch(true);

        int[] qos = {0, 1, 2};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        channel.runPendingTasks();
        channel.readOutbound();

        channel.writeInbound(MQTTMessageUtils.qoSMqttUnSubMessages(3));
        MqttUnsubAckMessage unsubAckMessage = channel.readOutbound();
        verifyMQTT5UnSubAck(unsubAckMessage, new int[] {Success.value(), Success.value(), Success.value()});
        verifyEvent(MQTT_SESSION_START, UNSUB_ACKED);
    }

    @Test
    public void transientUnSubWithoutSub() {
        mockCheckPermission(true);
        mockDistUnMatch(true);

        channel.writeInbound(MQTTMessageUtils.qoSMqttUnSubMessages(3));
        MqttUnsubAckMessage unsubAckMessage = channel.readOutbound();
        verifyMQTT5UnSubAck(unsubAckMessage,
            new int[] {NoSubscriptionExisted.value(), NoSubscriptionExisted.value(), NoSubscriptionExisted.value()});
        verifyEvent(MQTT_SESSION_START, UNSUB_ACKED);
    }

    @Test
    public void transientMixedUnSubWithDistUnSubFailed() {
        mockCheckPermission(true);
        mockDistMatch(true);
        mockDistUnMatch(true, false, true);

        int[] qos = {0, 1, 2};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);

        channel.writeInbound(MQTTMessageUtils.qoSMqttUnSubMessages(3));
        MqttUnsubAckMessage unsubAckMessage = channel.readOutbound();
        verifyMQTT5UnSubAck(unsubAckMessage, new int[] {Success.value(), UnspecifiedError.value(), Success.value()});
        verifyEvent(MQTT_SESSION_START, UNSUB_ACKED);
    }

    @Test
    public void unSubWithAuthFailed() {
        mockCheckPermission(true);
        mockDistMatch(true);
        mockDistUnMatch(true, false, true);

        int[] qos = {0, 1, 2};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);

        mockCheckPermission(false);
        channel.writeInbound(MQTTMessageUtils.qoSMqttUnSubMessages(3));
        MqttUnsubAckMessage unsubAckMessage = channel.readOutbound();
        verifyMQTT5UnSubAck(unsubAckMessage,
            new int[] {NotAuthorized.value(), NotAuthorized.value(), NotAuthorized.value()});
        verifyEvent(MQTT_SESSION_START, UNSUB_ACTION_DISALLOW, UNSUB_ACTION_DISALLOW, UNSUB_ACTION_DISALLOW,
            UNSUB_ACKED);
    }

    @Test
    public void c2sPubAlias() {
        mockCheckPermission(true);
        mockDistDist(true);
        when(distClient.pub(anyLong(), anyString(), any(Message.class), any(ClientInfo.class))).thenReturn(
            CompletableFuture.completedFuture(PubResult.OK));
        assertTrue(channel.isOpen());
        MqttPublishMessage message = MQTTMessageUtils.publishMQTT5QoS1Message(topic, 123, 1);
        channel.writeInbound(message);
        MqttMessage ackMessage = channel.readOutbound();
        assertEquals(((MqttMessageIdVariableHeader) ackMessage.variableHeader()).messageId(), 123);
        verifyEvent(MQTT_SESSION_START, PUB_ACKED);
        // pub with topic alias
        channel.writeInbound(MQTTMessageUtils.publishMQTT5QoS1Message("", 124, 1));
        ackMessage = channel.readOutbound();
        assertEquals(((MqttMessageIdVariableHeader) ackMessage.variableHeader()).messageId(), 124);
    }

    @Test
    public void handleQoS0Pub() {
        mockCheckPermission(true);
        mockDistDist(true);
        when(distClient.pub(anyLong(), anyString(), any(Message.class), any(ClientInfo.class))).thenReturn(
            CompletableFuture.completedFuture(PubResult.OK));
        assertTrue(channel.isOpen());
        MqttPublishMessage message = MQTTMessageUtils.publishQoS0Message(topic, 123);
        channel.writeInbound(message);
        verify(distClient, times(1)).pub(anyLong(), eq(topic), any(Message.class), eq(clientInfo));
    }

    @Test
    public void handleQoS0PubDistError() {
        mockCheckPermission(true);
        mockDistDist(false);
        assertTrue(channel.isOpen());

        MqttPublishMessage message = MQTTMessageUtils.publishQoS0Message(topic, 123);
        channel.writeInbound(message);
        verify(distClient, times(1)).pub(anyLong(), eq(topic), any(Message.class), eq(clientInfo));
        verifyEvent(MQTT_SESSION_START, QOS0_DIST_ERROR);
    }

    @Test
    public void handleQoS1Pub() {
        mockCheckPermission(true);
        mockDistDist(true);
        assertTrue(channel.isOpen());

        MqttPublishMessage message = MQTTMessageUtils.publishQoS1Message(topic, 123);
        channel.writeInbound(message);
        MqttMessage ackMessage = channel.readOutbound();
        assertEquals(((MqttMessageIdVariableHeader) ackMessage.variableHeader()).messageId(), 123);
        verifyEvent(MQTT_SESSION_START, PUB_ACKED);
    }

    @Test
    public void handleQoS1PubPacketIdInUse() {
        mockCheckPermission(true);
        when(distClient.pub(anyLong(), anyString(), any(), any(ClientInfo.class))).thenReturn(
            new CompletableFuture<>());
        assertTrue(channel.isOpen());

        MqttPublishMessage message = MQTTMessageUtils.publishQoS1Message(topic, 123);
        channel.writeInbound(message);
        channel.writeInbound(message);
        channel.advanceTimeBy(5, TimeUnit.SECONDS);
        channel.runPendingTasks();
        channel.flushOutbound();
        MqttMessage ackMessage = channel.readOutbound();
        assertEquals(((MqttPubReplyMessageVariableHeader) ackMessage.variableHeader()).messageId(), 123);
        assertEquals(((MqttPubReplyMessageVariableHeader) ackMessage.variableHeader()).reasonCode(),
            MQTT5PubAckReasonCode.PacketIdentifierInUse.value());
        assertTrue(channel.isOpen());
        verifyEvent(MQTT_SESSION_START, PROTOCOL_VIOLATION);
    }

    @Test
    public void handleQoS1PubDistError() {
        mockCheckPermission(true);
        mockDistDist(false);
        assertTrue(channel.isOpen());

        MqttPublishMessage message = MQTTMessageUtils.publishQoS1Message(topic, 123);
        channel.writeInbound(message);
        MqttMessage ackMessage = channel.readOutbound();
        // dist error still pub ack, record error log
        assertEquals(((MqttMessageIdVariableHeader) ackMessage.variableHeader()).messageId(), 123);
        verifyEvent(MQTT_SESSION_START, QOS1_DIST_ERROR, PUB_ACKED);
    }

    @Test
    public void handleQoS1PubDistRejected() {
        mockCheckPermission(true);
        mockDistBackPressure();
        assertTrue(channel.isOpen());

        MqttPublishMessage message = MQTTMessageUtils.publishQoS1Message(topic, 123);
        channel.writeInbound(message);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.runPendingTasks();
        assertFalse(channel.isOpen());
        verifyEvent(MQTT_SESSION_START, QOS1_DIST_ERROR, SERVER_BUSY, MQTT_SESSION_STOP);
    }

    @Test
    public void handleQoS2Pub() {
        mockCheckPermission(true);
        mockDistDist(true);
        assertTrue(channel.isOpen());

        // publish to channel
        MqttPublishMessage message = MQTTMessageUtils.publishQoS2Message(topic, 123);
        channel.writeInbound(message);
        MqttMessage mqttMessage = channel.readOutbound();
        assertEquals(mqttMessage.fixedHeader().messageType(), MqttMessageType.PUBREC);
        assertEquals(((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId(), 123);
        // publish release to channel
        channel.writeInbound(MQTTMessageUtils.publishRelMessage(123));
        mqttMessage = channel.readOutbound();
        assertEquals(mqttMessage.fixedHeader().messageType(), MqttMessageType.PUBCOMP);
        assertEquals(((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId(), 123);
        verifyEvent(MQTT_SESSION_START, PUB_RECED);
    }

    @Test
    public void handleQoS2PubPacketIdInUse() {
        mockCheckPermission(true);
        when(distClient.pub(anyLong(), anyString(), any(), any(ClientInfo.class))).thenReturn(
            new CompletableFuture<>());
        assertTrue(channel.isOpen());

        MqttPublishMessage message = MQTTMessageUtils.publishQoS2Message(topic, 123);
        channel.writeInbound(message);
        channel.writeInbound(message);
        channel.advanceTimeBy(5, TimeUnit.SECONDS);
        channel.runPendingTasks();
        channel.flushOutbound();
        MqttMessage ackMessage = channel.readOutbound();
        assertEquals(((MqttPubReplyMessageVariableHeader) ackMessage.variableHeader()).messageId(), 123);
        assertEquals(((MqttPubReplyMessageVariableHeader) ackMessage.variableHeader()).reasonCode(),
            MQTT5PubRecReasonCode.PacketIdentifierInUse.value());
        assertTrue(channel.isOpen());
        verifyEvent(MQTT_SESSION_START, PROTOCOL_VIOLATION);
    }

    @Test
    public void handleQoS2PubDistError() {
        mockCheckPermission(true);
        mockDistDist(false);
        assertTrue(channel.isOpen());

        // publish to channel
        MqttPublishMessage message = MQTTMessageUtils.publishQoS2Message(topic, 123);
        channel.writeInbound(message);
        // dist error still pub rec, record error log
        MqttMessage mqttMessage = channel.readOutbound();
        assertEquals(mqttMessage.fixedHeader().messageType(), MqttMessageType.PUBREC);
        assertEquals(((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId(), 123);
        // publish release to channel
        channel.writeInbound(MQTTMessageUtils.publishRelMessage(123));
        mqttMessage = channel.readOutbound();
        assertEquals(mqttMessage.fixedHeader().messageType(), MqttMessageType.PUBCOMP);
        assertEquals(((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId(), 123);
        verifyEvent(MQTT_SESSION_START, QOS2_DIST_ERROR, PUB_RECED);
    }

    @Test
    public void handleQoS2PubDistRejected() {
        mockCheckPermission(true);
        mockDistBackPressure();
        assertTrue(channel.isOpen());

        MqttPublishMessage message = MQTTMessageUtils.publishQoS2Message(topic, 123);
        channel.writeInbound(message);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.runPendingTasks();
        assertFalse(channel.isOpen());
        verifyEvent(MQTT_SESSION_START, QOS2_DIST_ERROR, SERVER_BUSY, MQTT_SESSION_STOP);
    }

    @Test
    public void qoS2PubWithUnWritable() {
        CompletableFuture<PubResult> distResult = new CompletableFuture<>();
        when(authProvider.checkPermission(any(ClientInfo.class), any(MQTTAction.class))).thenReturn(
            CompletableFuture.completedFuture(
                CheckResult.newBuilder().setGranted(Granted.newBuilder().build()).build()));
        when(distClient.pub(anyLong(), anyString(), any(), any(ClientInfo.class))).thenReturn(distResult);
        assertTrue(channel.isOpen());
        channel.writeInbound(MQTTMessageUtils.publishQoS2Message("testTopic", 123));

        // make channel unWritable and drop PubRec
        channel.writeOneOutbound(MQTTMessageUtils.largeMqttMessage(300 * 1024));
        channel.writeOneOutbound(MQTTMessageUtils.largeMqttMessage(300 * 1024));
        assertFalse(channel.isWritable());
        distResult.complete(PubResult.OK);
        channel.runPendingTasks();
        verifyEvent(MQTT_SESSION_START, PUB_REC_DROPPED);

        // flush channel
        channel.flush();
        channel.readOutbound();
        channel.readOutbound();
        assertTrue(channel.isWritable());

        // client did not receive PubRec, resend pub and receive PubRec
        channel.writeInbound(MQTTMessageUtils.publishQoS2Message(topic, 123));
        channel.runPendingTasks();
        MqttMessage mqttMessage = channel.readOutbound();
        assertEquals(mqttMessage.fixedHeader().messageType(), MqttMessageType.PUBREC);
        assertEquals(((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId(), 123);

        // continue to publish PubRel
        channel.writeInbound(MQTTMessageUtils.publishRelMessage(123));
        mqttMessage = channel.readOutbound();
        assertEquals(mqttMessage.fixedHeader().messageType(), MqttMessageType.PUBCOMP);
        assertEquals(((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId(), 123);
        verifyEvent(MQTT_SESSION_START, PUB_REC_DROPPED, PUB_RECED);
    }

    @Test
    public void retainDisabled() {
        when(settingProvider.provide(eq(RetainEnabled), anyString())).thenReturn(false);
        assertTrue(channel.isOpen());
        channel.pipeline().removeLast();
        channel.pipeline().addLast(new ChannelDuplexHandler() {
            @Override
            public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                super.channelActive(ctx);
                ctx.pipeline().addLast(MQTT5TransientSessionHandler.builder().connMsg(
                        MqttMessageBuilders.connect().protocolVersion(MqttVersion.MQTT_5).properties(new MqttProperties())
                            .build()).settings(new TenantSettings(tenantId, settingProvider)).tenantMeter(tenantMeter)
                    .oomCondition(oomCondition).userSessionId(userSessionId(clientInfo)).keepAliveTimeSeconds(120)
                    .clientInfo(clientInfo).willMessage(null).ctx(ctx).build());
                ctx.pipeline().remove(this);
            }
        });
        reset(eventCollector);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishRetainQoS1Message(topic, 123);
        channel.writeInbound(publishMessage);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runPendingTasks();
        channel.flushOutbound();
        MqttMessage disconnMessage = channel.readOutbound();
        assertEquals(disconnMessage.fixedHeader().messageType(), DISCONNECT);
        assertEquals(((MqttReasonCodeAndPropertiesVariableHeader) disconnMessage.variableHeader()).reasonCode(),
            MQTT5DisconnectReasonCode.RetainNotSupported.value());
        assertFalse(channel.isActive());
        verifyEvent(PROTOCOL_VIOLATION, MQTT_SESSION_STOP);
    }

    @Test
    public void malformedTopic() {
        assertTrue(channel.isOpen());
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS0Message("topic\u0000", 123);
        channel.writeInbound(publishMessage);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runPendingTasks();
        channel.flushOutbound();
        MqttMessage disconnMessage = channel.readOutbound();
        assertEquals(disconnMessage.fixedHeader().messageType(), DISCONNECT);
        assertEquals(((MqttReasonCodeAndPropertiesVariableHeader) disconnMessage.variableHeader()).reasonCode(),
            MQTT5DisconnectReasonCode.MalformedPacket.value());
        assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, MALFORMED_TOPIC, MQTT_SESSION_STOP);
    }

    @Test
    public void malformedResponseTopic() {
        assertTrue(channel.isOpen());
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS0Message("topic", 123, "topic\u0000");
        channel.writeInbound(publishMessage);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runPendingTasks();
        channel.flushOutbound();
        MqttMessage disconnMessage = channel.readOutbound();
        assertEquals(disconnMessage.fixedHeader().messageType(), DISCONNECT);
        assertEquals(((MqttReasonCodeAndPropertiesVariableHeader) disconnMessage.variableHeader()).reasonCode(),
            MQTT5DisconnectReasonCode.MalformedPacket.value());
        assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, PROTOCOL_VIOLATION, MQTT_SESSION_STOP);
    }

    @Test
    public void emptyTopic() {
        assertTrue(channel.isOpen());
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS0Message("", 123);
        channel.writeInbound(publishMessage);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runPendingTasks();
        channel.flushOutbound();
        MqttMessage disconnMessage = channel.readOutbound();
        assertEquals(disconnMessage.fixedHeader().messageType(), DISCONNECT);
        assertEquals(((MqttReasonCodeAndPropertiesVariableHeader) disconnMessage.variableHeader()).reasonCode(),
            MQTT5DisconnectReasonCode.ProtocolError.value());
        assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, PROTOCOL_VIOLATION, MQTT_SESSION_STOP);
    }

    @Test
    public void invalidTopicQoS0() {
        assertTrue(channel.isOpen());
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS0Message("$share/g/testTopic", 123);
        channel.writeInbound(publishMessage);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runPendingTasks();
        channel.flushOutbound();
        MqttMessage disconnMessage = channel.readOutbound();
        assertEquals(disconnMessage.fixedHeader().messageType(), DISCONNECT);
        assertEquals(((MqttReasonCodeAndPropertiesVariableHeader) disconnMessage.variableHeader()).reasonCode(),
            MQTT5DisconnectReasonCode.TopicNameInvalid.value());
        assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, INVALID_TOPIC, MQTT_SESSION_STOP);
    }

    @Test
    public void invalidResponseTopicQoS0() {
        assertTrue(channel.isOpen());
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS0Message("topic", 123, "$share/g/testTopic");
        channel.writeInbound(publishMessage);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runPendingTasks();
        channel.flushOutbound();
        MqttMessage disconnMessage = channel.readOutbound();
        assertEquals(disconnMessage.fixedHeader().messageType(), DISCONNECT);
        assertEquals(((MqttReasonCodeAndPropertiesVariableHeader) disconnMessage.variableHeader()).reasonCode(),
            MQTT5DisconnectReasonCode.TopicNameInvalid.value());
        assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, PROTOCOL_VIOLATION, MQTT_SESSION_STOP);
    }

    @Test
    public void invalidTopicQoS1() {
        assertTrue(channel.isOpen());
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS1Message("$share/g/testTopic", 123);
        channel.writeInbound(publishMessage);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runPendingTasks();
        channel.flushOutbound();
        MqttMessage pubAckMsg = channel.readOutbound();
        assertEquals(pubAckMsg.fixedHeader().messageType(), PUBACK);
        assertEquals(((MqttPubReplyMessageVariableHeader) pubAckMsg.variableHeader()).reasonCode(),
            MQTT5PubAckReasonCode.TopicNameInvalid.value());
        assertTrue(channel.isActive());
        verifyEvent(MQTT_SESSION_START, INVALID_TOPIC);
    }

    @Test
    public void invalidResponseTopicQoS1() {
        assertTrue(channel.isOpen());
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS1Message("topic", 123, "$share/g/testTopic");
        channel.writeInbound(publishMessage);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runPendingTasks();
        channel.flushOutbound();
        MqttMessage pubAckMsg = channel.readOutbound();
        assertEquals(pubAckMsg.fixedHeader().messageType(), PUBACK);
        assertEquals(((MqttPubReplyMessageVariableHeader) pubAckMsg.variableHeader()).reasonCode(),
            MQTT5PubAckReasonCode.TopicNameInvalid.value());
        assertTrue(channel.isActive());
        verifyEvent(MQTT_SESSION_START, PROTOCOL_VIOLATION);
    }

    @Test
    public void invalidTopicQoS2() {
        assertTrue(channel.isOpen());
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS2Message("$share/g/testTopic", 123);
        channel.writeInbound(publishMessage);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runPendingTasks();
        channel.flushOutbound();
        MqttMessage pubAckMsg = channel.readOutbound();
        assertEquals(pubAckMsg.fixedHeader().messageType(), PUBREC);
        assertEquals(((MqttPubReplyMessageVariableHeader) pubAckMsg.variableHeader()).reasonCode(),
            MQTT5PubAckReasonCode.TopicNameInvalid.value());
        assertTrue(channel.isActive());
        verifyEvent(MQTT_SESSION_START, INVALID_TOPIC);
    }

    @Test
    public void invalidResponseTopicQoS2() {
        assertTrue(channel.isOpen());
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS2Message("topic", 123, "$share/g/testTopic");
        channel.writeInbound(publishMessage);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runPendingTasks();
        channel.flushOutbound();
        MqttMessage pubAckMsg = channel.readOutbound();
        assertEquals(pubAckMsg.fixedHeader().messageType(), PUBREC);
        assertEquals(((MqttPubReplyMessageVariableHeader) pubAckMsg.variableHeader()).reasonCode(),
            MQTT5PubAckReasonCode.TopicNameInvalid.value());
        assertTrue(channel.isActive());
        verifyEvent(MQTT_SESSION_START, PROTOCOL_VIOLATION);
    }

    @Test
    public void pubTooFast() {
        mockCheckPermission(true);
        mockDistDist(true);
        when(settingProvider.provide(eq(MsgPubPerSec), anyString())).thenReturn(1);
        assertTrue(channel.isOpen());
        channel.pipeline().removeLast();
        channel.pipeline().addLast(new ChannelDuplexHandler() {
            @Override
            public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                super.channelActive(ctx);
                ctx.pipeline().addLast(MQTT5TransientSessionHandler.builder().connMsg(
                        MqttMessageBuilders.connect().protocolVersion(MqttVersion.MQTT_5).properties(new MqttProperties())
                            .build()).settings(new TenantSettings(tenantId, settingProvider)).tenantMeter(tenantMeter)
                    .oomCondition(oomCondition).userSessionId(userSessionId(clientInfo)).keepAliveTimeSeconds(120)
                    .clientInfo(clientInfo).willMessage(null).ctx(ctx).build());
                ctx.pipeline().remove(this);
            }
        });
        reset(eventCollector);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS1Message("testTopic", 1);
        MqttPublishMessage publishMessage2 = MQTTMessageUtils.publishQoS1Message("testTopic", 2);
        channel.writeInbound(publishMessage);
        channel.writeInbound(publishMessage2);
        MqttMessage ackMessage = channel.readOutbound();
        assertEquals(((MqttMessageIdVariableHeader) ackMessage.variableHeader()).messageId(), 1);

        channel.advanceTimeBy(5, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.runPendingTasks();
        MqttMessage message = channel.readOutbound();

        assertEquals(message.fixedHeader().messageType(), DISCONNECT);
        verifyEvent(PUB_ACKED, EXCEED_PUB_RATE, MQTT_SESSION_STOP);
        assertFalse(channel.isOpen());
    }

    @Test
    public void exceedReceiveMaximum() {
        mockCheckPermission(true);
        when(settingProvider.provide(eq(ReceivingMaximum), anyString())).thenReturn(1);
        assertTrue(channel.isOpen());
        channel.pipeline().removeLast();
        // add new MQTT3TransientSessionHandler with MsgPubPerSec = 1
        channel.pipeline().addLast(new ChannelDuplexHandler() {
            @Override
            public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                super.channelActive(ctx);
                ctx.pipeline().addLast(MQTT5TransientSessionHandler.builder().connMsg(
                        MqttMessageBuilders.connect().protocolVersion(MqttVersion.MQTT_5).properties(new MqttProperties())
                            .build()).settings(new TenantSettings(tenantId, settingProvider)).tenantMeter(tenantMeter)
                    .oomCondition(oomCondition).userSessionId(userSessionId(clientInfo)).keepAliveTimeSeconds(120)
                    .clientInfo(clientInfo).willMessage(null).ctx(ctx).build());
                ctx.pipeline().remove(this);
            }
        });

        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS1Message("testTopic", 1);
        MqttPublishMessage publishMessage2 = MQTTMessageUtils.publishQoS1Message("testTopic", 2);
        channel.writeInbound(publishMessage);
        channel.writeInbound(publishMessage2);

        channel.advanceTimeBy(5, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.runPendingTasks();
        verify(eventCollector).report(argThat(
            event -> event.type() == EXCEED_RECEIVING_LIMIT && ((ExceedReceivingLimit) event).limit() == 1 &&
                ((ExceedReceivingLimit) event).clientInfo() != null));

        // disconnect channel in MQTT5
        assertFalse(channel.isOpen());
    }

    @Test
    public void qoS1PubRetain() {
        mockCheckPermission(true);
        mockDistDist(true);
        mockRetainPipeline(RETAINED);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishRetainQoS1Message(topic, 123);
        channel.writeInbound(publishMessage);
        verifyEvent(MQTT_SESSION_START, MSG_RETAINED, PUB_ACKED);
    }

    @Test
    public void qoS1PubRetainClear() {
        mockCheckPermission(true);
        mockDistDist(true);
        mockRetainPipeline(CLEARED);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishRetainQoS1Message(topic, 123);
        channel.writeInbound(publishMessage);
        verifyEvent(MQTT_SESSION_START, RETAIN_MSG_CLEARED, PUB_ACKED);
    }

    @Test
    public void qoS1PubRetainFailed() {
        mockCheckPermission(true);
        mockDistDist(true);
        mockRetainPipeline(ERROR);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishRetainQoS1Message(topic, 123);
        channel.writeInbound(publishMessage);
        verifyEvent(MQTT_SESSION_START, MSG_RETAINED_ERROR, PUB_ACKED);
    }

    @Test
    public void qoS2PubRetainFailed() {
        mockCheckPermission(true);
        mockDistDist(true);
        mockRetainPipeline(ERROR);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishRetainQoS2Message(topic, 123);
        channel.writeInbound(publishMessage);
        verifyEvent(MQTT_SESSION_START, MSG_RETAINED_ERROR, PUB_RECED);
    }

    @Test
    public void qos0Pub() {
        mockCheckPermission(true);
        mockDistMatch(true);
        transientSessionHandler.subscribe(System.nanoTime(), topicFilter, QoS.AT_MOST_ONCE);
        channel.runPendingTasks();
        ArgumentCaptor<Long> longCaptor = ArgumentCaptor.forClass(Long.class);
        verify(localDistService).match(anyLong(), eq(topicFilter), longCaptor.capture(), any());

        transientSessionHandler.publish(s2cMQTT5MessageList(topic, 5, QoS.AT_MOST_ONCE),
            Collections.singleton(new IMQTTTransientSession.MatchedTopicFilter(topicFilter, longCaptor.getValue())));
        channel.runPendingTasks();
        for (int i = 0; i < 5; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_MOST_ONCE_VALUE);
            assertEquals(message.variableHeader().properties().getProperties(TOPIC_ALIAS.value()).get(0).value(), 1);
            if (i == 0) {
                assertEquals(message.variableHeader().topicName(), topic);
            }
        }
        verifyEvent(MQTT_SESSION_START, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED);
    }

    @Test
    public void qos0PubExceedBufferCapacity() {
        mockCheckPermission(true);
        mockDistMatch(true);
        transientSessionHandler.subscribe(System.nanoTime(), topicFilter, QoS.AT_MOST_ONCE);
        channel.runPendingTasks();
        ArgumentCaptor<Long> longCaptor = ArgumentCaptor.forClass(Long.class);
        verify(localDistService).match(anyLong(), eq(topicFilter), longCaptor.capture(), any());

        channel.writeOneOutbound(MQTTMessageUtils.largeMqttMessage(300 * 1024));
        List<ByteBuffer> payloads = s2cMessagesPayload(1, 32 * 1024);
        transientSessionHandler.publish(s2cMQTT5MessageList(topic, payloads, QoS.AT_MOST_ONCE),
            Collections.singleton(new IMQTTTransientSession.MatchedTopicFilter(topicFilter, longCaptor.getValue())));
        channel.runPendingTasks();
        MqttPublishMessage message = channel.readOutbound();
        assertNull(message);
        verifyEvent(MQTT_SESSION_START, QOS0_DROPPED);
    }

    @Test
    public void qos1PubAndAck() {
        mockCheckPermission(true);
        mockDistMatch(true);
        transientSessionHandler.subscribe(System.nanoTime(), topicFilter, QoS.AT_LEAST_ONCE);
        channel.runPendingTasks();
        ArgumentCaptor<Long> longCaptor = ArgumentCaptor.forClass(Long.class);
        verify(localDistService).match(anyLong(), eq(topicFilter), longCaptor.capture(), any());

        int messageCount = 3;
        transientSessionHandler.publish(s2cMQTT5MessageList(topic, messageCount, QoS.AT_LEAST_ONCE),
            Collections.singleton(new IMQTTTransientSession.MatchedTopicFilter(topicFilter, longCaptor.getValue())));
        channel.runPendingTasks();
        // s2c pub received and ack
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            if (i == 0) {
                assertEquals(message.variableHeader().topicName(), topic);
            }
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
            assertEquals(message.variableHeader().properties().getProperties(TOPIC_ALIAS.value()).get(0).value(), 1);
            channel.writeInbound(MQTTMessageUtils.pubAckMessage(message.variableHeader().packetId()));
        }
        verifyEvent(MQTT_SESSION_START, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_CONFIRMED, QOS1_CONFIRMED,
            QOS1_CONFIRMED);
    }

    @Test
    public void qoS2PubAndRel() {
        mockCheckPermission(true);
        mockDistMatch(true);
        transientSessionHandler.subscribe(System.nanoTime(), topicFilter, QoS.EXACTLY_ONCE);
        channel.runPendingTasks();
        ArgumentCaptor<Long> longCaptor = ArgumentCaptor.forClass(Long.class);
        verify(localDistService).match(anyLong(), eq(topicFilter), longCaptor.capture(), any());

        transientSessionHandler.publish(s2cMQTT5MessageList(topic, 1, QoS.EXACTLY_ONCE),
            Collections.singleton(new IMQTTTransientSession.MatchedTopicFilter(topicFilter, longCaptor.getValue())));
        channel.runPendingTasks();
        // s2c pub received and rec
        MqttPublishMessage message = channel.readOutbound();
        assertEquals(message.fixedHeader().qosLevel().value(), QoS.EXACTLY_ONCE_VALUE);
        assertEquals(message.variableHeader().topicName(), topic);
        channel.writeInbound(MQTTMessageUtils.publishMQTT5RecMessage(message.variableHeader().packetId()));
        channel.runPendingTasks();
        // pubRel received and comp
        MqttMessage pubRel = channel.readOutbound();
        assertEquals(pubRel.fixedHeader().messageType(), PUBREL);
        channel.writeInbound(
            MQTTMessageUtils.publishCompMessage(((MqttMessageIdVariableHeader) pubRel.variableHeader()).messageId()));
        verifyEvent(MQTT_SESSION_START, QOS2_PUSHED, QOS2_RECEIVED, QOS2_CONFIRMED);
    }

    @Test
    public void dedup() {
        mockCheckPermission(true);
        mockDistMatch(true);
        transientSessionHandler.subscribe(System.nanoTime(), topicFilter, QoS.AT_MOST_ONCE);
        channel.runPendingTasks();
        ArgumentCaptor<Long> longCaptor = ArgumentCaptor.forClass(Long.class);
        verify(localDistService).match(anyLong(), eq(topicFilter), longCaptor.capture(), any());

        TopicMessagePack topicMessagePack = TopicMessagePack.newBuilder().setTopic(topic).addMessage(
                TopicMessagePack.PublisherPack.newBuilder().setPublisher(
                        ClientInfo.newBuilder().putMetadata(MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY, "channel1").build())
                    .addMessage(Message.newBuilder().setMessageId(0).setExpiryInterval(Integer.MAX_VALUE)
                        .setPayload(ByteString.EMPTY).setTimestamp(HLC.INST.get()).setPubQoS(QoS.AT_MOST_ONCE).build()))
            .build();

        transientSessionHandler.publish(topicMessagePack,
            Collections.singleton(new IMQTTTransientSession.MatchedTopicFilter(topicFilter, longCaptor.getValue())));
        channel.runPendingTasks();
        // duplicate pub should be dropped
        transientSessionHandler.publish(topicMessagePack,
            Collections.singleton(new IMQTTTransientSession.MatchedTopicFilter(topicFilter, longCaptor.getValue())));
        channel.runPendingTasks();
        MqttPublishMessage message = channel.readOutbound();
        assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_MOST_ONCE_VALUE);
        assertEquals(message.variableHeader().properties().getProperties(TOPIC_ALIAS.value()).get(0).value(), 1);
        assertEquals(message.variableHeader().topicName(), topic);

        verifyEvent(MQTT_SESSION_START, QOS0_PUSHED, QOS0_DROPPED);
    }

    @Test
    public void ignoreDedupNonMQTTPublisher() {
        mockCheckPermission(true);
        mockDistMatch(true);
        transientSessionHandler.subscribe(System.nanoTime(), topicFilter, QoS.AT_MOST_ONCE);
        channel.runPendingTasks();
        ArgumentCaptor<Long> longCaptor = ArgumentCaptor.forClass(Long.class);
        verify(localDistService).match(anyLong(), eq(topicFilter), longCaptor.capture(), any());

        TopicMessagePack topicMessagePack = TopicMessagePack.newBuilder().setTopic(topic).addMessage(
                TopicMessagePack.PublisherPack.newBuilder()
                    .setPublisher(ClientInfo.newBuilder().build()) // non MQTT publisher
                    .addMessage(Message.newBuilder().setMessageId(0).setExpiryInterval(Integer.MAX_VALUE)
                        .setPayload(ByteString.EMPTY).setTimestamp(HLC.INST.get()).setPubQoS(QoS.AT_MOST_ONCE).build()))
            .build();

        transientSessionHandler.publish(topicMessagePack,
            Collections.singleton(new IMQTTTransientSession.MatchedTopicFilter(topicFilter, longCaptor.getValue())));
        channel.runPendingTasks();
        // duplicate pub should be dropped
        transientSessionHandler.publish(topicMessagePack,
            Collections.singleton(new IMQTTTransientSession.MatchedTopicFilter(topicFilter, longCaptor.getValue())));
        channel.runPendingTasks();
        MqttPublishMessage message = channel.readOutbound();
        assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_MOST_ONCE_VALUE);
        assertEquals(message.variableHeader().properties().getProperties(TOPIC_ALIAS.value()).get(0).value(), 1);
        assertEquals(message.variableHeader().topicName(), topic);

        verifyEvent(MQTT_SESSION_START, QOS0_PUSHED, QOS0_PUSHED);

    }

    @Test
    public void ignoreDedupRetainMessage() {
        mockCheckPermission(true);
        mockDistMatch(true);
        transientSessionHandler.subscribe(System.nanoTime(), topicFilter, QoS.AT_MOST_ONCE);
        channel.runPendingTasks();
        ArgumentCaptor<Long> longCaptor = ArgumentCaptor.forClass(Long.class);
        verify(localDistService).match(anyLong(), eq(topicFilter), longCaptor.capture(), any());

        long now = HLC.INST.get();
        TopicMessagePack topicMessagePack = TopicMessagePack.newBuilder().setTopic(topic).addMessage(
            TopicMessagePack.PublisherPack.newBuilder()
                .setPublisher(ClientInfo.newBuilder().build()) // non MQTT publisher
                .addMessage(Message.newBuilder().setMessageId(0).setExpiryInterval(Integer.MAX_VALUE)
                    .setPayload(ByteString.EMPTY).setTimestamp(now).setPubQoS(QoS.AT_MOST_ONCE).build())).build();

        TopicMessagePack retainMessagePack = TopicMessagePack.newBuilder().setTopic(topic).addMessage(
            TopicMessagePack.PublisherPack.newBuilder()
                .setPublisher(ClientInfo.newBuilder().build()) // non MQTT publisher
                .addMessage(Message.newBuilder().setMessageId(0).setExpiryInterval(Integer.MAX_VALUE)
                    .setPayload(ByteString.EMPTY).setIsRetain(true).setTimestamp(now - 100).setPubQoS(QoS.AT_MOST_ONCE)
                    .build())).build();

        transientSessionHandler.publish(topicMessagePack,
            Collections.singleton(new IMQTTTransientSession.MatchedTopicFilter(topicFilter, longCaptor.getValue())));
        channel.runPendingTasks();
        // duplicate pub should be dropped
        transientSessionHandler.publish(retainMessagePack,
            Collections.singleton(new IMQTTTransientSession.MatchedTopicFilter(topicFilter, longCaptor.getValue())));
        channel.runPendingTasks();
        MqttPublishMessage message = channel.readOutbound();
        assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_MOST_ONCE_VALUE);
        assertEquals(message.variableHeader().properties().getProperties(TOPIC_ALIAS.value()).get(0).value(), 1);
        assertEquals(message.variableHeader().topicName(), topic);

        verifyEvent(MQTT_SESSION_START, QOS0_PUSHED, QOS0_PUSHED);

    }
}
