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

package org.apache.bifromq.mqtt.handler.v3;


import static io.netty.handler.codec.mqtt.MqttMessageType.PUBREL;
import static org.apache.bifromq.inbox.rpc.proto.UnsubReply.Code.ERROR;
import static org.apache.bifromq.inbox.rpc.proto.UnsubReply.Code.OK;
import static org.apache.bifromq.mqtt.handler.MQTTSessionIdUtil.userSessionId;
import static org.apache.bifromq.plugin.eventcollector.EventType.INBOX_TRANSIENT_ERROR;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS0_DROPPED;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS0_PUSHED;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS1_CONFIRMED;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS1_DROPPED;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS1_PUSHED;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS2_CONFIRMED;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS2_DROPPED;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS2_PUSHED;
import static org.apache.bifromq.plugin.eventcollector.EventType.QOS2_RECEIVED;
import static org.apache.bifromq.plugin.eventcollector.EventType.RETAIN_MSG_MATCHED;
import static org.apache.bifromq.plugin.eventcollector.EventType.SUB_ACKED;
import static org.apache.bifromq.plugin.eventcollector.EventType.UNSUB_ACKED;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;
import static org.apache.bifromq.type.QoS.AT_LEAST_ONCE;
import static org.apache.bifromq.type.QoS.EXACTLY_ONCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.rpc.proto.CommitReply;
import org.apache.bifromq.inbox.rpc.proto.CommitRequest;
import org.apache.bifromq.inbox.rpc.proto.UnsubReply;
import org.apache.bifromq.inbox.storage.proto.Fetched.Result;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import org.apache.bifromq.inbox.storage.proto.InboxMessage;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.mqtt.handler.BaseSessionHandlerTest;
import org.apache.bifromq.mqtt.handler.ChannelAttrs;
import org.apache.bifromq.mqtt.handler.MQTTPacketFilter;
import org.apache.bifromq.mqtt.handler.TenantSettings;
import org.apache.bifromq.mqtt.utils.MQTTMessageUtils;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Denied;
import org.apache.bifromq.plugin.eventcollector.Event;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.OversizePacketDropped;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Confirmed;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Dropped;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Confirmed;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Dropped;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicFilterOption;
import org.apache.bifromq.type.TopicMessage;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class MQTT3PersistentSessionHandlerTest extends BaseSessionHandlerTest {

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) {
        super.setup(method);
        ChannelDuplexHandler sessionHandlerAdder = buildChannelHandler();
        mockInboxReader();
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
    }

    @SneakyThrows
    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) {
        super.tearDown(method);
        fetchHints.clear();
    }

    @Override
    protected ChannelDuplexHandler buildChannelHandler() {
        return new ChannelDuplexHandler() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                super.channelActive(ctx);
                ctx.pipeline().addLast(MQTT3PersistentSessionHandler.builder()
                    .settings(new TenantSettings(tenantId, settingProvider))
                    .tenantMeter(tenantMeter)
                    .inboxVersion(InboxVersion.newBuilder().setMod(0).setIncarnation(0).build())
                    .oomCondition(oomCondition)
                    .userSessionId(userSessionId(clientInfo))
                    .keepAliveTimeSeconds(120)
                    .clientInfo(clientInfo)
                    .noDelayLWT(null)
                    .ctx(ctx)
                    .build());
                ctx.pipeline().remove(this);
            }
        };
    }

    @Test
    public void persistentQoS0Sub() {
        mockCheckPermission(true);
        mockInboxSub(QoS.AT_MOST_ONCE, true);
        mockRetainMatch();
        int[] qos = {0, 0, 0};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        channel.runPendingTasks();
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, qos);
        verifyEvent(RETAIN_MSG_MATCHED, RETAIN_MSG_MATCHED, RETAIN_MSG_MATCHED, SUB_ACKED);
    }

    @Test
    public void persistentQoS1Sub() {
        mockCheckPermission(true);
        mockInboxSub(QoS.AT_LEAST_ONCE, true);
        mockRetainMatch();
        int[] qos = {1, 1, 1};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        channel.runPendingTasks();
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, qos);
        verifyEvent(RETAIN_MSG_MATCHED, RETAIN_MSG_MATCHED, RETAIN_MSG_MATCHED, SUB_ACKED);
    }

    @Test
    public void persistentQoS2Sub() {
        mockCheckPermission(true);
        mockInboxSub(QoS.EXACTLY_ONCE, true);
        mockRetainMatch();
        int[] qos = {2, 2, 2};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        channel.runPendingTasks();
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, qos);
        verifyEvent(RETAIN_MSG_MATCHED, RETAIN_MSG_MATCHED, RETAIN_MSG_MATCHED, SUB_ACKED);
    }

    @Test
    public void persistentMixedSub() {
        mockCheckPermission(true);
        mockInboxSub(QoS.AT_MOST_ONCE, true);
        mockInboxSub(QoS.AT_LEAST_ONCE, true);
        mockInboxSub(QoS.EXACTLY_ONCE, true);
        mockRetainMatch();
        int[] qos = {0, 1, 2};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        channel.runPendingTasks();
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, qos);
        verifyEvent(RETAIN_MSG_MATCHED, RETAIN_MSG_MATCHED, RETAIN_MSG_MATCHED, SUB_ACKED);
    }

    @Test
    public void persistentUnSub() {
        mockCheckPermission(true);
        when(inboxClient.unsub(any())).thenReturn(
            CompletableFuture.completedFuture(UnsubReply.newBuilder().setCode(OK).build()));
        channel.writeInbound(MQTTMessageUtils.qoSMqttUnSubMessages(3));
        MqttUnsubAckMessage unsubAckMessage = channel.readOutbound();
        Assert.assertNotNull(unsubAckMessage);
        verifyEvent(UNSUB_ACKED);
    }

    @Test
    public void persistentMixedSubWithDistUnSubFailed() {
        mockCheckPermission(true);
        mockDistUnmatch(true, false, true);
        when(inboxClient.unsub(any()))
            .thenReturn(CompletableFuture.completedFuture(UnsubReply.newBuilder().setCode(OK).build()))
            .thenReturn(CompletableFuture.completedFuture(UnsubReply.newBuilder().setCode(ERROR).build()))
            .thenReturn(CompletableFuture.completedFuture(UnsubReply.newBuilder().setCode(OK).build()));
        channel.writeInbound(MQTTMessageUtils.qoSMqttUnSubMessages(3));
        MqttUnsubAckMessage unsubAckMessage = channel.readOutbound();
        Assert.assertNotNull(unsubAckMessage);
        verifyEvent(UNSUB_ACKED);
    }

    @Test
    public void qoS0Pub() {
        mockInboxCommit(CommitReply.Code.OK);
        mockCheckPermission(true);
        inboxFetchConsumer.accept(fetch(5, 128, QoS.AT_MOST_ONCE));
        channel.runPendingTasks();
        for (int i = 0; i < 5; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_MOST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), topic);
        }
        verifyEvent(QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED);
        assertEquals(fetchHints.size(), 1);
    }

    @Test
    public void qos0PubWithMultipleTopicFilters() {
        mockInboxCommit(CommitReply.Code.OK);
        mockCheckPermission(true);
        InboxMessage inboxMessage = InboxMessage.newBuilder()
            .setSeq(0)
            .putMatchedTopicFilter("#", TopicFilterOption.newBuilder().setQos(QoS.AT_MOST_ONCE).build())
            .putMatchedTopicFilter("+", TopicFilterOption.newBuilder().setQos(QoS.AT_MOST_ONCE).build())
            .setMsg(
                TopicMessage.newBuilder()
                    .setTopic(topic)
                    .setMessage(Message.newBuilder()
                        .setMessageId(0)
                        .setPayload(ByteString.copyFromUtf8("payload"))
                        .setTimestamp(HLC.INST.get())
                        .setExpiryInterval(120)
                        .setPubQoS(QoS.AT_MOST_ONCE)
                        .build()
                    )
                    .setPublisher(
                        ClientInfo.newBuilder()
                            .setType(MQTT_TYPE_VALUE)
                            .build()
                    )
                    .build()
            ).build();

        inboxFetchConsumer.accept(Fetched.newBuilder().addQos0Msg(inboxMessage).build());
        channel.runPendingTasks();
        for (int i = 0; i < 2; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_MOST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), topic);
        }
        verifyEvent(QOS0_PUSHED, QOS0_PUSHED);
        assertEquals(fetchHints.size(), 1);
    }

    @Test
    public void tryLaterImmediateOnly() {
        fetchHints.clear();
        inboxFetchConsumer.accept(Fetched.newBuilder().setResult(Result.TRY_LATER).build());
        channel.runPendingTasks();
        assertEquals(fetchHints.size(), 1);
        channel.advanceTimeBy(60, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.runPendingTasks();
        assertEquals(fetchHints.size(), 1);
    }

    @Test
    public void backPressureSchedulesOneShotHint() {
        fetchHints.clear();
        inboxFetchConsumer.accept(Fetched.newBuilder().setResult(Result.BACK_PRESSURE_REJECTED).build());
        channel.runPendingTasks();
        assertEquals(fetchHints.size(), 0);
        channel.advanceTimeBy(60, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.runPendingTasks();
        assertEquals(fetchHints.size(), 1);
        channel.advanceTimeBy(60, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.runPendingTasks();
        assertEquals(fetchHints.size(), 1);
    }

    @Test
    public void okCancelsScheduledHint() {
        fetchHints.clear();
        inboxFetchConsumer.accept(Fetched.newBuilder().setResult(Result.BACK_PRESSURE_REJECTED).build());
        channel.runPendingTasks();
        assertEquals(fetchHints.size(), 0);
        inboxFetchConsumer.accept(Fetched.newBuilder().setResult(Result.OK).build());
        channel.runPendingTasks();
        channel.advanceTimeBy(60, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.runPendingTasks();
        assertEquals(fetchHints.size(), 0);
    }

    @Test
    public void qoS0PubAuthFailed() {
        // not by pass
        when(authProvider.checkPermission(any(ClientInfo.class),
            argThat(action -> action.hasSub() && action.getSub().getQos() == QoS.AT_MOST_ONCE)))
            .thenReturn(CompletableFuture.completedFuture(CheckResult.newBuilder()
                .setDenied(Denied.getDefaultInstance())
                .build()));

        mockInboxCommit(CommitReply.Code.OK);
        mockDistUnMatch(true);
        inboxFetchConsumer.accept(fetch(5, 128, QoS.AT_MOST_ONCE));
        channel.runPendingTasks();
        for (int i = 0; i < 5; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertNull(message);
        }
        verifyEvent(QOS0_DROPPED, QOS0_DROPPED, QOS0_DROPPED, QOS0_DROPPED, QOS0_DROPPED);
        verify(inboxClient, times(5)).unsub(any());
    }

    @Test
    public void qoS0PubAndHintChange() {
        mockInboxCommit(CommitReply.Code.OK);
        mockCheckPermission(true);
        int messageCount = 2;
        inboxFetchConsumer.accept(fetch(messageCount, 64 * 1024, QoS.AT_MOST_ONCE));
        channel.runPendingTasks();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_MOST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), topic);
        }
        verifyEvent(QOS0_PUSHED, QOS0_PUSHED);
        assertEquals(fetchHints.size(), 1);
    }

    @Test
    public void qoS1PubAndAck() {
        mockInboxCommit(CommitReply.Code.OK);
        mockCheckPermission(true);
        int messageCount = 3;
        inboxFetchConsumer.accept(fetch(messageCount, 128, AT_LEAST_ONCE));
        channel.runPendingTasks();
        // s2c pub received and ack
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), topic);
            channel.writeInbound(MQTTMessageUtils.pubAckMessage(message.variableHeader().packetId()));
        }
        verifyEventUnordered(QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_CONFIRMED, QOS1_CONFIRMED, QOS1_CONFIRMED);
        verify(inboxClient, times(1)).commit(argThat(CommitRequest::hasSendBufferUpToSeq));
    }

    @Test
    public void qos1PubWithMultipleTopicFilters() {
        mockInboxCommit(CommitReply.Code.OK);
        mockCheckPermission(true);
        InboxMessage inboxMessage = InboxMessage.newBuilder()
            .setSeq(0)
            .putMatchedTopicFilter("#", TopicFilterOption.newBuilder().setQos(AT_LEAST_ONCE).build())
            .putMatchedTopicFilter("+", TopicFilterOption.newBuilder().setQos(AT_LEAST_ONCE).build())
            .setMsg(
                TopicMessage.newBuilder()
                    .setTopic(topic)
                    .setMessage(Message.newBuilder()
                        .setMessageId(0)
                        .setPayload(ByteString.copyFromUtf8("payload"))
                        .setTimestamp(HLC.INST.get())
                        .setExpiryInterval(120)
                        .setPubQoS(AT_LEAST_ONCE)
                        .build()
                    )
                    .setPublisher(
                        ClientInfo.newBuilder()
                            .setType(MQTT_TYPE_VALUE)
                            .build()
                    )
                    .build()
            ).build();
        inboxFetchConsumer.accept(Fetched.newBuilder().addSendBufferMsg(inboxMessage).build());
        channel.runPendingTasks();
        // s2c pub received and ack
        for (int i = 0; i < 2; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), topic);
            channel.writeInbound(MQTTMessageUtils.pubAckMessage(message.variableHeader().packetId()));
        }
        verifyEvent(QOS1_PUSHED, QOS1_PUSHED, QOS1_CONFIRMED, QOS1_CONFIRMED);
        verify(inboxClient, times(1)).commit(argThat(CommitRequest::hasSendBufferUpToSeq));
    }

    @Test
    public void qos1PubWithMultipleTopicFiltersWithPartialReply() {
        mockInboxCommit(CommitReply.Code.OK);
        mockCheckPermission(true);
        InboxMessage inboxMessage = InboxMessage.newBuilder()
            .setSeq(0)
            .putMatchedTopicFilter("#", TopicFilterOption.newBuilder().setQos(AT_LEAST_ONCE).build())
            .putMatchedTopicFilter("+", TopicFilterOption.newBuilder().setQos(AT_LEAST_ONCE).build())
            .setMsg(
                TopicMessage.newBuilder()
                    .setTopic(topic)
                    .setMessage(Message.newBuilder()
                        .setMessageId(0)
                        .setPayload(ByteString.copyFromUtf8("payload"))
                        .setTimestamp(HLC.INST.get())
                        .setExpiryInterval(120)
                        .setPubQoS(AT_LEAST_ONCE)
                        .build()
                    )
                    .setPublisher(
                        ClientInfo.newBuilder()
                            .setType(MQTT_TYPE_VALUE)
                            .build()
                    )
                    .build()
            ).build();
        inboxFetchConsumer.accept(Fetched.newBuilder().addSendBufferMsg(inboxMessage).build());
        channel.runPendingTasks();
        // s2c pub received and ack
        for (int i = 0; i < 2; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), topic);
        }
        channel.writeInbound(MQTTMessageUtils.pubAckMessage(1));
        verifyEvent(QOS1_PUSHED, QOS1_PUSHED, QOS1_CONFIRMED);
        verify(inboxClient, times(1)).commit(argThat(CommitRequest::hasSendBufferUpToSeq));
    }

    @Test
    public void qoS1PubAndNotAllAck() {
        mockCheckPermission(true);
        mockInboxCommit(CommitReply.Code.OK);
        int messageCount = 3;
        inboxFetchConsumer.accept(fetch(messageCount, 128, AT_LEAST_ONCE));
        channel.runPendingTasks();
        // s2c pub received and ack
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), topic);
            if (i != messageCount - 1) {
                channel.writeInbound(MQTTMessageUtils.pubAckMessage(message.variableHeader().packetId()));
            }
        }
        verifyEventUnordered(QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_CONFIRMED, QOS1_CONFIRMED);
        verify(inboxClient, times(0)).commit(argThat(CommitRequest::hasSendBufferUpToSeq));
    }

    @Test
    public void qoS1PubAuthFailed() {
        // not by pass
        when(authProvider.checkPermission(any(ClientInfo.class),
            argThat(action -> action.hasSub() && action.getSub().getQos() == QoS.AT_LEAST_ONCE)))
            .thenReturn(CompletableFuture.completedFuture(CheckResult.newBuilder()
                .setDenied(Denied.getDefaultInstance())
                .build()));
        mockDistUnMatch(true);
        when(inboxClient.unsub(any())).thenReturn(new CompletableFuture<>());
        int messageCount = 3;
        inboxFetchConsumer.accept(fetch(messageCount, 128, AT_LEAST_ONCE));
        channel.runPendingTasks();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertNull(message);
        }
        verifyEventUnordered(QOS1_DROPPED, QOS1_DROPPED, QOS1_DROPPED, QOS1_CONFIRMED, QOS1_CONFIRMED, QOS1_CONFIRMED);
        verify(eventCollector, times(6)).report(argThat(e -> {
            if (e instanceof QoS1Confirmed evt) {
                return !evt.delivered();
            }
            return true;
        }));
        verify(inboxClient, times(messageCount)).unsub(any());
    }

    @Test
    public void qoS2PubAndRel() {
        mockCheckPermission(true);
        mockInboxCommit(CommitReply.Code.OK);
        int messageCount = 2;
        inboxFetchConsumer.accept(fetch(messageCount, 128, EXACTLY_ONCE));
        channel.runPendingTasks();
        // s2c pub received and rec
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.EXACTLY_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), topic);
            channel.writeInbound(MQTTMessageUtils.publishRecMessage(message.variableHeader().packetId()));
        }
        // pubRel received and comp
        for (int i = 0; i < messageCount; i++) {
            MqttMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().messageType(), PUBREL);
            channel.writeInbound(MQTTMessageUtils.publishCompMessage(
                ((MqttMessageIdVariableHeader) message.variableHeader()).messageId()));
        }
        verifyEvent(QOS2_PUSHED, QOS2_PUSHED, QOS2_RECEIVED, QOS2_RECEIVED, QOS2_CONFIRMED, QOS2_CONFIRMED);
        verify(inboxClient, times(1)).commit(argThat(CommitRequest::hasSendBufferUpToSeq));
    }

    @Test
    public void qoS2PubAuthFailed() {
        // not by pass
        when(authProvider.checkPermission(any(ClientInfo.class),
            argThat(action -> action.hasSub() && action.getSub().getQos() == EXACTLY_ONCE)))
            .thenReturn(CompletableFuture.completedFuture(CheckResult.newBuilder()
                .setDenied(Denied.getDefaultInstance())
                .build()));
        mockDistUnMatch(true);
        when(inboxClient.unsub(any())).thenReturn(new CompletableFuture<>());
        int messageCount = 3;
        inboxFetchConsumer.accept(fetch(messageCount, 128, EXACTLY_ONCE));
        channel.runPendingTasks();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertNull(message);
        }
        verifyEventUnordered(QOS2_DROPPED, QOS2_DROPPED, QOS2_DROPPED, QOS2_CONFIRMED, QOS2_CONFIRMED, QOS2_CONFIRMED);
        verify(eventCollector, times(6)).report(argThat(e -> {
            if (e instanceof QoS2Confirmed evt) {
                return !evt.delivered();
            }
            return true;
        }));
        verify(inboxClient, times(messageCount)).unsub(any());
    }

    @Test
    public void fetchTryLater() {
        inboxFetchConsumer.accept(Fetched.newBuilder().setResult(Result.TRY_LATER).build());
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runPendingTasks();
        verifyEvent();
        assertTrue(channel.isOpen());
    }

    @Test
    public void fetchError() {
        inboxFetchConsumer.accept(Fetched.newBuilder().setResult(Result.ERROR).build());
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runPendingTasks();
        verifyEvent(INBOX_TRANSIENT_ERROR);
    }

    @Test
    public void fetchNoInbox() {
        inboxFetchConsumer.accept(Fetched.newBuilder().setResult(Result.NO_INBOX).build());
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runPendingTasks();
        verifyEvent(INBOX_TRANSIENT_ERROR);
    }

    @DataProvider
    public Object[][] confirmingQoS() {
        return new Object[][] {{AT_LEAST_ONCE}, {EXACTLY_ONCE}};
    }

    @Test(dataProvider = "confirmingQoS")
    public void outOfOrderAckKeepsEarlierInboxMessage(QoS qos) {
        mockCheckPermission(true);
        mockInboxCommit(CommitReply.Code.OK);
        inboxFetchConsumer.accept(fetch(2, 16, qos));
        channel.runPendingTasks();
        MqttPublishMessage first = channel.readOutbound();
        MqttPublishMessage second = channel.readOutbound();
        assertNotNull(first);
        assertNotNull(second);
        acknowledge(second.variableHeader().packetId(), qos);
        channel.runPendingTasks();
        verify(inboxClient, never()).commit(argThat(CommitRequest::hasSendBufferUpToSeq));
        acknowledge(first.variableHeader().packetId(), qos);
        channel.runPendingTasks();
        verify(inboxClient).commit(argThat(req -> req.hasSendBufferUpToSeq() && req.getSendBufferUpToSeq() == 1));
        first.release();
        second.release();
    }

    @Test(dataProvider = "confirmingQoS")
    public void oversizeMiddleWaitsForEarlierInboxMessage(QoS qos) {
        List<Event<?>> reported = new ArrayList<>();
        doAnswer(invocation -> {
            reported.add((Event<?>) ((Event<?>) invocation.getArgument(0)).clone());
            return null;
        }).when(eventCollector).report(any());
        mockCheckPermission(true);
        mockInboxCommit(CommitReply.Code.OK);
        channel.pipeline().addBefore(channel.pipeline().lastContext().name(), "packetFilter",
            new MQTTPacketFilter(70, new TenantSettings(tenantId, settingProvider), clientInfo, eventCollector));
        Fetched.Builder fetched = fetch(2, 16, qos).toBuilder();
        InboxMessage middle = fetched.getSendBufferMsg(1);
        fetched.setSendBufferMsg(1, middle.toBuilder().setMsg(middle.getMsg().toBuilder()
            .setMessage(middle.getMsg().getMessage().toBuilder().setPayload(ByteString.copyFrom(new byte[128])))));
        inboxFetchConsumer.accept(fetched.build());
        channel.runPendingTasks();
        inboxFetchConsumer.accept(Fetched.newBuilder()
            .addSendBufferMsg(fetch(1, 16, qos).getSendBufferMsg(0).toBuilder().setSeq(2)).build());
        channel.runPendingTasks();
        MqttPublishMessage first = channel.readOutbound();
        MqttPublishMessage last = channel.readOutbound();
        assertNotNull(first);
        assertNotNull(last);
        assertEquals(last.variableHeader().packetId(), first.variableHeader().packetId() + 2);
        assertNull(channel.readOutbound());
        verify(inboxClient, never()).commit(argThat(CommitRequest::hasSendBufferUpToSeq));
        testTicker.advanceTimeBy(11, TimeUnit.SECONDS);
        channel.advanceTimeBy(11, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.runPendingTasks();
        verify(eventCollector).report(argThat(e -> e instanceof OversizePacketDropped));
        verify(eventCollector, never()).report(argThat(e ->
            e instanceof QoS1Dropped || e instanceof QoS2Dropped));
        acknowledge(first.variableHeader().packetId(), qos);
        channel.runPendingTasks();
        verify(inboxClient).commit(argThat(req -> req.hasSendBufferUpToSeq() && req.getSendBufferUpToSeq() == 1));
        verify(inboxClient, never()).commit(argThat(req -> req.hasSendBufferUpToSeq() && req.getSendBufferUpToSeq() > 1));
        if (qos == AT_LEAST_ONCE) {
            assertEquals(reported.stream().filter(e -> e instanceof QoS1Confirmed c
                && c.messageId() == first.variableHeader().packetId() + 1 && !c.delivered()).count(), 1L);
        } else {
            assertEquals(reported.stream().filter(e -> e instanceof QoS2Confirmed c
                && c.messageId() == first.variableHeader().packetId() + 1 && !c.delivered()).count(), 1L);
        }
        acknowledge(last.variableHeader().packetId(), qos);
        channel.runPendingTasks();
        verify(inboxClient).commit(argThat(req -> req.hasSendBufferUpToSeq() && req.getSendBufferUpToSeq() == 2));
        assertEquals(reported.stream().filter(e ->
            (e instanceof QoS1Confirmed c && c.messageId() == first.variableHeader().packetId() + 1 && c.delivered())
                || (e instanceof QoS2Confirmed c2 && c2.messageId() == first.variableHeader().packetId() + 1
                && c2.delivered())).count(), 0L);
        first.release();
        last.release();
    }

    private void acknowledge(int packetId, QoS qos) {
        if (qos == AT_LEAST_ONCE) {
            channel.writeInbound(MQTTMessageUtils.pubAckMessage(packetId));
        } else {
            channel.writeInbound(MQTTMessageUtils.publishRecMessage(packetId));
            channel.writeInbound(MQTTMessageUtils.publishCompMessage(packetId));
        }
    }

}
