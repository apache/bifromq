/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.mqtt.handler.v3;


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
import static org.apache.bifromq.plugin.eventcollector.EventType.SUB_ACKED;
import static org.apache.bifromq.plugin.eventcollector.EventType.UNSUB_ACKED;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;
import static org.apache.bifromq.type.QoS.AT_LEAST_ONCE;
import static org.apache.bifromq.type.QoS.EXACTLY_ONCE;
import static io.netty.handler.codec.mqtt.MqttMessageType.PUBREL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.rpc.proto.CommitReply;
import org.apache.bifromq.inbox.rpc.proto.CommitRequest;
import org.apache.bifromq.inbox.rpc.proto.UnsubReply;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import org.apache.bifromq.inbox.storage.proto.Fetched.Result;
import org.apache.bifromq.inbox.storage.proto.InboxMessage;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.TopicFilterOption;
import org.apache.bifromq.mqtt.handler.BaseSessionHandlerTest;
import org.apache.bifromq.mqtt.handler.ChannelAttrs;
import org.apache.bifromq.mqtt.handler.TenantSettings;
import org.apache.bifromq.mqtt.utils.MQTTMessageUtils;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Denied;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Confirmed;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Confirmed;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicMessage;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
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
        verifyEvent(SUB_ACKED);
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
        verifyEvent(SUB_ACKED);
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
        verifyEvent(SUB_ACKED);
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
        verifyEvent(SUB_ACKED);
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
        verifyEvent(QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_CONFIRMED, QOS1_CONFIRMED, QOS1_CONFIRMED);
        verify(inboxClient, times(3)).commit(argThat(CommitRequest::hasSendBufferUpToSeq));
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
        verifyEvent(QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_CONFIRMED, QOS1_CONFIRMED);
        verify(inboxClient, times(2)).commit(argThat(CommitRequest::hasSendBufferUpToSeq));
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
        verifyEvent(QOS1_DROPPED, QOS1_DROPPED, QOS1_DROPPED, QOS1_CONFIRMED, QOS1_CONFIRMED);
        verify(eventCollector, times(5)).report(argThat(e -> {
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
        verify(inboxClient, times(2)).commit(argThat(CommitRequest::hasSendBufferUpToSeq));
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
        verifyEvent(QOS2_DROPPED, QOS2_DROPPED, QOS2_DROPPED, QOS2_CONFIRMED, QOS2_CONFIRMED);
        verify(eventCollector, times(5)).report(argThat(e -> {
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
}
