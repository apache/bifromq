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


import static org.apache.bifromq.plugin.eventcollector.EventType.CLIENT_CONNECTED;
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

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.rpc.proto.CommitReply;
import org.apache.bifromq.inbox.rpc.proto.CommitRequest;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import org.apache.bifromq.inbox.storage.proto.Fetched.Builder;
import org.apache.bifromq.inbox.storage.proto.Fetched.Result;
import org.apache.bifromq.inbox.storage.proto.InboxMessage;
import org.apache.bifromq.inbox.storage.proto.TopicFilterOption;
import org.apache.bifromq.mqtt.utils.MQTTMessageUtils;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Confirmed;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicMessage;
import com.google.protobuf.ByteString;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class MQTTPersistentS2CPubTest extends BaseMQTTTest {

    @BeforeMethod
    public void setup() {
        super.setup();
        setupPersistentSession();
    }

    @AfterMethod
    public void clean() throws Exception {
        super.clean();
    }

    @Test
    public void qoS0Pub() {
        mockInboxCommit(CommitReply.Code.OK);
        mockAuthCheck(true);
        inboxFetchConsumer.accept(fetch(5, 128, QoS.AT_MOST_ONCE));
        channel.runPendingTasks();
        for (int i = 0; i < 5; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_MOST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), "testTopic");
        }
        verifyEvent(CLIENT_CONNECTED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED);
        assertEquals(fetchHints.size(), 1);
    }

    @Test
    public void qoS0PubAuthFailed() {
        // not by pass
        mockInboxCommit(CommitReply.Code.OK);
        mockAuthCheck(false);
        mockDistUnmatch(true);
        inboxFetchConsumer.accept(fetch(5, 128, QoS.AT_MOST_ONCE));
        channel.runPendingTasks();
        for (int i = 0; i < 5; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertNull(message);
        }
        verifyEvent(CLIENT_CONNECTED, QOS0_DROPPED, QOS0_DROPPED, QOS0_DROPPED, QOS0_DROPPED, QOS0_DROPPED);
        verify(inboxClient, times(5)).unsub(any());
    }

    @Test
    public void qoS0PubAndHintChange() {
        mockInboxCommit(CommitReply.Code.OK);
        mockAuthCheck(true);
        int messageCount = 2;
        inboxFetchConsumer.accept(fetch(messageCount, 64 * 1024, QoS.AT_MOST_ONCE));
        channel.runPendingTasks();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_MOST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), "testTopic");
        }
        verifyEvent(CLIENT_CONNECTED, QOS0_PUSHED, QOS0_PUSHED);
        assertEquals(fetchHints.size(), 1);
    }

    @Test
    public void qoS1PubAndAck() {
        mockInboxCommit(CommitReply.Code.OK);
        mockAuthCheck(true);
        int messageCount = 3;
        inboxFetchConsumer.accept(fetch(messageCount, 128, AT_LEAST_ONCE));
        channel.runPendingTasks();
        // s2c pub received and ack
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), "testTopic");
            channel.writeInbound(MQTTMessageUtils.pubAckMessage(message.variableHeader().packetId()));
        }
        verifyEvent(CLIENT_CONNECTED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_CONFIRMED, QOS1_CONFIRMED,
            QOS1_CONFIRMED);
        verify(inboxClient, times(3)).commit(argThat(CommitRequest::hasSendBufferUpToSeq));
    }

    @Test
    public void qoS1PubAndNotAllAck() {
        mockAuthCheck(true);
        mockInboxCommit(CommitReply.Code.OK);
        int messageCount = 3;
        inboxFetchConsumer.accept(fetch(messageCount, 128, AT_LEAST_ONCE));
        channel.runPendingTasks();
        // s2c pub received and ack
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), "testTopic");
            if (i != messageCount - 1) {
                channel.writeInbound(MQTTMessageUtils.pubAckMessage(message.variableHeader().packetId()));
            }
        }
        verifyEvent(CLIENT_CONNECTED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_CONFIRMED, QOS1_CONFIRMED);
        verify(inboxClient, times(2)).commit(argThat(CommitRequest::hasSendBufferUpToSeq));
    }

    @Test
    public void qoS1PubAuthFailed() {
        // not by pass
        mockAuthCheck(false);
        mockDistUnmatch(true);
        when(inboxClient.commit(any())).thenReturn(new CompletableFuture<>());
        when(inboxClient.unsub(any())).thenReturn(new CompletableFuture<>());
        int messageCount = 3;
        inboxFetchConsumer.accept(fetch(messageCount, 128, AT_LEAST_ONCE));
        channel.runPendingTasks();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertNull(message);
        }
        channel.runPendingTasks();
        verifyEvent(CLIENT_CONNECTED, QOS1_DROPPED, QOS1_DROPPED, QOS1_DROPPED,
            QOS1_CONFIRMED, QOS1_CONFIRMED, QOS1_CONFIRMED);
        verify(eventCollector, times(7)).report(argThat(e -> {
            if (e instanceof QoS1Confirmed evt) {
                return !evt.delivered();
            }
            return true;
        }));
        verify(inboxClient, times(messageCount)).unsub(any());
    }

    @Test
    public void qoS2PubAndRel() {
        mockAuthCheck(true);
        mockInboxCommit(CommitReply.Code.OK);
        int messageCount = 2;
        inboxFetchConsumer.accept(fetch(messageCount, 128, EXACTLY_ONCE));
        channel.runPendingTasks();
        // s2c pub received and rec
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.EXACTLY_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), "testTopic");
            channel.writeInbound(MQTTMessageUtils.publishRecMessage(message.variableHeader().packetId()));
        }
        // pubRel received and comp
        for (int i = 0; i < messageCount; i++) {
            MqttMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().messageType(), PUBREL);
            channel.writeInbound(MQTTMessageUtils.publishCompMessage(
                ((MqttMessageIdVariableHeader) message.variableHeader()).messageId()));
        }
        verifyEvent(CLIENT_CONNECTED, QOS2_PUSHED, QOS2_PUSHED, QOS2_RECEIVED, QOS2_RECEIVED, QOS2_CONFIRMED,
            QOS2_CONFIRMED);
        verify(inboxClient, times(2)).commit(argThat(CommitRequest::hasSendBufferUpToSeq));
    }

    @Test
    public void qoS2PubAuthFailed() {
        // not by pass
        mockAuthCheck(false);
        mockDistUnmatch(true);
        when(inboxClient.commit(any())).thenReturn(new CompletableFuture<>());
        when(inboxClient.unsub(any())).thenReturn(new CompletableFuture<>());
        int messageCount = 3;
        inboxFetchConsumer.accept(fetch(messageCount, 128, EXACTLY_ONCE));
        channel.runPendingTasks();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertNull(message);
        }
        verifyEvent(CLIENT_CONNECTED, QOS2_DROPPED, QOS2_DROPPED, QOS2_DROPPED,
            QOS2_CONFIRMED, QOS2_CONFIRMED, QOS2_CONFIRMED);
        verify(eventCollector, times(7)).report(argThat(e -> {
            if (e instanceof QoS1Confirmed evt) {
                return !evt.delivered();
            }
            return true;
        }));
        verify(inboxClient, times(messageCount)).unsub(any());
    }

    @Test
    public void fetchTryLater() {
        inboxFetchConsumer.accept(Fetched.newBuilder().setResult(Result.TRY_LATER).build());
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        verifyEvent(CLIENT_CONNECTED);
    }

    @Test
    public void fetchError() {
        inboxFetchConsumer.accept(Fetched.newBuilder().setResult(Result.ERROR).build());
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        verifyEvent(CLIENT_CONNECTED, INBOX_TRANSIENT_ERROR);
    }

    @Test
    public void fetchNoInbox() {
        inboxFetchConsumer.accept(Fetched.newBuilder().setResult(Result.NO_INBOX).build());
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        verifyEvent(CLIENT_CONNECTED, INBOX_TRANSIENT_ERROR);
    }


    private Fetched fetch(int count, int payloadSize, QoS qoS) {
        Builder builder = Fetched.newBuilder();
        byte[] bytes = new byte[payloadSize];
        Arrays.fill(bytes, (byte) 1);
        for (int i = 0; i < count; i++) {
            InboxMessage inboxMessage = InboxMessage.newBuilder()
                .setSeq(i)
                .putMatchedTopicFilter("testTopicFilter", TopicFilterOption.newBuilder().setQos(qoS).build())
                .setMsg(
                    TopicMessage.newBuilder()
                        .setTopic("testTopic")
                        .setMessage(
                            Message.newBuilder()
                                .setMessageId(i)
                                .setPayload(ByteString.copyFrom(bytes))
                                .setTimestamp(HLC.INST.get())
                                .setPubQoS(qoS)
                                .build()
                        )
                        .setPublisher(
                            ClientInfo.newBuilder()
                                .setType(MQTT_TYPE_VALUE)
                                .build()
                        )
                        .build()
                ).build();
            switch (qoS) {
                case AT_MOST_ONCE -> builder.addQos0Msg(inboxMessage);
                case AT_LEAST_ONCE, EXACTLY_ONCE -> builder.addSendBufferMsg(inboxMessage);
            }
        }
        return builder.build();
    }
}
