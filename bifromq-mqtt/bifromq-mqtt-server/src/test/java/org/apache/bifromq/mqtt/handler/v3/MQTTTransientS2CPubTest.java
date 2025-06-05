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


import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQTTTransientS2CPubTest extends BaseMQTTTest {

//    private MQTT3TransientSessionHandler transientSessionHandler;
//
//    @BeforeMethod
//    public void setup() {
//        super.setup();
//        setupTransientSession();
//        transientSessionHandler = (MQTT3TransientSessionHandler) channel.pipeline().get(MQTT3SessionHandler.NAME);
//    }
//
//    @AfterMethod
//    public void clean() {
//        channel.close();
//    }
//
//    @Test
//    public void qoS0Pub() {
//        mockAuthCheck(true);
//        mockDistMatch(QoS.AT_MOST_ONCE, true);
//        transientSessionHandler.doSub(System.nanoTime(), "testTopicFilter", MqttQoS.AT_MOST_ONCE);
//        channel.runPendingTasks();
//
//        transientSessionHandler.publish(subInfo("testTopicFilter", QoS.AT_MOST_ONCE),
//            s2cMessages("testTopic", 5, QoS.AT_MOST_ONCE));
//        channel.runPendingTasks();
//        for (int i = 0; i < 5; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_MOST_ONCE_VALUE);
//            assertEquals(message.variableHeader().topicName(), "testTopic");
//        }
//        verifyEvent(CLIENT_CONNECTED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED);
//    }
//
//    @Test
//    public void qoS0PubAuthFailed() {
//        // not by pass
//        mockAuthCheck(false);
//        mockDistUnmatch(true);
//        mockDistMatch(QoS.AT_MOST_ONCE, true);
//        transientSessionHandler.doSub(System.nanoTime(), "testTopic", MqttQoS.AT_MOST_ONCE);
//        channel.runPendingTasks();
//
//        transientSessionHandler.publish(subInfo("testTopic", QoS.AT_MOST_ONCE),
//            s2cMessages("testTopic", 5, QoS.AT_MOST_ONCE));
//        channel.runPendingTasks();
//        for (int i = 0; i < 5; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            assertNull(message);
//        }
//        verifyEvent(CLIENT_CONNECTED, QOS0_DROPPED, QOS0_DROPPED, QOS0_DROPPED, QOS0_DROPPED, QOS0_DROPPED);
//        verify(distClient, times(1))
//            .unmatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt());
//    }
//
//    @Test
//    public void qoS0PubExceedBufferCapacity() {
//        mockAuthCheck(true);
//        mockDistMatch(QoS.AT_MOST_ONCE, true);
//        transientSessionHandler.doSub(System.nanoTime(), "testTopic", MqttQoS.AT_MOST_ONCE);
//        channel.runPendingTasks();
//
//        List<ByteBuffer> payloads = s2cMessagesPayload(10, 32 * 1024);
//        transientSessionHandler.publish(subInfo("testTopic", QoS.AT_MOST_ONCE),
//            s2cMessages("testTopic", payloads, QoS.AT_MOST_ONCE));
//        channel.runPendingTasks();
//        // channel unWritable after 9 messages and lastHintRemaining is 0, then drop
//        for (int i = 0; i < 10; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            if (i < 9) {
//                assertEquals(message.variableHeader().topicName(), "testTopic");
//            } else {
//                assertNull(message);
//            }
//        }
//        verifyEvent(CLIENT_CONNECTED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED,
//            QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED, QOS0_DROPPED);
//    }
//
//
//    @Test
//    public void qoS1PubAndAck() {
//        mockAuthCheck(true);
//        mockDistMatch(QoS.AT_LEAST_ONCE, true);
//        transientSessionHandler.doSub(System.nanoTime(), "testTopic", MqttQoS.AT_LEAST_ONCE);
//        channel.runPendingTasks();
//
//        int messageCount = 3;
//        transientSessionHandler.publish(subInfo("testTopic", QoS.AT_LEAST_ONCE),
//            s2cMessages("testTopic", messageCount, QoS.AT_LEAST_ONCE));
//        channel.runPendingTasks();
//        // s2c pub received and ack
//        for (int i = 0; i < messageCount; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
//            assertEquals(message.variableHeader().topicName(), "testTopic");
//            channel.writeInbound(MQTTMessageUtils.pubAckMessage(message.variableHeader().packetId()));
//        }
//        verifyEvent(CLIENT_CONNECTED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_CONFIRMED, QOS1_CONFIRMED,
//            QOS1_CONFIRMED);
//    }
//
//    @Test
//    public void qoS1PubAuthFailed() {
//        // not by pass
//        mockAuthCheck(false);
//        mockDistUnmatch(true);
//        mockDistMatch(QoS.AT_MOST_ONCE, true);
//        transientSessionHandler.doSub(System.nanoTime(), "testTopic", MqttQoS.AT_MOST_ONCE);
//        channel.runPendingTasks();
//
//        transientSessionHandler.publish(subInfo("testTopic", QoS.AT_LEAST_ONCE),
//            s2cMessages("testTopic", 5, QoS.AT_LEAST_ONCE));
//        channel.runPendingTasks();
//        for (int i = 0; i < 5; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            assertNull(message);
//        }
//        verifyEvent(CLIENT_CONNECTED, QOS1_DROPPED, QOS1_DROPPED, QOS1_DROPPED, QOS1_DROPPED, QOS1_DROPPED);
//        verify(distClient, times(1))
//            .unmatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt());
//    }
//
//    @Test
//    public void qoS1PubExceedBufferCapacity() {
//        mockAuthCheck(true);
//        mockDistMatch(QoS.AT_LEAST_ONCE, true);
//        transientSessionHandler.doSub(System.nanoTime(), "testTopic", MqttQoS.AT_LEAST_ONCE);
//        channel.runPendingTasks();
//
//        List<ByteBuffer> payloads = s2cMessagesPayload(10, 32 * 1024);
//        transientSessionHandler.publish(subInfo("testTopic", QoS.AT_LEAST_ONCE),
//            s2cMessages("testTopic", payloads, QoS.AT_LEAST_ONCE));
//        channel.runPendingTasks();
//        // channel unWritable after 9 messages and lastHintRemaining is 0, then drop
//        for (int i = 0; i < 10; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            if (i < 9) {
//                assertEquals(message.variableHeader().topicName(), "testTopic");
//            } else {
//                assertNull(message);
//            }
//        }
//        verifyEvent(CLIENT_CONNECTED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED,
//            QOS1_PUSHED,
//            QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_DROPPED);
//    }
//
//    @Test
//    public void qoS1PubAndNoAck() {
//        mockAuthCheck(true);
//        mockDistMatch(QoS.AT_LEAST_ONCE, true);
//        transientSessionHandler.doSub(System.nanoTime(), "testTopic", MqttQoS.AT_LEAST_ONCE);
//        channel.runPendingTasks();
//
//        int messageCount = 3;
//        transientSessionHandler.publish(subInfo("testTopic", QoS.AT_LEAST_ONCE),
//            s2cMessages("testTopic", messageCount, QoS.AT_LEAST_ONCE));
//        channel.runPendingTasks();
//        // s2c pub received and not ack
//        List<Integer> packetIds = Lists.newArrayList();
//        for (int i = 0; i < messageCount; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
//            assertEquals(message.variableHeader().topicName(), "testTopic");
//            packetIds.add(message.variableHeader().packetId());
//        }
//        // resent once
//        testTicker.advanceTimeBy(100, TimeUnit.MILLISECONDS);
//        channel.advanceTimeBy(100, TimeUnit.MILLISECONDS);
//        channel.runPendingTasks();
//        for (int i = 0; i < messageCount; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
//            assertEquals(message.variableHeader().topicName(), "testTopic");
//            assertEquals(message.variableHeader().packetId(), (int) packetIds.get(i));
//        }
//        // resent twice
//        testTicker.advanceTimeBy(100, TimeUnit.MILLISECONDS);
//        channel.advanceTimeBy(100, TimeUnit.MILLISECONDS);
//        channel.runPendingTasks();
//        for (int i = 0; i < messageCount; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
//            assertEquals(message.variableHeader().topicName(), "testTopic");
//            assertEquals(message.variableHeader().packetId(), (int) packetIds.get(i));
//        }
//        // resent three times and remove
//        testTicker.advanceTimeBy(100, TimeUnit.MILLISECONDS);
//        channel.advanceTimeBy(100, TimeUnit.MILLISECONDS);
//        channel.runPendingTasks();
//        for (int i = 0; i < messageCount; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            assertNull(message);
//        }
//
//        verifyEvent(CLIENT_CONNECTED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED,
//            QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_CONFIRMED, QOS1_CONFIRMED, QOS1_CONFIRMED);
//    }
//
//    @Test
//    public void qoS1PubAndPacketIdOverflow() {
//        channel.freezeTime();
//        mockAuthCheck(true);
//        mockDistMatch(QoS.AT_LEAST_ONCE, true);
//        transientSessionHandler.doSub(System.nanoTime(), "testTopic", MqttQoS.AT_LEAST_ONCE);
//        channel.runPendingTasks();
//
//        int messageCount = 65535;
//        int overFlowCount = 10;
//        for (int i = 0; i < messageCount + overFlowCount; i++) {
//            transientSessionHandler.publish(subInfo("testTopic", QoS.AT_LEAST_ONCE),
//                s2cMessages("testTopic", 1, QoS.AT_LEAST_ONCE));
//            channel.runPendingTasks();
//        }
//        // s2c pub received and not ack
//        List<Integer> packetIds = Lists.newArrayList();
//        for (int i = 0; i < messageCount; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            if (message != null) {
//                assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
//                assertEquals(message.variableHeader().topicName(), "testTopic");
//                packetIds.add(message.variableHeader().packetId());
//            }
//        }
//        assertNotEquals(packetIds.size(), messageCount + overFlowCount);
//        assertNotEquals(packetIds.size(), messageCount + overFlowCount);
//
//        EventType[] eventTypes = new EventType[1 + messageCount + overFlowCount];
//        eventTypes[0] = CLIENT_CONNECTED;
//        for (int i = 1; i < 1 + messageCount; i++) {
//            eventTypes[i] = QOS1_PUSHED;
//        }
//        for (int i = 1 + messageCount; i < 1 + messageCount + overFlowCount; i++) {
//            eventTypes[i] = QOS1_CONFIRMED;
//        }
//        verifyEvent(eventTypes);
//    }
//
//    @Test
//    public void qoS2PubAndRel() {
//        mockAuthCheck(true);
//        mockDistMatch(QoS.EXACTLY_ONCE, true);
//        transientSessionHandler.doSub(System.nanoTime(), "testTopic", MqttQoS.EXACTLY_ONCE);
//        channel.runPendingTasks();
//
//        int messageCount = 1;
//        transientSessionHandler.publish(subInfo("testTopic", QoS.EXACTLY_ONCE),
//            s2cMessages("testTopic", messageCount, QoS.EXACTLY_ONCE));
//        channel.runPendingTasks();
//        // s2c pub received and rec
//        for (int i = 0; i < messageCount; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            assertEquals(message.fixedHeader().qosLevel().value(), QoS.EXACTLY_ONCE_VALUE);
//            assertEquals(message.variableHeader().topicName(), "testTopic");
//            channel.writeInbound(MQTTMessageUtils.publishRecMessage(message.variableHeader().packetId()));
//        }
//        // pubRel received and comp
//        for (int i = 0; i < messageCount; i++) {
//            MqttMessage message = channel.readOutbound();
//            assertEquals(message.fixedHeader().messageType(), PUBREL);
//            channel.writeInbound(MQTTMessageUtils.publishCompMessage(
//                ((MqttMessageIdVariableHeader) message.variableHeader()).messageId()));
//        }
//        verifyEvent(CLIENT_CONNECTED, QOS2_PUSHED, QOS2_RECEIVED, QOS2_CONFIRMED);
//    }
//
//    @Test
//    public void qoS2PubAuthFailed() {
//        // not by pass
//        mockAuthCheck(false);
//        mockDistUnmatch(true);
//        mockDistMatch(QoS.AT_MOST_ONCE, true);
//        transientSessionHandler.doSub(System.nanoTime(), "testTopic", MqttQoS.AT_MOST_ONCE);
//        channel.runPendingTasks();
//
//        transientSessionHandler.publish(subInfo("testTopic", QoS.EXACTLY_ONCE),
//            s2cMessages("testTopic", 5, QoS.EXACTLY_ONCE));
//        channel.runPendingTasks();
//        for (int i = 0; i < 5; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            assertNull(message);
//        }
//        verifyEvent(CLIENT_CONNECTED, QOS2_DROPPED, QOS2_DROPPED, QOS2_DROPPED, QOS2_DROPPED, QOS2_DROPPED);
//        verify(distClient, times(1))
//            .unmatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt());
//    }
//
//    @Test
//    public void qoS2PubExceedBufferCapacity() {
//        mockAuthCheck(true);
//        mockDistMatch(QoS.EXACTLY_ONCE, true);
//        transientSessionHandler.doSub(System.nanoTime(), "testTopic", MqttQoS.EXACTLY_ONCE);
//        channel.runPendingTasks();
//
//        List<ByteBuffer> payloads = s2cMessagesPayload(10, 32 * 1024);
//        transientSessionHandler.publish(subInfo("testTopic", QoS.EXACTLY_ONCE),
//            s2cMessages("testTopic", payloads, QoS.EXACTLY_ONCE));
//        channel.runPendingTasks();
//        // channel unWritable after 9 messages and lastHintRemaining is 0, then drop
//        for (int i = 0; i < 10; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            if (i < 9) {
//                assertEquals(message.variableHeader().topicName(), "testTopic");
//            } else {
//                assertNull(message);
//            }
//        }
//        verifyEvent(CLIENT_CONNECTED, QOS2_PUSHED, QOS2_PUSHED, QOS2_PUSHED, QOS2_PUSHED, QOS2_PUSHED, QOS2_PUSHED,
//            QOS2_PUSHED, QOS2_PUSHED, QOS2_PUSHED, QOS2_DROPPED);
//    }
//
//    @Test
//    public void qoS2PubAndNoRec() throws InterruptedException {
//        channel.unfreezeTime();
//        mockAuthCheck(true);
//        mockDistMatch(QoS.EXACTLY_ONCE, true);
//        transientSessionHandler.doSub(System.nanoTime(), "testTopic", MqttQoS.EXACTLY_ONCE);
//        channel.runPendingTasks();
//
//        int messageCount = 1;
//        transientSessionHandler.publish(subInfo("testTopic", QoS.EXACTLY_ONCE),
//            s2cMessages("testTopic", messageCount, QoS.EXACTLY_ONCE));
//        channel.runPendingTasks();
//        // s2c pub received and not ack
//        List<Integer> packetIds = Lists.newArrayList();
//        for (int i = 0; i < messageCount; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            assertEquals(message.fixedHeader().qosLevel().value(), QoS.EXACTLY_ONCE_VALUE);
//            assertEquals(message.variableHeader().topicName(), "testTopic");
//            packetIds.add(message.variableHeader().packetId());
//        }
//        // resent once
//        channel.advanceTimeBy(100, TimeUnit.MILLISECONDS);
//        testTicker.advanceTimeBy(100, TimeUnit.MILLISECONDS);
//        channel.runPendingTasks();
//        channel.flushOutbound();
//        for (int i = 0; i < messageCount; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            assertEquals(message.fixedHeader().qosLevel().value(), QoS.EXACTLY_ONCE_VALUE);
//            assertEquals(message.variableHeader().topicName(), "testTopic");
//            assertEquals(message.variableHeader().packetId(), (int) packetIds.get(i));
//        }
//        // resent twice
//        channel.advanceTimeBy(100, TimeUnit.MILLISECONDS);
//        testTicker.advanceTimeBy(100, TimeUnit.MILLISECONDS);
//        channel.runPendingTasks();
//        Thread.sleep(100);
//        channel.flushOutbound();
//        for (int i = 0; i < messageCount; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            assertEquals(message.fixedHeader().qosLevel().value(), QoS.EXACTLY_ONCE_VALUE);
//            assertEquals(message.variableHeader().topicName(), "testTopic");
//            assertEquals(message.variableHeader().packetId(), (int) packetIds.get(i));
//        }
//        // resent three times and remove
//        channel.advanceTimeBy(100, TimeUnit.MILLISECONDS);
//        testTicker.advanceTimeBy(100, TimeUnit.MILLISECONDS);
//        channel.runPendingTasks();
//        channel.flushOutbound();
//        for (int i = 0; i < messageCount; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            assertNull(message);
//        }
//        verifyEvent(CLIENT_CONNECTED, QOS2_PUSHED, QOS2_PUSHED, QOS2_PUSHED, QOS2_CONFIRMED);
//    }
//
//    @Test
//    public void qoS2PubAndPacketIdOverflow() {
//        channel.freezeTime();
//        mockAuthCheck(true);
//        mockDistMatch(QoS.EXACTLY_ONCE, true);
//        transientSessionHandler.doSub(System.nanoTime(), "testTopic", MqttQoS.EXACTLY_ONCE);
//        channel.runPendingTasks();
//
//        int messageCount = 65535;
//        int overFlowCount = 10;
//        List<TopicMessagePack> messages = s2cMessageList("testTopic", messageCount + overFlowCount, QoS.EXACTLY_ONCE);
//        for (int i = 0; i < messageCount + overFlowCount; i++) {
//            if (i > messageCount) {
//                transientSessionHandler.publish(subInfo("testTopic", QoS.EXACTLY_ONCE), messages.get(i));
//            } else {
//                transientSessionHandler.publish(subInfo("testTopic", QoS.EXACTLY_ONCE), messages.get(i));
//            }
//            channel.runPendingTasks();
//        }
//        // s2c pub received and not rec
//        List<Integer> packetIds = Lists.newArrayList();
//        for (int i = 0; i < messageCount; i++) {
//            MqttPublishMessage message = channel.readOutbound();
//            if (message != null) {
//                assertEquals(message.fixedHeader().qosLevel().value(), QoS.EXACTLY_ONCE_VALUE);
//                assertEquals(message.variableHeader().topicName(), "testTopic");
//                packetIds.add(message.variableHeader().packetId());
//            }
//        }
//        assertNotEquals(packetIds.size(), messageCount + overFlowCount);
//        EventType[] eventTypes = new EventType[1 + messageCount + overFlowCount];
//        eventTypes[0] = CLIENT_CONNECTED;
//        for (int i = 1; i < 1 + messageCount; i++) {
//            eventTypes[i] = QOS2_PUSHED;
//        }
//        for (int i = 1 + messageCount; i < 1 + messageCount + overFlowCount; i++) {
//            eventTypes[i] = QOS2_CONFIRMED;
//        }
//        verifyEvent(eventTypes);
//    }
//
//    @Test
//    public void qoS2PubWithSameSourcePacketId() {
//        mockAuthCheck(true);
//        mockDistMatch(QoS.EXACTLY_ONCE, true);
//        transientSessionHandler.doSub(System.nanoTime(), "#", MqttQoS.EXACTLY_ONCE);
//        channel.runPendingTasks();
//
//        int messageCount = 2;
//        List<ByteBuffer> payloads = s2cMessagesPayload(messageCount, 1);
//        List<TopicMessagePack.PublisherPack> messagesFromClient1 = Lists.newArrayList();
//        List<TopicMessagePack.PublisherPack> messagesFromClient2 = Lists.newArrayList();
//        for (ByteBuffer payload : payloads) {
//            // messages with duplicated messageId
//            messagesFromClient1.add(TopicMessagePack.PublisherPack.newBuilder()
//                .setPublisher(ClientInfo.newBuilder()
//                    .setTenantId(tenantId)
//                    .setType(MQTT_TYPE_VALUE)
//                    .putMetadata(MQTT_PROTOCOL_VER_KEY, "3.1.1")
//                    .putMetadata(MQTT_USER_ID_KEY, "testUser")
//                    .putMetadata(MQTT_CHANNEL_ID_KEY, "client1")
//                    .putMetadata(MQTT_CLIENT_ID_KEY, "channel1")
//                    .putMetadata(MQTT_CLIENT_ADDRESS_KEY, "127.0.0.1:11111")
//                    .build()
//                )
//                .addMessage(Message.newBuilder()
//                    .setMessageId(1)
//                    .setPayload(ByteString.copyFrom(payload.duplicate()))
//                    .setTimestamp(System.currentTimeMillis())
//                    .setPubQoS(QoS.EXACTLY_ONCE)
//                    .build())
//                .build());
//            messagesFromClient2.add(TopicMessagePack.PublisherPack.newBuilder()
//                .setPublisher(ClientInfo.newBuilder()
//                    .setTenantId(tenantId)
//                    .setType(MQTT_TYPE_VALUE)
//                    .putMetadata(MQTT_PROTOCOL_VER_KEY, "3.1.1")
//                    .putMetadata(MQTT_USER_ID_KEY, "testUser")
//                    .putMetadata(MQTT_CHANNEL_ID_KEY, "client2")
//                    .putMetadata(MQTT_CLIENT_ID_KEY, "channel2")
//                    .putMetadata(MQTT_CLIENT_ADDRESS_KEY, "127.0.0.1:22222")
//                    .build()
//                )
//                .addMessage(Message.newBuilder()
//                    .setMessageId(1)
//                    .setPayload(ByteString.copyFrom(payload.duplicate()))
//                    .setTimestamp(System.currentTimeMillis())
//                    .setPubQoS(QoS.EXACTLY_ONCE)
//                    .build())
//                .build()
//            );
//        }
//        transientSessionHandler.publish(subInfo("#", QoS.EXACTLY_ONCE), TopicMessagePack.newBuilder()
//            .setTopic("testTopic1")
//            .addAllMessage(messagesFromClient1)
//            .build());
//        transientSessionHandler.publish(subInfo("#", QoS.EXACTLY_ONCE), TopicMessagePack.newBuilder()
//            .setTopic("testTopic2")
//            .addAllMessage(messagesFromClient2)
//            .build());
//        channel.runPendingTasks();
//        // should two messages from client1 and client2
//        MqttPublishMessage message = channel.readOutbound();
//        assertEquals(message.fixedHeader().qosLevel().value(), QoS.EXACTLY_ONCE_VALUE);
//        assertEquals(message.variableHeader().topicName(), "testTopic1");
//
//        message = channel.readOutbound();
//        assertEquals(message.fixedHeader().qosLevel().value(), QoS.EXACTLY_ONCE_VALUE);
//        assertEquals(message.variableHeader().topicName(), "testTopic2");
//
//        message = channel.readOutbound();
//        assertNull(message);
//
//        verifyEvent(CLIENT_CONNECTED, QOS2_PUSHED, QOS2_PUSHED);
//    }
//
//
//    private SubInfo subInfo(String topicFilter, QoS qoS) {
//        return SubInfo.newBuilder()
//            .setTopicFilter(topicFilter)
//            .setInboxId("testInboxId")
//            .setTenantId(tenantId)
//            .setSubQoS(qoS)
//            .build();
//    }
//
//    private List<TopicMessagePack> s2cMessageList(String topic, int count, QoS qos) {
//        List<TopicMessagePack> topicMessagePacks = new ArrayList<>();
//        for (int i = 0; i < count; i++) {
//            topicMessagePacks.add(TopicMessagePack.newBuilder()
//                .setTopic(topic)
//                .addMessage(TopicMessagePack.PublisherPack.newBuilder()
//                    .setPublisher(ClientInfo.newBuilder().build())
//                    .addMessage(Message.newBuilder()
//                        .setMessageId(i)
//                        .setPayload(ByteString.EMPTY)
//                        .setTimestamp(System.currentTimeMillis())
//                        .setPubQoS(qos)
//                        .build()))
//                .build());
//        }
//        return topicMessagePacks;
//    }
//
//    private TopicMessagePack s2cMessages(String topic, int count, QoS qoS) {
//        return s2cMessages(topic, s2cMessagesPayload(count, 128), qoS);
//    }
//
//    private TopicMessagePack s2cMessages(String topic, List<ByteBuffer> payloads, QoS qoS) {
//        TopicMessagePack.Builder topicMsgPackBuilder = TopicMessagePack.newBuilder()
//            .setTopic(topic);
//        for (int i = 0; i < payloads.size(); i++) {
//            topicMsgPackBuilder
//                .addMessage(TopicMessagePack.PublisherPack.newBuilder()
//                    .setPublisher(ClientInfo.newBuilder().build())
//                    .addMessage(Message.newBuilder()
//                        .setMessageId(i)
//                        .setPayload(ByteString.copyFrom(payloads.get(i).duplicate()))
//                        .setTimestamp(System.currentTimeMillis())
//                        .setPubQoS(qoS)
//                        .build()))
//                .build();
//        }
//        return topicMsgPackBuilder.build();
//    }
//
//    private List<ByteBuffer> s2cMessagesPayload(int count, int size) {
//        List<ByteBuffer> list = Lists.newArrayList();
//        for (int i = 0; i < count; i++) {
//            byte[] bytes = new byte[size];
//            Arrays.fill(bytes, (byte) 1);
//            list.add(ByteBuffer.wrap(bytes));
//        }
//        return list;
//    }
}
