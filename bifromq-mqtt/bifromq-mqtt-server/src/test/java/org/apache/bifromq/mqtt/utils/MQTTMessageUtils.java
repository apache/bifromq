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

package org.apache.bifromq.mqtt.utils;

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.RESPONSE_TOPIC;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.TOPIC_ALIAS;

import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5PubRecReasonCode;
import org.apache.bifromq.type.QoS;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttIdentifierRejectedException;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttProperties.IntegerProperty;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnacceptableProtocolVersionException;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribePayload;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class MQTTMessageUtils {

    static int MSG_ID = 1;

    public static MqttMessage pingMessage() {
        return new MqttMessage(new MqttFixedHeader(MqttMessageType.PINGREQ, false, MqttQoS.AT_MOST_ONCE, false, 0));
    }

    public static MqttConnectMessage connectMessageWithBadClientId() {
        return connectMessage("sdsdf$1231&", "willTopic", null);
    }

    public static MqttConnectMessage connectMessage(String clientId, String willTopic, String username) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader mqttConnectVariableHeader =
            new MqttConnectVariableHeader("MQTT", (byte) 4, username != null, false, false, 1, willTopic != null, true,
                30);
        MqttConnectPayload mqttConnectPayload =
            new MqttConnectPayload(clientId, willTopic, new byte[] {}, username, new byte[] {});
        return new MqttConnectMessage(mqttFixedHeader, mqttConnectVariableHeader, mqttConnectPayload);
    }


    public static MqttMessage connectMessageWithMqttIdentifierRejected() {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader mqttConnectVariableHeader =
            new MqttConnectVariableHeader("MQTT", (byte) 3, false, false, false, 1, true, true, 30);
        MqttConnectPayload mqttConnectPayload =
            new MqttConnectPayload("aaaaaaaaaaaaaaaaaaaaaaaa", "willTopic", new byte[] {}, null, new byte[] {});
        return new MqttMessage(mqttFixedHeader, mqttConnectVariableHeader, mqttConnectPayload,
            DecoderResult.failure(new MqttIdentifierRejectedException()));
    }

    public static MqttMessage failedToDecodeMessage() {
        return new MqttMessage(null, null, null, DecoderResult.failure(new DecoderException()));
    }

    public static MqttMessage connectMessageWithMqttUnacceptableProtocolVersion() {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader mqttConnectVariableHeader =
            new MqttConnectVariableHeader("MQTT", (byte) 5, false, false, false, 1, true, true, 30);
        MqttConnectPayload mqttConnectPayload =
            new MqttConnectPayload("aaaaaaaaaaaaaaaaaaaaaaaa", "willTopic", new byte[] {}, null, new byte[] {});
        return new MqttMessage(mqttFixedHeader, mqttConnectVariableHeader, mqttConnectPayload,
            DecoderResult.failure(new MqttUnacceptableProtocolVersionException()));
    }

    public static MqttConnectMessage badWillTopicMqttConnectMessage() {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader mqttConnectVariableHeader =
            new MqttConnectVariableHeader("MQTT", (byte) 3, false, false, false, 1, true, false, 30);
        MqttConnectPayload mqttConnectPayload =
            new MqttConnectPayload("testClientId", "willTopic/#", new byte[] {}, null, new byte[] {});
        return new MqttConnectMessage(mqttFixedHeader, mqttConnectVariableHeader, mqttConnectPayload);
    }

    public static MqttConnectMessage mqttConnectMessage(boolean cleanSession) {
        return mqttConnectMessage(cleanSession, "testClientId", 30);
    }

    public static MqttConnectMessage mqttConnectMessage(boolean cleanSession, String clientId, int keepAliveInSeconds) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader mqttConnectVariableHeader =
            new MqttConnectVariableHeader("MQTT", (byte) 4, true, false, false, 1, false, cleanSession,
                keepAliveInSeconds);
        MqttConnectPayload mqttConnectPayload =
            new MqttConnectPayload(clientId, "", new byte[] {}, "EndpointTest/TestUser", new byte[] {});
        return new MqttConnectMessage(mqttFixedHeader, mqttConnectVariableHeader, mqttConnectPayload);
    }

    public static MqttConnectMessage qoSWillMqttConnectMessage(int willQoS, boolean cleanSession) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader mqttConnectVariableHeader =
            new MqttConnectVariableHeader("MQTT", (byte) 4, true, false, false, willQoS, true, cleanSession,
                willQoS == 0 ? 0 : 30);
        MqttConnectPayload mqttConnectPayload =
            new MqttConnectPayload("testClientId", "willTopic", new byte[] {}, "EndpointTest/TestUser", new byte[] {});
        return new MqttConnectMessage(mqttFixedHeader, mqttConnectVariableHeader, mqttConnectPayload);
    }

    public static MqttConnectMessage willRetainMqttConnectMessage(int willQoS, boolean cleanSession) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader mqttConnectVariableHeader =
            new MqttConnectVariableHeader("MQTT", (byte) 4, true, false, true, willQoS, true, cleanSession, 30);
        MqttConnectPayload mqttConnectPayload =
            new MqttConnectPayload("testClientId", "willTopic", new byte[] {}, "EndpointTest/TestUser", new byte[] {});
        return new MqttConnectMessage(mqttFixedHeader, mqttConnectVariableHeader, mqttConnectPayload);
    }


    public static MqttConnectMessage disconnectMessage() {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader mqttConnectVariableHeader =
            new MqttConnectVariableHeader("MQTT", (byte) 2, false, false, false, 1, false, false, 30);
        MqttConnectPayload mqttConnectPayload =
            new MqttConnectPayload("testClientId", null, new byte[] {}, null, new byte[] {});
        return new MqttConnectMessage(mqttFixedHeader, mqttConnectVariableHeader, mqttConnectPayload);
    }

    public static MqttPublishMessage publishQoS0DupMessage(String topicName, int packetId) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.PUBLISH, true, MqttQoS.AT_MOST_ONCE, false, 0);

        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topicName, packetId);

        ByteBuf buf = Unpooled.wrappedBuffer("test_pub_payload".getBytes());

        return new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, buf);
    }

    public static MqttPublishMessage publishMQTT5QoS0Message(String topicName, int packetId, int alias) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, false, 0);

        MqttProperties mqttProperties = new MqttProperties();
        mqttProperties.add(new IntegerProperty(TOPIC_ALIAS.value(), alias));
        MqttPublishVariableHeader mqttPublishVariableHeader =
            new MqttPublishVariableHeader(topicName, packetId, mqttProperties);

        ByteBuf buf = Unpooled.wrappedBuffer("test_pub_payload".getBytes());

        return new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, buf);
    }

    public static MqttPublishMessage publishQoS0Message(String topicName, int packetId) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, false, 0);

        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topicName, packetId);

        ByteBuf buf = Unpooled.wrappedBuffer("test_pub_payload".getBytes());

        return new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, buf);
    }

    public static MqttPublishMessage publishQoS0Message(String topicName, int packetId, String responseTopic) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, false, 0);

        MqttProperties mqttProperties = new MqttProperties();
        mqttProperties.add(new MqttProperties.StringProperty(RESPONSE_TOPIC.value(), responseTopic));
        MqttPublishVariableHeader mqttPublishVariableHeader =
            new MqttPublishVariableHeader(topicName, packetId, mqttProperties);

        ByteBuf buf = Unpooled.wrappedBuffer("test_pub_payload".getBytes());

        return new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, buf);
    }

    public static MqttPublishMessage publishMQTT5QoS1Message(String topicName, int packetId, int alias) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0);

        MqttProperties mqttProperties = new MqttProperties();
        mqttProperties.add(new IntegerProperty(TOPIC_ALIAS.value(), alias));
        MqttPublishVariableHeader mqttPublishVariableHeader =
            new MqttPublishVariableHeader(topicName, packetId, mqttProperties);

        ByteBuf buf = Unpooled.wrappedBuffer("test_pub_payload".getBytes());

        return new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, buf);
    }

    public static MqttPublishMessage publishQoS1Message(String topicName, int packetId) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0);

        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topicName, packetId);

        ByteBuf buf = Unpooled.wrappedBuffer("test_pub_payload".getBytes());

        return new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, buf);
    }

    public static MqttPublishMessage publishQoS1Message(String topicName, int packetId, String responseTopic) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0);

        MqttProperties mqttProperties = new MqttProperties();
        mqttProperties.add(new MqttProperties.StringProperty(RESPONSE_TOPIC.value(), responseTopic));
        MqttPublishVariableHeader mqttPublishVariableHeader =
            new MqttPublishVariableHeader(topicName, packetId, mqttProperties);

        ByteBuf buf = Unpooled.wrappedBuffer("test_pub_payload".getBytes());

        return new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, buf);
    }

    public static MqttPublishMessage publishQoS2Message(String topicName, int packetId) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.EXACTLY_ONCE, false, 0);

        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topicName, packetId);

        ByteBuf buf = Unpooled.wrappedBuffer("test_pub_payload".getBytes());

        return new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, buf);
    }

    public static MqttPublishMessage publishQoS2Message(String topicName, int packetId, String responseTopic) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.EXACTLY_ONCE, false, 0);

        MqttProperties mqttProperties = new MqttProperties();
        mqttProperties.add(new MqttProperties.StringProperty(RESPONSE_TOPIC.value(), responseTopic));
        MqttPublishVariableHeader mqttPublishVariableHeader =
            new MqttPublishVariableHeader(topicName, packetId, mqttProperties);

        ByteBuf buf = Unpooled.wrappedBuffer("test_pub_payload".getBytes());

        return new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, buf);
    }


    public static MqttMessage publishRelMessage(int packetId) {
        return MqttMessageFactory.newMessage(new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE,
            // according to [MQTT-3.6.1-1]
            false, 2), MqttMessageIdVariableHeader.from(packetId), null);
    }

    public static MqttMessage publishRecMessage(int packetId) {
        return MqttMessageFactory.newMessage(new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_LEAST_ONCE,
            // according to [MQTT-3.6.1-1]
            false, 2), MqttMessageIdVariableHeader.from(packetId), null);
    }

    public static MqttMessage publishMQTT5RecMessage(int packetId) {
        return MqttMessageFactory.newMessage(new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_LEAST_ONCE,
                // according to [MQTT-3.6.1-1]
                false, 2), new MqttPubReplyMessageVariableHeader(packetId, MQTT5PubRecReasonCode.Success.value(), null),
            null);
    }

    public static MqttMessage publishCompMessage(int packetId) {
        return MqttMessageFactory.newMessage(new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_LEAST_ONCE,
            // according to [MQTT-3.6.1-1]
            false, 2), MqttMessageIdVariableHeader.from(packetId), null);
    }

    public static MqttPublishMessage publishRetainQoS0Message(String topicName, int packetId) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, true, 0);

        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topicName, packetId);

        ByteBuf buf = Unpooled.wrappedBuffer("test_pub_payload".getBytes());

        return new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, buf);
    }

    public static MqttPublishMessage publishRetainQoS1Message(String topicName, int packetId) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, true, 0);

        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topicName, packetId);

        ByteBuf buf = Unpooled.wrappedBuffer("test_pub_payload".getBytes());

        return new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, buf);
    }

    public static MqttPublishMessage publishRetainQoS2Message(String topicName, int packetId) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.EXACTLY_ONCE, true, 0);

        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topicName, packetId);

        ByteBuf buf = Unpooled.wrappedBuffer("test_pub_payload".getBytes());

        return new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, buf);
    }

    public static MqttSubscribeMessage subscribeMessageWithWildCard() {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.valueOf(0), false, 0);
        MqttMessageIdVariableHeader mqttConnectVariableHeader = MqttMessageIdVariableHeader.from(MSG_ID);

        List<MqttTopicSubscription> topicList = new ArrayList<MqttTopicSubscription>() {{
            add(new MqttTopicSubscription("testTopic/#", MqttQoS.valueOf(0)));
            add(new MqttTopicSubscription("testTopic/+/status", MqttQoS.valueOf(1)));
        }};
        MqttSubscribePayload mqttSubPayload = new MqttSubscribePayload(topicList);
        return new MqttSubscribeMessage(mqttFixedHeader, mqttConnectVariableHeader, mqttSubPayload);
    }

    public static MqttPubAckMessage pubAckMessage(int messageId) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_LEAST_ONCE, false, 0);

        MqttMessageIdVariableHeader mqttPubAckVariableHeadr = MqttMessageIdVariableHeader.from(messageId);

        return new MqttPubAckMessage(mqttFixedHeader, mqttPubAckVariableHeadr);
    }

    public static MqttSubscribeMessage badQoS0MqttSubMessageWithoutTopic() {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader mqttSubVariableHeader = MqttMessageIdVariableHeader.from(MSG_ID);

        MqttSubscribePayload mqttSubPayload = new MqttSubscribePayload(new ArrayList<>());
        return new MqttSubscribeMessage(mqttFixedHeader, mqttSubVariableHeader, mqttSubPayload);
    }

    public static MqttSubscribeMessage qoSMqttSubMessages(String[] tfs, int[] qos) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.valueOf(QoS.AT_MOST_ONCE_VALUE), false, 0);
        MqttMessageIdVariableHeader mqttConnectVariableHeader = MqttMessageIdVariableHeader.from(MSG_ID);

        List<MqttTopicSubscription> topicList = new ArrayList<>();
        IntStream.range(0, qos.length)
            .forEach(i -> topicList.add(new MqttTopicSubscription(tfs[i], MqttQoS.valueOf(qos[i]))));
        MqttSubscribePayload mqttSubPayload = new MqttSubscribePayload(topicList);
        return new MqttSubscribeMessage(mqttFixedHeader, mqttConnectVariableHeader, mqttSubPayload);
    }

    public static MqttSubscribeMessage qoSMqttSubMessages(int[] qos) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.valueOf(QoS.AT_MOST_ONCE_VALUE), false, 0);
        MqttMessageIdVariableHeader mqttConnectVariableHeader = MqttMessageIdVariableHeader.from(MSG_ID);

        List<MqttTopicSubscription> topicList = new ArrayList<>();
        IntStream.range(0, qos.length)
            .forEach(i -> topicList.add(new MqttTopicSubscription("testTopic" + i, MqttQoS.valueOf(qos[i]))));
        MqttSubscribePayload mqttSubPayload = new MqttSubscribePayload(topicList);
        return new MqttSubscribeMessage(mqttFixedHeader, mqttConnectVariableHeader, mqttSubPayload);
    }

    public static MqttSubscribeMessage badTopicMqttSubMessages() {
        return topicMqttSubMessages("testTopic/#/1");
    }

    public static MqttSubscribeMessage topicMqttSubMessages(String topicFilter) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.valueOf(QoS.AT_MOST_ONCE_VALUE), false, 0);
        MqttMessageIdVariableHeader mqttConnectVariableHeader = MqttMessageIdVariableHeader.from(MSG_ID);

        List<MqttTopicSubscription> topicList = new ArrayList<>();
        topicList.add(new MqttTopicSubscription(topicFilter, MqttQoS.AT_MOST_ONCE));
        MqttSubscribePayload mqttSubPayload = new MqttSubscribePayload(topicList);
        return new MqttSubscribeMessage(mqttFixedHeader, mqttConnectVariableHeader, mqttSubPayload);
    }


    public static MqttUnsubscribeMessage qoSMqttUnSubMessages(int count) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.UNSUBSCRIBE, false, MqttQoS.valueOf(QoS.AT_MOST_ONCE_VALUE), false, 0);
        MqttMessageIdVariableHeader mqttConnectVariableHeader = MqttMessageIdVariableHeader.from(MSG_ID + 1);
        List<String> topicList = new ArrayList<>();
        IntStream.range(0, count).forEach(i -> topicList.add("testTopic" + i));
        MqttUnsubscribePayload mqttUnSubPayload = new MqttUnsubscribePayload(topicList);
        return new MqttUnsubscribeMessage(mqttFixedHeader, mqttConnectVariableHeader, mqttUnSubPayload);
    }

    public static MqttUnsubscribeMessage badMqttUnSubMessageWithoutTopic() {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.UNSUBSCRIBE, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader mqttSubVariableHeader = MqttMessageIdVariableHeader.from(MSG_ID);

        MqttUnsubscribePayload mqttSubPayload = new MqttUnsubscribePayload(new ArrayList<>());
        return new MqttUnsubscribeMessage(mqttFixedHeader, mqttSubVariableHeader, mqttSubPayload);
    }

    public static MqttUnsubscribeMessage invalidTopicMqttUnSubMessage() {
        return topicMqttUnSubMessage("a/#/a");
    }

    public static MqttUnsubscribeMessage topicMqttUnSubMessage(String topicFilter) {
        MqttFixedHeader mqttFixedHeader =
            new MqttFixedHeader(MqttMessageType.UNSUBSCRIBE, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader mqttSubVariableHeader = MqttMessageIdVariableHeader.from(MSG_ID);

        MqttUnsubscribePayload mqttSubPayload = new MqttUnsubscribePayload(Lists.newArrayList(topicFilter));
        return new MqttUnsubscribeMessage(mqttFixedHeader, mqttSubVariableHeader, mqttSubPayload);
    }


    public static MqttMessage mqttMessage(MqttMessageType type) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(type, false, MqttQoS.valueOf(0), false, 0);
        MqttMessageIdVariableHeader mqttConnectVariableHeader = MqttMessageIdVariableHeader.from(MSG_ID);

        return new MqttMessage(mqttFixedHeader, mqttConnectVariableHeader, null);
    }

    public static MqttMessage largeMqttMessage(int bytesCount) {
        byte[] bytes = new byte[bytesCount];
        Arrays.fill(bytes, (byte) 1);
        return MqttMessageFactory.newMessage(
            new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 2),
            new MqttPublishVariableHeader("testTopic", 1), Unpooled.wrappedBuffer(bytes));
    }
}
