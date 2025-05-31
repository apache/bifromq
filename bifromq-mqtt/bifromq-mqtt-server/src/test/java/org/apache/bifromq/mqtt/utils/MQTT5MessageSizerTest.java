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

package org.apache.bifromq.mqtt.utils;

import static io.netty.handler.codec.mqtt.MqttSubscriptionOption.RetainedHandlingPolicy.SEND_AT_SUBSCRIBE;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.inbox.storage.proto.TopicFilterOption;
import org.apache.bifromq.mqtt.MockableTest;
import org.apache.bifromq.mqtt.handler.MQTTSessionHandler;
import org.apache.bifromq.mqtt.handler.v5.MQTT5MessageBuilders;
import org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5AuthReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5DisconnectReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5PubAckReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5PubCompReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5PubRecReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5PubRelReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5SubAckReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5UnsubAckReasonCode;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.StringPair;
import org.apache.bifromq.type.UserProperties;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MQTT5MessageSizerTest extends MockableTest {
    private final IMQTTMessageSizer sizer = IMQTTMessageSizer.mqtt5();
    private EmbeddedChannel inChannel;
    private EmbeddedChannel outChannel;

    @BeforeMethod
    public void setup() {
        inChannel = new EmbeddedChannel(new MqttDecoder());
        outChannel = new EmbeddedChannel(MqttEncoder.INSTANCE);
        outChannel.writeOutbound(MqttMessageBuilders.connect()
            .protocolVersion(MqttVersion.MQTT_5)
            .clientId("")
            .build());
        ByteBuf byteBuf = outChannel.readOutbound();
        inChannel.writeInbound(byteBuf);
        inChannel.readInbound();
    }

    @Test
    public void testConnect() {
        MqttProperties mqttProperties = MQTT5MessageUtils.mqttProps()
            .addReasonString("reasonString")
            .addUserProperty("key", "value").build();
        MqttMessage message = MqttMessageBuilders.connect()
            .clientId("")
            .protocolVersion(MqttVersion.MQTT_5)
            .keepAlive(10)
            .username("user")
            .password(new byte[8])
            .properties(mqttProperties)
            .willFlag(true)
            .willTopic("willTopic")
            .willRetain(true)
            .willMessage(new byte[8])
            .willProperties(mqttProperties)
            .build();
        verifySize(message);
    }

    @Test
    public void testConnectAck() {
        MqttMessage message = MqttMessageBuilders.connAck()
            .properties(MQTT5MessageUtils.mqttProps().addReasonString("reasonString").build())
            .sessionPresent(true)
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
            .build();
        verifySize(message);
    }

    @Test
    public void testPublish() {
        MqttMessage message = MQTT5MessageBuilders.pub()
            .setupAlias(true)
            .topicAlias(1)
            .message(new MQTTSessionHandler.SubMessage("topic",
                Message.newBuilder()
                    .setPayload(ByteString.copyFromUtf8("payload"))
                    .build(),
                ClientInfo.getDefaultInstance(), "topicFilter",
                TopicFilterOption.getDefaultInstance(),
                true, false, 0))
            .build();
        verifySize(message);
    }

    @Test
    public void testPubAck() {
        MqttMessage message = MQTT5MessageBuilders.pubAck(true)
            .packetId(1)
            .reasonCode(MQTT5PubAckReasonCode.Success)
            .reasonString("reasonString")
            .userProps(UserProperties.newBuilder()
                .addUserProperties(StringPair.newBuilder()
                    .setKey("key")
                    .setValue("value")
                    .build())
                .build())
            .build();
        verifySize(message);
        message = MQTT5MessageBuilders.pubAck(true)
            .packetId(1)
            .reasonCode(MQTT5PubAckReasonCode.Success)
            .build();
        verifySize(message);
    }

    @Test
    public void testPubRec() {
        MqttMessage message = MQTT5MessageBuilders.pubRec(true)
            .packetId(1)
            .reasonCode(MQTT5PubRecReasonCode.Success)
            .reasonString("reasonString")
            .build();
        verifySize(message);
        message = MQTT5MessageBuilders.pubRec(true)
            .packetId(1)
            .reasonCode(MQTT5PubRecReasonCode.Success)
            .build();
        verifySize(message);
    }

    @Test
    public void testPubRel() {
        MqttMessage message = MQTT5MessageBuilders.pubRel(true)
            .packetId(1)
            .reasonCode(MQTT5PubRelReasonCode.Success)
            .reasonString("reasonString")
            .build();
        verifySize(message);
        message = MQTT5MessageBuilders.pubRel(true)
            .packetId(1)
            .reasonCode(MQTT5PubRelReasonCode.Success)
            .build();
        verifySize(message);
    }

    @Test
    public void testPubComp() {
        MqttMessage message = MQTT5MessageBuilders.pubComp(true)
            .packetId(1)
            .reasonCode(MQTT5PubCompReasonCode.Success)
            .reasonString("reasonString")
            .build();
        verifySize(message);
        message = MQTT5MessageBuilders.pubComp(true)
            .packetId(1)
            .reasonCode(MQTT5PubCompReasonCode.Success)
            .build();
        verifySize(message);
    }

    @Test
    public void testSub() {
        MqttMessage message = MqttMessageBuilders.subscribe()
            .messageId(1)
            .addSubscription("topic", new MqttSubscriptionOption(MqttQoS.AT_MOST_ONCE, true, true, SEND_AT_SUBSCRIBE))
            .build();
        verifySize(message);
    }

    @Test
    public void testSubAck() {
        MqttMessage message = MQTT5MessageBuilders.subAck()
            .packetId(1)
            .reasonCodes(MQTT5SubAckReasonCode.GrantedQoS0, MQTT5SubAckReasonCode.GrantedQoS0)
            .build();
        verifySize(message);
    }

    @Test
    public void testUnsub() {
        MqttMessage message = MQTT5MessageBuilders.unsubAck()
            .packetId(1)
            .addReasonCode(MQTT5UnsubAckReasonCode.Success)
            .addReasonCode(MQTT5UnsubAckReasonCode.Success)
            .build();
        verifySize(message);
    }

    @Test
    public void testUnsubAck() {
        MqttMessage message = MQTT5MessageBuilders.unsubAck()
            .packetId(1)
            .addReasonCodes(MQTT5UnsubAckReasonCode.Success, MQTT5UnsubAckReasonCode.Success)
            .reasonString("reasonString")
            .build();
        verifySize(message);
    }

    @Test
    public void testAuth() {
        MqttMessage message = MQTT5MessageBuilders.auth("jwt")
            .authData(ByteString.copyFromUtf8("authData"))
            .reasonCode(MQTT5AuthReasonCode.Success)
            .reasonString("reasonString")
            .build();
        verifySize(message);
        message = MQTT5MessageBuilders.auth("jwt")
            .reasonCode(MQTT5AuthReasonCode.Success)
            .build();
        verifySize(message);
    }

    @Test
    public void testDisconnect() {
        MqttMessage message = MQTT5MessageBuilders.disconnect()
            .reasonCode(MQTT5DisconnectReasonCode.Normal)
            .build();
        verifySize(message);

        message = MQTT5MessageBuilders.disconnect()
            .reasonCode(MQTT5DisconnectReasonCode.Normal)
            .reasonString("disconnected")
            .build();
        verifySize(message);

        message = MQTT5MessageBuilders.disconnect()
            .reasonCode(MQTT5DisconnectReasonCode.PacketTooLarge)
            .reasonString("packet toolarge")
            .userProps(UserProperties.newBuilder()
                .addUserProperties(StringPair.newBuilder().setKey("key").setValue("val").build()).build())
            .build();
        verifySize(message);
    }

    private void verifySize(MqttMessage message) {
        IMQTTMessageSizer.MqttMessageSize calcSize = sizer.sizeOf(message);
        outChannel.writeOutbound(message);
        ByteBuf readBytes = outChannel.readOutbound();

        int realSize = readBytes.readableBytes();
        assertEquals(calcSize.encodedBytes(), realSize);

        inChannel.writeInbound(readBytes);
        MqttMessage decodedMessage = inChannel.readInbound();
        assertEquals(sizer.sizeByHeader(decodedMessage.fixedHeader()), realSize);
    }
}
