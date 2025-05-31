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

import static org.testng.Assert.assertEquals;

import org.apache.bifromq.mqtt.MockableTest;
import org.apache.bifromq.mqtt.handler.v3.MQTT3MessageBuilders;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MQTT3MessageSizerTest extends MockableTest {
    private final IMQTTMessageSizer sizer = IMQTTMessageSizer.mqtt3();
    private EmbeddedChannel inChannel;
    private EmbeddedChannel outChannel;

    @BeforeMethod
    public void setup() {
        inChannel = new EmbeddedChannel(new MqttDecoder());

        outChannel = new EmbeddedChannel(MqttEncoder.INSTANCE);
        outChannel.writeOutbound(MqttMessageBuilders.connect()
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .clientId("")
            .build());
        outChannel.readOutbound();
    }

    @Test
    public void testConnect() {
        MqttMessage message = MqttMessageBuilders.connect()
            .clientId("")
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .keepAlive(10)
            .username("user")
            .password(new byte[8])
            .willFlag(true)
            .willTopic("willTopic")
            .willRetain(true)
            .willMessage(new byte[8])
            .build();
        verifySize(message);
    }

    @Test
    public void testPublish() {
        MqttMessage message = MqttMessageBuilders.publish()
            .topicName("topic")
            .qos(MqttQoS.AT_MOST_ONCE)
            .retained(true)
            .payload(Unpooled.wrappedBuffer(new byte[8]))
            .build();
        verifySize(message);
    }

    @Test
    public void testPubAck() {
        MqttMessage message = MqttMessageBuilders.pubAck()
            .packetId(1)
            .build();
        verifySize(message);
    }

    @Test
    public void testPubRec() {
        MqttMessage message = MQTT3MessageBuilders.pubRec()
            .packetId(1)
            .build();
        verifySize(message);
    }

    @Test
    public void testPubRel() {
        MqttMessage message = MQTT3MessageBuilders.pubRel()
            .packetId(1)
            .build();
        verifySize(message);
    }

    @Test
    public void testPubComp() {
        MqttMessage message = MQTT3MessageBuilders.pubComp()
            .packetId(1)
            .build();
        verifySize(message);
    }

    @Test
    public void testSub() {
        MqttMessage message = MqttMessageBuilders.subscribe()
            .messageId(1)
            .addSubscription(MqttQoS.AT_MOST_ONCE, "topic")
            .addSubscription(MqttQoS.AT_MOST_ONCE, "topic1")
            .build();
        verifySize(message);
    }

    @Test
    public void testSubAck() {
        MqttMessage message = MqttMessageBuilders.subAck()
            .packetId(1)
            .addGrantedQoses(MqttQoS.AT_MOST_ONCE, MqttQoS.AT_LEAST_ONCE)
            .build();
        verifySize(message);
    }

    @Test
    public void testUnsub() {
        MqttMessage message = MqttMessageBuilders.unsubscribe()
            .messageId(1)
            .addTopicFilter("topic")
            .addTopicFilter("topic1")
            .build();
        verifySize(message);
    }

    @Test
    public void testUnsubAck() {
        MqttMessage message = MqttMessageBuilders.unsubAck().packetId(1).build();
        verifySize(message);
    }

    @Test
    public void testDisconnect() {
        MqttMessage message = MqttMessageBuilders.disconnect().build();
        verifySize(message);
    }

    @Test
    public void testPing() {
        verifySize(MqttMessage.PINGREQ);
        verifySize(MqttMessage.PINGRESP);
    }

    @Test
    public void testConnectAck() {
        MqttMessage message = MqttMessageBuilders.connAck()
            .sessionPresent(true)
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
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
