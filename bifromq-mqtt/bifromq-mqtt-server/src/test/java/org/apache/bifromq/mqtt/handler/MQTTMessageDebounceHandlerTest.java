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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.mqtt.MockableTest;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MQTTMessageDebounceHandlerTest extends MockableTest {
    private EmbeddedChannel channel;

    @BeforeMethod
    public void setUp() {
        channel = new EmbeddedChannel(new MQTTMessageDebounceHandler());
    }

    @Test
    public void testAutoReadModeMessagePassThrough() {
        // Enable auto read
        channel.config().setAutoRead(true);

        MqttMessage message = MqttMessageBuilders.connect()
            .clientId("abc")
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();
        assertTrue(channel.writeInbound(message));
        assertTrue(channel.finish());

        MqttMessage readMessage = channel.readInbound();
        assertNotNull(readMessage);

        assertEquals(MqttMessageType.CONNECT, readMessage.fixedHeader().messageType());
    }

    @Test
    public void testNonAutoReadModeMessageBuffered() {
        // Disable auto read
        channel.config().setAutoRead(false);

        MqttMessage message1 =
            new MqttMessage(new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_LEAST_ONCE, false, 0));
        MqttMessage message2 =
            new MqttMessage(new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0));
        channel.writeInbound(message1);
        channel.writeInbound(message2);
        // Initially, no message should be passed through
        assertNull(channel.readInbound());

        // Trigger read
        channel.read();
        MqttMessage readMessage1 = channel.readInbound();
        assertNotNull(readMessage1);
        assertEquals(MqttMessageType.CONNECT, readMessage1.fixedHeader().messageType());

        // Ensure no more messages are passed through until next read
        assertNull(channel.readInbound());

        // Trigger another read
        channel.read();
        MqttMessage readMessage2 = channel.readInbound();
        assertNotNull(readMessage2);
        assertEquals(MqttMessageType.PUBLISH, readMessage2.fixedHeader().messageType());
    }

    @Test
    public void testChannelInactiveReleasesMessages() {
        // Disable auto read to buffer messages
        channel.config().setAutoRead(false);

        MqttPublishMessage message = MqttMessageBuilders.publish()
            .qos(MqttQoS.AT_MOST_ONCE)
            .payload(Unpooled.buffer().writeBytes(new byte[10]))
            .build();
        message.retain();
        assertEquals(message.content().refCnt(), 2);
        channel.writeInbound(message);
        // Simulate channel becoming inactive
        channel.close().awaitUninterruptibly();
        assertFalse(channel.isOpen());
        assertEquals(message.refCnt(), 1);
    }
}
