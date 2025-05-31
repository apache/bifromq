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

package org.apache.bifromq.mqtt.handler.ws;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.apache.bifromq.mqtt.handler.ChannelAttrs;
import org.apache.bifromq.mqtt.handler.ConditionalRejectHandler;
import org.apache.bifromq.mqtt.handler.MQTTMessageDebounceHandler;
import org.apache.bifromq.mqtt.handler.MQTTPreludeHandler;
import org.apache.bifromq.mqtt.session.MQTTSessionContext;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import java.net.InetSocketAddress;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MqttOverWSHandlerTest {
    private EmbeddedChannel channel;
    private MQTTSessionContext sessionContext;
    private IEventCollector eventCollector;

    @BeforeMethod
    public void setUp() {
        eventCollector = mock(IEventCollector.class);
        // Initialize channel with the MqttOverWSHandler
        sessionContext = MQTTSessionContext.builder()
            .eventCollector(eventCollector)
            .build();
        channel = new EmbeddedChannel(true, true, new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ch.attr(ChannelAttrs.MQTT_SESSION_CTX).set(sessionContext);
                ch.attr(ChannelAttrs.PEER_ADDR).set(new InetSocketAddress("127.0.0.1", 8080));
                ch.pipeline().addLast(new MqttOverWSHandler(65536, 30, eventCollector));
            }
        });
    }

    @Test
    public void testMqttHandlerAdditionAfterHandshakeComplete() {
        // Simulate a WebSocket handshake completion event
        channel.pipeline()
            .fireUserEventTriggered(new WebSocketServerProtocolHandler.HandshakeComplete(null, null, null));

        // Check if all handlers are added
        assertNotNull(channel.pipeline().get(WebSocketFrameToByteBufDecoder.class));
        assertNotNull(channel.pipeline().get(ByteBufToWebSocketFrameEncoder.class));
        assertNotNull(channel.pipeline().get(MqttEncoder.class));
        assertNotNull(channel.pipeline().get(MqttDecoder.class));
        assertNotNull(channel.pipeline().get(MQTTMessageDebounceHandler.class));
        assertNotNull(channel.pipeline().get(ConditionalRejectHandler.class));
        assertNotNull(channel.pipeline().get(MQTTPreludeHandler.class));

        // Check that the MqttOverWSHandler itself has been removed from the pipeline
        assertNull(channel.pipeline().get(MqttOverWSHandler.class));
    }
}
