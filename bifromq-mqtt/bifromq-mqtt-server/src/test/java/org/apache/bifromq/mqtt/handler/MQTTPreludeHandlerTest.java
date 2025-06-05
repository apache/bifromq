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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.apache.bifromq.mqtt.MockableTest;
import org.apache.bifromq.mqtt.handler.v3.MQTT3ConnectHandler;
import org.apache.bifromq.mqtt.handler.v5.MQTT5ConnectHandler;
import org.apache.bifromq.mqtt.session.MQTTSessionContext;
import org.apache.bifromq.plugin.authprovider.IAuthProvider;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthData;
import org.apache.bifromq.plugin.eventcollector.EventType;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttIdentifierRejectedException;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttUnacceptableProtocolVersionException;
import io.netty.handler.codec.mqtt.MqttVersion;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MQTTPreludeHandlerTest extends MockableTest {
    @Mock
    private IAuthProvider authProvider;
    @Mock
    private IEventCollector eventCollector;
    private String remoteIp = "127.0.0.1";
    private int remotePort = 8888;
    private MQTTSessionContext sessionContext;
    private EmbeddedChannel embeddedChannel;

    @BeforeMethod
    public void setUp() {
        sessionContext = MQTTSessionContext.builder()
            .authProvider(authProvider)
            .eventCollector(eventCollector)
            .build();
        embeddedChannel = new EmbeddedChannel(true, true, new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ch.attr(ChannelAttrs.MQTT_SESSION_CTX).set(sessionContext);
                ch.attr(ChannelAttrs.PEER_ADDR).set(new InetSocketAddress(remoteIp, remotePort));
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(MQTTPreludeHandler.NAME, new MQTTPreludeHandler(2));
            }
        });
        embeddedChannel.freezeTime();
    }

    @Test
    public void testTimeout() {
        embeddedChannel.advanceTimeBy(3, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        verify(eventCollector).report(argThat(e -> e.type() == EventType.CONNECT_TIMEOUT));
    }

    @Test
    public void testTimeoutCancelledOnFirstMessage() {
        embeddedChannel.writeInbound(MqttMessageBuilders.connect().protocolVersion(MqttVersion.MQTT_3_1_1).build());
        embeddedChannel.advanceTimeBy(3, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        assertFalse(embeddedChannel.config().isAutoRead());
        verify(eventCollector).report(argThat(e -> e.type() != EventType.CONNECT_TIMEOUT));
    }

    @Test
    public void testDecodeErrorUnsupportVersion() {
        MqttMessage message =
            new MqttMessage(null, null, null, DecoderResult.failure(new MqttUnacceptableProtocolVersionException()));
        embeddedChannel.writeInbound(message);
        embeddedChannel.advanceTimeBy(5, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        assertNull(embeddedChannel.readOutbound());
        verify(eventCollector).report(argThat(e -> e.type() == EventType.UNACCEPTED_PROTOCOL_VER));
    }

    @Test
    public void testDecodeErrorProtocolError() {
        MqttMessage message =
            new MqttMessage(null, null, null, DecoderResult.failure(new RuntimeException()));
        embeddedChannel.writeInbound(message);
        embeddedChannel.advanceTimeBy(5, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        assertNull(embeddedChannel.readOutbound());
        verify(eventCollector).report(argThat(e -> e.type() == EventType.PROTOCOL_ERROR));
    }

    @Test
    public void testDecodeErrorBadConnectMessage() {
        MqttFixedHeader fixedHeader =
            new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessage message = new MqttMessage(fixedHeader, null,
            null, DecoderResult.failure(new RuntimeException()));
        embeddedChannel.writeInbound(message);
        embeddedChannel.advanceTimeBy(5, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        assertNull(embeddedChannel.readOutbound());
        verify(eventCollector).report(argThat(e -> e.type() == EventType.PROTOCOL_ERROR));
    }

    @Test
    public void testLargeMQTT3ConnectMessage() {
        MqttFixedHeader fixedHeader =
            new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader variableHeader =
            new MqttConnectVariableHeader("MQTT", 3, false, false, false, 0, false, false, 0);
        MqttMessage message = new MqttMessage(fixedHeader, variableHeader,
            null, DecoderResult.failure(new TooLongFrameException()));
        embeddedChannel.writeInbound(message);
        embeddedChannel.advanceTimeBy(5, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        assertNull(embeddedChannel.readOutbound());
        verify(eventCollector).report(argThat(e -> e.type() == EventType.PROTOCOL_ERROR));
    }

    @Test
    public void testMQTT3ConnectMessageWithBadIdentifier() {
        MqttFixedHeader fixedHeader =
            new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader variableHeader =
            new MqttConnectVariableHeader("MQTT", 3, false, false, false, 0, false, false, 0);
        MqttMessage message = new MqttMessage(fixedHeader, variableHeader,
            null, DecoderResult.failure(new MqttIdentifierRejectedException()));
        embeddedChannel.writeInbound(message);
        embeddedChannel.advanceTimeBy(5, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        MqttConnAckMessage connAck = embeddedChannel.readOutbound();
        assertEquals(connAck.variableHeader().connectReturnCode(),
            MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
        verify(eventCollector).report(argThat(e -> e.type() == EventType.IDENTIFIER_REJECTED));
    }

    @Test
    public void testBadMQTT3ConnectMessage() {
        MqttFixedHeader fixedHeader =
            new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader variableHeader =
            new MqttConnectVariableHeader("MQTT", 3, false, false, false, 0, false, false, 0);
        MqttMessage message = new MqttMessage(fixedHeader, variableHeader,
            null, DecoderResult.failure(new RuntimeException()));
        embeddedChannel.writeInbound(message);
        embeddedChannel.advanceTimeBy(5, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        assertNull(embeddedChannel.readOutbound());
        verify(eventCollector).report(argThat(e -> e.type() == EventType.PROTOCOL_ERROR));
    }

    @Test
    public void testLargeMQTT5ConnectMessage() {
        MqttFixedHeader fixedHeader =
            new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader variableHeader =
            new MqttConnectVariableHeader("MQTT", 5, false, false, false, 0, false, false, 0);
        MqttMessage message = new MqttMessage(fixedHeader, variableHeader,
            null, DecoderResult.failure(new TooLongFrameException()));
        embeddedChannel.writeInbound(message);
        embeddedChannel.advanceTimeBy(5, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        MqttConnAckMessage connAckMessage = embeddedChannel.readOutbound();
        assertEquals(connAckMessage.variableHeader().connectReturnCode(),
            MqttConnectReturnCode.CONNECTION_REFUSED_PACKET_TOO_LARGE);
        verify(eventCollector).report(argThat(e -> e.type() == EventType.PROTOCOL_ERROR));
    }

    @Test
    public void testMQTT5ConnectMessageWithBadIdentifier() {
        MqttFixedHeader fixedHeader =
            new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader variableHeader =
            new MqttConnectVariableHeader("MQTT", 5, false, false, false, 0, false, false, 0);
        MqttMessage message = new MqttMessage(fixedHeader, variableHeader,
            null, DecoderResult.failure(new MqttIdentifierRejectedException()));
        embeddedChannel.writeInbound(message);
        embeddedChannel.advanceTimeBy(5, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        MqttConnAckMessage connAck = embeddedChannel.readOutbound();
        assertEquals(connAck.variableHeader().connectReturnCode(),
            MqttConnectReturnCode.CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID);
        verify(eventCollector).report(argThat(e -> e.type() == EventType.IDENTIFIER_REJECTED));
    }

    @Test
    public void testBadMQTT5ConnectMessage() {
        MqttFixedHeader fixedHeader =
            new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader variableHeader =
            new MqttConnectVariableHeader("MQTT", 5, false, false, false, 0, false, false, 0);
        MqttMessage message = new MqttMessage(fixedHeader, variableHeader,
            null, DecoderResult.failure(new RuntimeException()));
        embeddedChannel.writeInbound(message);
        embeddedChannel.advanceTimeBy(5, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        MqttConnAckMessage connAck = embeddedChannel.readOutbound();
        assertEquals(connAck.variableHeader().connectReturnCode(),
            MqttConnectReturnCode.CONNECTION_REFUSED_MALFORMED_PACKET);
        verify(eventCollector).report(argThat(e -> e.type() == EventType.PROTOCOL_ERROR));
    }

    @Test
    public void testFirstNotConnect() {
        embeddedChannel.writeInbound(MqttMessageBuilders.connAck().build());
        embeddedChannel.advanceTimeBy(5, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        assertNull(embeddedChannel.readOutbound());
        verify(eventCollector).report(argThat(e -> e.type() == EventType.PROTOCOL_ERROR));
    }

    @Test
    public void testReleaseFirstPublish() {
        MqttPublishMessage message = MqttMessageBuilders.publish()
            .topicName("abc")
            .qos(MqttQoS.AT_MOST_ONCE)
            .payload(Unpooled.buffer().writeBytes("abc".getBytes()))
            .build();
        ByteBuf payload = message.payload();
        assertEquals(payload.refCnt(), 1);
        embeddedChannel.writeInbound(message);
        embeddedChannel.advanceTimeBy(5, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        assertNull(embeddedChannel.readOutbound());
        verify(eventCollector).report(argThat(e -> e.type() == EventType.PROTOCOL_ERROR));
        assertEquals(payload.refCnt(), 0);
    }

    @Test
    public void testChannelException() {
        embeddedChannel.pipeline().addBefore(MQTTPreludeHandler.NAME, "exception", new ChannelInboundHandlerAdapter() {
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                throw new RuntimeException("test");
            }
        });
        embeddedChannel.writeInbound("test");
        verify(eventCollector).report(argThat(e -> e.type() == EventType.CHANNEL_ERROR));
        assertFalse(embeddedChannel.isOpen());
    }

    @Test
    public void testSetupMQTT3ConnectionHandler() {
        when(authProvider.auth(any(MQTT3AuthData.class))).thenReturn(new CompletableFuture<>());
        embeddedChannel.writeInbound(
            MqttMessageBuilders.connect().clientId("abc")
                .protocolVersion(MqttVersion.MQTT_3_1_1)
                .build());
        embeddedChannel.advanceTimeBy(3, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        assertNotNull(embeddedChannel.pipeline().get(MQTT3ConnectHandler.NAME));
        assertNull(embeddedChannel.pipeline().get(MQTTPreludeHandler.NAME));
    }

    @Test
    public void testSetupMQTT5ConnectionHandler() {
        when(authProvider.auth(any(MQTT3AuthData.class))).thenReturn(new CompletableFuture<>());
        when(authProvider.auth(any(MQTT5AuthData.class))).thenReturn(new CompletableFuture<>());
        embeddedChannel.writeInbound(MqttMessageBuilders.connect()
            .protocolVersion(MqttVersion.MQTT_5)
            .cleanSession(true)
            .clientId("")
            .build());
        embeddedChannel.advanceTimeBy(3, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        assertNotNull(embeddedChannel.pipeline().get(MQTT5ConnectHandler.NAME));
        assertNull(embeddedChannel.pipeline().get(MQTTPreludeHandler.NAME));
    }
}
