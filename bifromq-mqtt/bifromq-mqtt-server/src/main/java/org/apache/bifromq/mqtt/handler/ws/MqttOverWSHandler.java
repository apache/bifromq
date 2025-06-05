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

package org.apache.bifromq.mqtt.handler.ws;

import static org.apache.bifromq.mqtt.handler.condition.ORCondition.or;

import org.apache.bifromq.mqtt.handler.ConditionalRejectHandler;
import org.apache.bifromq.mqtt.handler.MQTTMessageDebounceHandler;
import org.apache.bifromq.mqtt.handler.MQTTPreludeHandler;
import org.apache.bifromq.mqtt.handler.condition.DirectMemPressureCondition;
import org.apache.bifromq.mqtt.handler.condition.HeapMemPressureCondition;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;

/**
 * A handler that adds MQTT handlers to the pipeline after the WebSocket handshake is complete.
 */
public class MqttOverWSHandler extends ChannelInboundHandlerAdapter {
    private final int maxMQTTConnectPacketSize;
    private final int connectTimeoutSeconds;
    private final IEventCollector eventCollector;

    public MqttOverWSHandler(int maxMQTTConnectPacketSize, int connectTimeoutSeconds, IEventCollector eventCollector) {
        this.maxMQTTConnectPacketSize = maxMQTTConnectPacketSize;
        this.connectTimeoutSeconds = connectTimeoutSeconds;
        this.eventCollector = eventCollector;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
            ChannelPipeline pipeline = ctx.pipeline();
            // Handshake complete, add MQTT handlers.
            pipeline.addLast("ws2bytebufDecoder", new WebSocketFrameToByteBufDecoder());
            pipeline.addLast("bytebuf2wsEncoder", new ByteBufToWebSocketFrameEncoder());
            pipeline.addLast(MqttEncoder.class.getName(), MqttEncoder.INSTANCE);
            // insert PacketFilter between Encoder
            pipeline.addLast(MqttDecoder.class.getName(), new MqttDecoder(maxMQTTConnectPacketSize));
            pipeline.addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
            pipeline.addLast(ConditionalRejectHandler.NAME,
                new ConditionalRejectHandler(or(DirectMemPressureCondition.INSTANCE, HeapMemPressureCondition.INSTANCE),
                    eventCollector));
            pipeline.addLast(MQTTPreludeHandler.NAME, new MQTTPreludeHandler(connectTimeoutSeconds));
            // Remove the handshake listener after adding MQTT handlers.
            ctx.pipeline().remove(this);
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }
}
