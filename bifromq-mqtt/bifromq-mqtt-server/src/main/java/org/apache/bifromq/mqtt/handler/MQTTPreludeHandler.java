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

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_MALFORMED_PACKET;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_PACKET_TOO_LARGE;
import static io.netty.handler.codec.mqtt.MqttMessageType.CONNECT;
import static io.netty.handler.codec.mqtt.MqttMessageType.PUBLISH;
import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttIdentifierRejectedException;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttUnacceptableProtocolVersionException;
import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.mqtt.handler.v3.MQTT3ConnectHandler;
import org.apache.bifromq.mqtt.handler.v5.MQTT5ConnectHandler;
import org.apache.bifromq.mqtt.handler.v5.MQTT5MessageBuilders;
import org.apache.bifromq.plugin.eventcollector.Event;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed.ChannelError;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed.ConnectTimeout;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed.IdentifierRejected;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed.ProtocolError;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed.UnacceptedProtocolVer;

@Slf4j
public class MQTTPreludeHandler extends ChannelDuplexHandler {
    public static final String NAME = "MqttPreludeHandler";
    private final long timeoutInSec;
    private ChannelHandlerContext ctx;
    private IEventCollector eventCollector;
    private InetSocketAddress remoteAddr;
    private ScheduledFuture<?> timeoutCloseTask;
    private ScheduledFuture<?> closeConnectionTask;

    public MQTTPreludeHandler(int timeoutInSec) {
        this.timeoutInSec = timeoutInSec;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        eventCollector = ChannelAttrs.mqttSessionContext(ctx).eventCollector;
        remoteAddr = ChannelAttrs.socketAddress(ctx.channel());
        timeoutCloseTask = ctx.executor().schedule(() -> {
            eventCollector.report(getLocal(ConnectTimeout.class).peerAddress(remoteAddr));
            ctx.channel().close();
        }, timeoutInSec, TimeUnit.SECONDS);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        if (timeoutCloseTask != null) {
            timeoutCloseTask.cancel(true);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (timeoutCloseTask != null) {
            timeoutCloseTask.cancel(true);
        }
        ctx.fireChannelInactive();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        assert msg instanceof MqttMessage;
        // stop reading next message and resume reading once finish processing current one
        ctx.channel().config().setAutoRead(false);
        // cancel the scheduled connect timeout task
        timeoutCloseTask.cancel(true);
        MqttMessage message = (MqttMessage) msg;
        if (!message.decoderResult().isSuccess()) {
            Throwable cause = message.decoderResult().cause();
            if (cause instanceof MqttUnacceptableProtocolVersionException) {
                closeChannelWithRandomDelay(getLocal(UnacceptedProtocolVer.class)
                    .peerAddress(remoteAddr));
                return;
            }
            if (message.fixedHeader() != null && message.fixedHeader().messageType() != CONNECT) {
                closeChannelWithRandomDelay(getLocal(ProtocolError.class).peerAddress(remoteAddr)
                    .statement("MQTT-3.1.0-1"));
                return;
            }
            if (message.variableHeader() instanceof MqttConnectVariableHeader connVarHeader) {
                switch (connVarHeader.version()) {
                    case 3:
                    case 4:
                        if (cause instanceof TooLongFrameException) {
                            closeChannelWithRandomDelay(getLocal(ProtocolError.class)
                                .statement("Too large packet")
                                .peerAddress(remoteAddr));
                        } else if (cause instanceof MqttIdentifierRejectedException) {
                            closeChannelWithRandomDelay(getLocal(IdentifierRejected.class).peerAddress(remoteAddr),
                                MqttMessageBuilders.connAck()
                                    .returnCode(CONNECTION_REFUSED_IDENTIFIER_REJECTED)
                                    .build());
                        } else {
                            closeChannelWithRandomDelay(getLocal(ProtocolError.class)
                                .peerAddress(remoteAddr).statement("MQTT3-4.8.0-2"));
                        }
                        return;
                    case 5:
                    default:
                        if (cause instanceof TooLongFrameException) {
                            closeChannelWithRandomDelay(getLocal(ProtocolError.class)
                                    .statement("Too large packet")
                                    .peerAddress(remoteAddr),
                                MqttMessageBuilders.connAck()
                                    .properties(MQTT5MessageBuilders.connAckProperties()
                                        .reasonString(cause.getMessage())
                                        .build())
                                    .returnCode(CONNECTION_REFUSED_PACKET_TOO_LARGE)
                                    .build());
                        } else if (cause instanceof MqttIdentifierRejectedException) {
                            // decode mqtt connect packet error
                            closeChannelWithRandomDelay(getLocal(IdentifierRejected.class).peerAddress(remoteAddr),
                                MqttMessageBuilders.connAck()
                                    .properties(MQTT5MessageBuilders.connAckProperties()
                                        .reasonString(cause.getMessage())
                                        .build())
                                    .returnCode(CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID)
                                    .build());
                        } else {
                            // according to [MQTT-4.13.1-1]
                            closeChannelWithRandomDelay(getLocal(ProtocolError.class).peerAddress(remoteAddr)
                                    .statement("MQTT5-4.13.1-1"),
                                MqttMessageBuilders.connAck()
                                    .properties(MQTT5MessageBuilders.connAckProperties()
                                        .reasonString(cause.getMessage())
                                        .build())
                                    .returnCode(CONNECTION_REFUSED_MALFORMED_PACKET)
                                    .build());
                        }
                        return;
                }
            } else {
                closeChannelWithRandomDelay(
                    getLocal(ProtocolError.class).peerAddress(remoteAddr).statement(cause.getMessage()));
                return;
            }
        } else if (message.fixedHeader().messageType() != CONNECT) {
            if (message.fixedHeader().messageType() == PUBLISH) {
                ((MqttPublishMessage) message).release();
            }
            // according to [MQTT-3.1.0-1]
            closeChannelWithRandomDelay(getLocal(ProtocolError.class).statement("MQTT-3.1.0-1"));
            log.debug("First packet must be mqtt connect message: remote={}", remoteAddr);
            return;
        }

        MqttConnectMessage connectMessage = (MqttConnectMessage) message;
        switch (connectMessage.variableHeader().version()) {
            case 3:
            case 4:
                ctx.pipeline().addAfter(ctx.executor(),
                    MQTTPreludeHandler.NAME, MQTT3ConnectHandler.NAME, new MQTT3ConnectHandler());
                // delegate to MQTT 3 handler
                ctx.fireChannelRead(connectMessage);
                ctx.pipeline().remove(this);
                break;
            case 5:
                ctx.pipeline().addAfter(ctx.executor(),
                    MQTTPreludeHandler.NAME, MQTT5ConnectHandler.NAME, new MQTT5ConnectHandler());
                // delegate to MQTT 5 handler
                ctx.fireChannelRead(connectMessage);
                ctx.pipeline().remove(this);
                break;
            default:
                log.debug("Unsupported protocol version: {}", connectMessage.variableHeader().version());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // simple strategy: shutdown the channel directly
        log.debug("ctx: {}, cause:", ctx, cause);
        eventCollector.report(getLocal(ChannelError.class).peerAddress(remoteAddr).cause(cause));
        ctx.channel().close();
    }

    private void closeChannelWithRandomDelay(Event<?> reason) {
        closeChannelWithRandomDelay(reason, null);
    }

    private void closeChannelWithRandomDelay(Event<?> reason, MqttMessage farewell) {
        if (timeoutCloseTask != null) {
            timeoutCloseTask.cancel(true);
        }
        eventCollector.report(reason);
        assert closeConnectionTask == null;
        closeConnectionTask = ctx.executor().schedule(() -> {
            if (!ctx.channel().isActive()) {
                return;
            }
            if (farewell != null) {
                ctx.writeAndFlush(farewell).addListener(ChannelFutureListener.CLOSE);
            } else {
                ctx.channel().close();
            }
        }, ThreadLocalRandom.current().nextInt(5000), TimeUnit.MILLISECONDS);
    }
}
