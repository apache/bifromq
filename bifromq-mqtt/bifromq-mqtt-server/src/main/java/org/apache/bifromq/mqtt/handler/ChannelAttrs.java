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

import org.apache.bifromq.mqtt.session.MQTTSessionContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.util.AttributeKey;
import java.net.InetSocketAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLPeerUnverifiedException;

public class ChannelAttrs {
    public static final AttributeKey<MQTTSessionContext> MQTT_SESSION_CTX = AttributeKey.valueOf("MQTTSessionContext");
    public static final AttributeKey<InetSocketAddress> PEER_ADDR = AttributeKey.valueOf("PeerAddress");

    public static X509Certificate clientCertificate(Channel channel) {
        SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
        if (sslHandler != null) {
            try {
                Certificate[] certChains = sslHandler.engine().getSession().getPeerCertificates();
                if (certChains != null && certChains.length != 0) {
                    return (X509Certificate) certChains[0];
                }
            } catch (SSLPeerUnverifiedException ex) {
                return null;
            }
        }
        return null;
    }

    public static MQTTSessionContext mqttSessionContext(ChannelHandlerContext ctx) {
        return ctx.channel().attr(MQTT_SESSION_CTX).get();
    }

    public static ChannelTrafficShapingHandler trafficShaper(ChannelHandlerContext ctx) {
        return ctx.channel().pipeline().get(ChannelTrafficShapingHandler.class);
    }

    public static void setMaxPayload(int maxUserPayloadSize, ChannelHandlerContext ctx) {
        ctx.channel().pipeline().replace(ctx.pipeline().get(MqttDecoder.class.getName()), MqttDecoder.class.getName(),
            new MqttDecoder(maxUserPayloadSize));
        if (maxUserPayloadSize > ctx.channel().config().getWriteBufferHighWaterMark()) {
            ctx.channel().config().setWriteBufferHighWaterMark(maxUserPayloadSize + 1024);
            ctx.channel().config().setWriteBufferLowWaterMark(maxUserPayloadSize / 2);
        }
    }

    public static void socketAddress(ChannelHandlerContext ctx, InetSocketAddress socketAddress) {
        ctx.channel().attr(PEER_ADDR).set(socketAddress);
    }

    public static InetSocketAddress socketAddress(Channel channel) {
        InetSocketAddress addr = channel.attr(PEER_ADDR).get();
        if (addr != null) {
            return addr;
        } else if (channel.remoteAddress() instanceof InetSocketAddress) {
            return (InetSocketAddress) channel.remoteAddress();
        } else {
            return null;
        }
    }
}
