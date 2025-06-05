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

import static org.apache.bifromq.mqtt.handler.ChannelAttrs.PEER_ADDR;

import com.google.common.base.Strings;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpRequest;
import java.net.InetSocketAddress;
import lombok.extern.slf4j.Slf4j;

/**
 * This handler is used to parse the X-Real-IP and X-Real-Port headers from the HTTP request
 * and set the real IP and port of the client.
 */
@Slf4j
public class ClientAddrHandler extends ChannelInboundHandlerAdapter {
    private static final String X_REAL_IP = "X-Real-IP";
    private static final String X_REAL_PORT = "X-Real-Port";

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (ctx.channel().attr(PEER_ADDR).get() != null) {
            // Client address already set(by ProxyProtocolHandler), no need to parse headers
            ctx.fireChannelRead(msg);
            ctx.pipeline().remove(this);
            return;
        }
        if (msg instanceof HttpRequest req) {
            String realIP = req.headers().get(X_REAL_IP);
            String realPort = req.headers().get(X_REAL_PORT);

            if (!Strings.isNullOrEmpty(realIP) && !Strings.isNullOrEmpty(realPort)) {
                int port = 0;
                try {
                    port = Integer.parseInt(realPort);
                } catch (Exception e) {
                    log.warn("Invalid forwarded port number: {}", realPort);
                }
                ChannelAttrs.socketAddress(ctx, new InetSocketAddress(realIP, port));
            }
        }
        ctx.fireChannelRead(msg);
        ctx.pipeline().remove(this);
    }
}
