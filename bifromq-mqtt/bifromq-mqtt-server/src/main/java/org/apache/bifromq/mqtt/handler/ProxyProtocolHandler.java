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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;

/**
 * This handler is used to parse the HAProxy protocol message and extract the real IP and port of the client.
 */
public class ProxyProtocolHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            if (msg instanceof HAProxyMessage proxyMsg) {
                switch (proxyMsg.command()) {
                    case PROXY -> {
                        String realIP = proxyMsg.sourceAddress();
                        int realPort = proxyMsg.sourcePort();
                        if (realIP != null && realPort > 0) {
                            ChannelAttrs.socketAddress(ctx, new InetSocketAddress(realIP, realPort));
                        }
                    }
                    case LOCAL -> {
                        // this is a health check probe from Proxy
                        ChannelPipeline pipeline = ctx.pipeline();
                        Iterator<Map.Entry<String, ChannelHandler>> entryItr = pipeline.iterator();
                        boolean startRemove = false;
                        while (entryItr.hasNext()) {
                            Map.Entry<String, ChannelHandler> entry = entryItr.next();
                            if (entry.getValue().getClass().equals(this.getClass())) {
                                startRemove = true;
                                continue;
                            }
                            if (startRemove) {
                                pipeline.remove(entry.getValue());
                            }
                        }
                    }
                    default -> {
                        // never happen
                    }
                }
                proxyMsg.release();
            } else {
                ctx.fireChannelRead(msg);
            }
        } finally {
            ctx.pipeline().remove(this);
        }
    }
}
