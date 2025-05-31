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

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * A simple handler that rejects all requests that are not WebSocket upgrade requests.
 */
public class WebSocketOnlyHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private final String websocketPath;

    public WebSocketOnlyHandler(String websocketPath) {
        super(false);
        this.websocketPath = websocketPath;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
        if (!req.uri().equals(websocketPath)
            ||
            !req.headers().get(HttpHeaderNames.UPGRADE, "").equalsIgnoreCase("websocket")) {
            FullHttpResponse response =
                new DefaultFullHttpResponse(req.protocolVersion(), HttpResponseStatus.BAD_REQUEST);
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        } else {
            // Proceed with the pipeline setup for WebSocket.
            ctx.pipeline().remove(this); // Remove the validator after it's used.
            ctx.fireChannelRead(req); // Pass the request further if it's valid.
        }
    }
}
