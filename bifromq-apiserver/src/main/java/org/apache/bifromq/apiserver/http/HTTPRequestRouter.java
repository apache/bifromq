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

package org.apache.bifromq.apiserver.http;

import static org.apache.bifromq.apiserver.Headers.HEADER_REQ_ID;
import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_0;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.apache.bifromq.apiserver.http.handler.HeaderUtils;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HTTPRequestRouter extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final FullHttpResponse TOO_LARGE_CLOSE = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, Unpooled.EMPTY_BUFFER);

    static {
        TOO_LARGE_CLOSE.headers().set(CONTENT_LENGTH, 0);
        TOO_LARGE_CLOSE.headers().set(CONNECTION, HttpHeaderValues.CLOSE);
    }

    private final IHTTPRouteMap routeMap;
    private final ISettingProvider settingProvider;

    public HTTPRequestRouter(IHTTPRouteMap routeMap, ISettingProvider settingProvider) {
        this.routeMap = routeMap;
        this.settingProvider = settingProvider;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof HttpServerUpgradeHandler.UpgradeEvent) {
            ctx.pipeline().remove(this);
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
        long reqId = HeaderUtils.getOptionalReqId(req);
        req.retain();
        doHandle(reqId, req)
            .whenComplete((v, e) -> {
                FullHttpResponse response;
                if (e != null) {
                    ByteBuf content = ctx.alloc().buffer();
                    content.writeBytes(e.getMessage().getBytes());
                    response = new DefaultFullHttpResponse(HTTP_1_1, toHTTPStatus(e), content);
                    response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
                } else {
                    response = v;
                }
                response.headers().set(HEADER_REQ_ID.header, reqId);
                response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes());
                doResponse(ctx, req, response);
                req.release();
            });
    }

    private CompletableFuture<FullHttpResponse> doHandle(long reqId, FullHttpRequest req) {
        try {
            return routeMap.getHandler(req).handle(reqId, req);
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private void doResponse(ChannelHandlerContext ctx, FullHttpRequest req, FullHttpResponse response) {
        boolean keepAlive = HttpUtil.isKeepAlive(req);
        if (keepAlive) {
            if (req.protocolVersion().equals(HTTP_1_0)) {
                response.headers().set(CONNECTION, KEEP_ALIVE);
            }
            ctx.writeAndFlush(response);
        } else {
            // Tell the client we're going to close the connection.
            response.headers().set(CONNECTION, CLOSE);
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }

    private HttpResponseStatus toHTTPStatus(Throwable t) {
        if (t instanceof IllegalArgumentException) {
            return HttpResponseStatus.BAD_REQUEST;
        } else {
            return HttpResponseStatus.INTERNAL_SERVER_ERROR;
        }
    }
}
