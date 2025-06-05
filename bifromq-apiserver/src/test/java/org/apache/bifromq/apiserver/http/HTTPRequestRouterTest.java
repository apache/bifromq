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

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.apiserver.Headers;
import org.apache.bifromq.apiserver.MockableTest;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class HTTPRequestRouterTest extends MockableTest {
    private EmbeddedChannel channel;
    @Mock
    private IHTTPRouteMap routeMap;
    @Mock
    private IHTTPRequestHandler requestHandler;
    @Mock
    private ISettingProvider settingProvider;

    private String tenantId = "tenantId";

    @BeforeMethod
    public void setup() {
        super.setup();
        channel = new EmbeddedChannel(true, true, new ChannelInitializer<EmbeddedChannel>() {
            @Override
            protected void initChannel(EmbeddedChannel ch) {
                ch.pipeline().addLast(new HTTPRequestRouter(routeMap, settingProvider));
            }
        });
        channel.freezeTime();
        when(settingProvider.provide(Setting.MaxUserPayloadBytes, tenantId)).thenReturn(256 * 1024);
    }

    @Test
    public void keepAliveHeaderForHTTP_1_0() {
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/fake");
        req.headers().set(CONNECTION, KEEP_ALIVE);
        when(routeMap.getHandler(any())).thenReturn(requestHandler);
        when(requestHandler.handle(anyLong(), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new DefaultFullHttpResponse(HttpVersion.HTTP_1_0, HttpResponseStatus.OK)));

        channel.writeInbound(req);
        HttpResponse resp = channel.readOutbound();
        assertEquals(resp.protocolVersion(), req.protocolVersion());
        assertEquals(resp.headers().get(CONNECTION), KEEP_ALIVE.toString());
    }

    @Test
    public void closeConnectionForHTTP_1_0() {
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/fake");
        when(routeMap.getHandler(any())).thenReturn(requestHandler);
        when(requestHandler.handle(anyLong(), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new DefaultFullHttpResponse(HttpVersion.HTTP_1_0, HttpResponseStatus.OK)));

        channel.writeInbound(req);
        HttpResponse resp = channel.readOutbound();
        assertEquals(resp.protocolVersion(), req.protocolVersion());
        assertEquals(resp.status().code(), OK.code());
        assertEquals(resp.headers().get(CONNECTION), CLOSE.toString());
        assertFalse(channel.isOpen());
    }

    @Test
    public void handleNormalReturn() {
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        long reqId = System.nanoTime();
        req.headers().set(Headers.HEADER_REQ_ID.header, reqId);
        when(routeMap.getHandler(any())).thenReturn(requestHandler);
        when(requestHandler.handle(anyLong(), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)));

        channel.writeInbound(req);

        HttpResponse resp = channel.readOutbound();
        assertEquals(resp.protocolVersion(), req.protocolVersion());
        assertEquals(resp.status(), HttpResponseStatus.OK);
        assertEquals(resp.headers().get(Headers.HEADER_REQ_ID.header), Long.toString(reqId));
    }

    @Test
    public void handleException() {
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        long reqId = System.nanoTime();
        req.headers().set(Headers.HEADER_REQ_ID.header, reqId);
        when(routeMap.getHandler(any())).thenReturn(requestHandler);
        when(requestHandler.handle(anyLong(), any())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Mocked Exception")));

        channel.writeInbound(req);

        HttpResponse resp = channel.readOutbound();
        assertEquals(resp.protocolVersion(), req.protocolVersion());
        assertEquals(resp.status(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        assertEquals(resp.headers().get(CONTENT_TYPE), "text/plain; charset=UTF-8");
        assertTrue(resp.headers().getInt(CONTENT_LENGTH) > 0);
        assertEquals(resp.headers().get(Headers.HEADER_REQ_ID.header), Long.toString(reqId));
    }

}
