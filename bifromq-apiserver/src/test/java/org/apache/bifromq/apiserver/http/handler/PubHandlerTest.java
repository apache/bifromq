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

package org.apache.bifromq.apiserver.http.handler;

import static org.apache.bifromq.apiserver.Headers.HEADER_CLIENT_META_PREFIX;
import static org.apache.bifromq.apiserver.Headers.HEADER_CLIENT_TYPE;
import static org.apache.bifromq.apiserver.Headers.HEADER_EXPIRY_SECONDS;
import static org.apache.bifromq.apiserver.Headers.HEADER_QOS;
import static org.apache.bifromq.apiserver.Headers.HEADER_TOPIC;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.apiserver.Headers;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.dist.client.PubResult;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.type.QoS;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class PubHandlerTest extends AbstractHTTPRequestHandlerTest<PubHandler> {
    @Mock
    private IDistClient distClient;
    private ISettingProvider settingProvider = Setting::current;

    @Override
    protected Class<PubHandler> handlerClass() {
        return PubHandler.class;
    }

    @Test
    public void missingHeaders() {
        DefaultFullHttpRequest req = buildRequest();

        PubHandler handler = new PubHandler(settingProvider, distClient);
        assertThrows(() -> handler.handle(123, "fakeTenant", req).join());
    }

    @Test
    public void pub() {
        ByteBuf content = Unpooled.wrappedBuffer("Hello BifroMQ".getBytes());
        DefaultFullHttpRequest req = buildRequest(HttpMethod.POST, content);
        req.headers().set(HEADER_TOPIC.header, "admin_user");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_QOS.header, "1");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(PubResult.OK));
        PubHandler handler = new PubHandler(settingProvider, distClient);
        handler.handle(reqId, tenantId, req);
        verify(distClient).pub(eq(reqId),
            eq(req.headers().get(HEADER_TOPIC.header)),
            argThat(m -> m.getPubQoS().equals(QoS.AT_LEAST_ONCE) &&
                m.getPayload().equals(ByteString.copyFrom(content.nioBuffer())) &&
                m.getExpiryInterval() == Integer.MAX_VALUE &&
                m.getTimestamp() > 0),
            argThat(killer -> killer.getType().equals(req.headers().get(HEADER_CLIENT_TYPE.header)) &&
                killer.getMetadataCount() == 1 &&
                killer.getMetadataMap().get("age").equals("4")));
    }

    @Test
    public void distResults() {
        dist(PubResult.OK, HttpResponseStatus.OK);
        dist(PubResult.NO_MATCH, HttpResponseStatus.OK);
        dist(PubResult.ERROR, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        dist(PubResult.BACK_PRESSURE_REJECTED, HttpResponseStatus.BAD_REQUEST);
    }

    @Test
    public void pubWithWrongQoS() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_TOPIC.header, "/greeting");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_QOS.header, "3");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        PubHandler handler = new PubHandler(settingProvider, distClient);

        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(PubResult.OK));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.BAD_REQUEST);
        assertTrue(response.content().readableBytes() > 0);
    }

    @Test
    public void pubWithWrongExpirySeconds() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_TOPIC.header, "/greeting");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_QOS.header, "3");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        req.headers().set(HEADER_EXPIRY_SECONDS.header, "0");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        PubHandler handler = new PubHandler(settingProvider, distClient);

        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(PubResult.OK));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.BAD_REQUEST);
        assertTrue(response.content().readableBytes() > 0);
    }


    public void dist(PubResult result, HttpResponseStatus expectedStatus) {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_TOPIC.header, "/greeting");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_QOS.header, "1");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        PubHandler handler = new PubHandler(settingProvider, distClient);

        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(result));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), expectedStatus);
    }

    @Test
    public void requestWithoutTenantId() {
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        long reqId = System.nanoTime();
        req.headers().set(Headers.HEADER_REQ_ID.header, reqId);
        PubHandler handler = new PubHandler(settingProvider, distClient);
        FullHttpResponse response = handler.handle(reqId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), BAD_REQUEST);
    }


    @Test
    public void tooLargeRequest() {
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        long reqId = System.nanoTime();
        req.headers().set(Headers.HEADER_REQ_ID.header, reqId);
        req.headers().set(Headers.HEADER_TENANT_ID.header, "tenantId");
        req.headers().set(CONTENT_LENGTH, 1024 * 2048);
        PubHandler handler = new PubHandler(settingProvider, distClient);
        FullHttpResponse response = handler.handle(reqId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE);
    }

    private DefaultFullHttpRequest buildRequest() {
        return buildRequest(HttpMethod.POST);
    }
}
