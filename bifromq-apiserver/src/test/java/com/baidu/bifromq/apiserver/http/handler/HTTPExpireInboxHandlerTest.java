/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.apiserver.http.handler;

import static com.baidu.bifromq.apiserver.Headers.HEADER_EXPIRY_SECONDS;
import static com.baidu.bifromq.apiserver.Headers.HEADER_TENANT_ID;
import static com.baidu.bifromq.inbox.rpc.proto.ExpireAllReply.Code.ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.ExpireAllReply;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class HTTPExpireInboxHandlerTest extends AbstractHTTPRequestHandlerTest<HTTPKickHandler> {
    @Mock
    private IInboxClient inboxClient;

    @Override
    protected Class<HTTPKickHandler> handlerClass() {
        return HTTPKickHandler.class;
    }

    @Test
    public void missingHeaders() {
        DefaultFullHttpRequest req = buildRequest();
        HTTPExpireInboxHandler handler = new HTTPExpireInboxHandler(inboxClient);
        assertThrows(() -> handler.handle(123, "fakeTenant", req).join());
    }

    @Test
    public void expire() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_TENANT_ID.header, "tenant_id");
        req.headers().set(HEADER_EXPIRY_SECONDS.header, "10");
        long reqId = 123;
        String tenantId = "bifromq_dev";
        HTTPExpireInboxHandler handler = new HTTPExpireInboxHandler(inboxClient);
        when(inboxClient.expireAll(any())).thenReturn(CompletableFuture.completedFuture(
            ExpireAllReply.newBuilder()
                .setCode(ExpireAllReply.Code.OK)
                .build()));

        CompletableFuture<FullHttpResponse> responseCompletableFuture = handler.handle(reqId, tenantId, req);
        verify(inboxClient).expireAll(
            argThat(r -> r.getReqId() == reqId && r.getTenantId().equals(tenantId) && r.getExpirySeconds() == 10));
        assertEquals(responseCompletableFuture.join().status(), OK);
    }

    @Test
    public void expireError() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_TENANT_ID.header, "tenant_id");
        req.headers().set(HEADER_EXPIRY_SECONDS.header, "10");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPExpireInboxHandler handler = new HTTPExpireInboxHandler(inboxClient);
        when(inboxClient.expireAll(any())).thenReturn(CompletableFuture.completedFuture(
            ExpireAllReply.newBuilder()
                .setCode(ERROR)
                .build()));
        CompletableFuture<FullHttpResponse> responseCompletableFuture = handler.handle(reqId, tenantId, req);
        FullHttpResponse httpResponse = responseCompletableFuture.join();
        assertEquals(httpResponse.status(), INTERNAL_SERVER_ERROR);
    }

    private DefaultFullHttpRequest buildRequest() {
        return buildRequest(HttpMethod.DELETE);
    }
}
