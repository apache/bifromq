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

package org.apache.bifromq.apiserver.http.handler;

import static org.apache.bifromq.apiserver.Headers.HEADER_EXPIRY_SECONDS;
import static org.apache.bifromq.apiserver.Headers.HEADER_TENANT_ID;
import static org.apache.bifromq.retain.rpc.proto.ExpireAllReply.Result.ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import org.apache.bifromq.retain.client.IRetainClient;
import org.apache.bifromq.retain.rpc.proto.ExpireAllReply;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class ExpireRetainHandlerTest extends AbstractHTTPRequestHandlerTest<ExpireRetainHandler> {
    @Mock
    private IRetainClient retainClient;

    @Override
    protected Class<ExpireRetainHandler> handlerClass() {
        return ExpireRetainHandler.class;
    }

    @Test
    public void missingHeaders() {
        DefaultFullHttpRequest req = buildRequest();
        ExpireRetainHandler handler = new ExpireRetainHandler(settingProvider, retainClient);
        assertThrows(() -> handler.handle(123, "fakeTenant", req).join());
    }

    @Test
    public void expire() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_TENANT_ID.header, "tenant_id");
        req.headers().set(HEADER_EXPIRY_SECONDS.header, "10");
        long reqId = 123;
        String tenantId = "bifromq_dev";
        ExpireRetainHandler handler = new ExpireRetainHandler(settingProvider, retainClient);
        when(retainClient.expireAll(any())).thenReturn(CompletableFuture.completedFuture(
            ExpireAllReply.newBuilder()
                .setResult(ExpireAllReply.Result.OK)
                .build()));

        CompletableFuture<FullHttpResponse> responseCompletableFuture = handler.handle(reqId, tenantId, req);
        verify(retainClient).expireAll(
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

        ExpireRetainHandler handler = new ExpireRetainHandler(settingProvider, retainClient);
        when(retainClient.expireAll(any())).thenReturn(CompletableFuture.completedFuture(
            ExpireAllReply.newBuilder()
                .setResult(ERROR)
                .build()));
        CompletableFuture<FullHttpResponse> responseCompletableFuture = handler.handle(reqId, tenantId, req);
        FullHttpResponse httpResponse = responseCompletableFuture.join();
        assertEquals(httpResponse.status(), TOO_MANY_REQUESTS);
    }

    private DefaultFullHttpRequest buildRequest() {
        return buildRequest(HttpMethod.DELETE);
    }
}
