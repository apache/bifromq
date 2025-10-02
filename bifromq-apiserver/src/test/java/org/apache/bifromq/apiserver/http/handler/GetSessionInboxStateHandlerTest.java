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

import static org.apache.bifromq.apiserver.Headers.HEADER_CLIENT_ID;
import static org.apache.bifromq.apiserver.Headers.HEADER_USER_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.google.protobuf.UnsafeByteOperations;
import com.google.protobuf.util.JsonFormat;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;
import org.apache.bifromq.sessiondict.rpc.proto.GetInboxStateReply;
import org.apache.bifromq.type.InboxState;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class GetSessionInboxStateHandlerTest extends AbstractHTTPRequestHandlerTest<GetSessionInboxStateHandler> {
    @Mock
    private ISessionDictClient sessionDictClient;

    @Override
    protected Class<GetSessionInboxStateHandler> handlerClass() {
        return GetSessionInboxStateHandler.class;
    }

    @Test
    public void missingHeaders() {
        DefaultFullHttpRequest req = buildRequest();
        GetSessionInboxStateHandler handler = new GetSessionInboxStateHandler(settingProvider, sessionDictClient);
        assertThrows(() -> handler.handle(123, "tenant", req).join());
    }

    @Test
    public void okResponse() throws Exception {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "user");
        req.headers().set(HEADER_CLIENT_ID.header, "client");
        long reqId = 1;
        String tenantId = "tenant";
        InboxState expectedState = InboxState.newBuilder().setUndeliveredMsgCount(10).build();
        when(sessionDictClient.inboxState(any()))
            .thenReturn(CompletableFuture.completedFuture(GetInboxStateReply.newBuilder()
                .setReqId(reqId)
                .setCode(GetInboxStateReply.Code.OK)
                .setState(expectedState)
                .build()));

        GetSessionInboxStateHandler handler = new GetSessionInboxStateHandler(settingProvider, sessionDictClient);
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();

        verify(sessionDictClient).inboxState(argThat(r -> r.getReqId() == reqId
            && r.getTenantId().equals(tenantId)
            && r.getUserId().equals("user")
            && r.getClientId().equals("client")));
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.OK);
        assertEquals(response.headers().get("Content-Type"), "application/json");
        InboxState.Builder builder = InboxState.newBuilder();
        String json = UnsafeByteOperations.unsafeWrap(response.content().duplicate().nioBuffer()).toStringUtf8();
        JsonFormat.parser().merge(json, builder);
        assertEquals(builder.build(), expectedState);
    }

    @Test
    public void noInboxResponse() {
        DefaultFullHttpRequest req = buildRequestWithHeaders();
        when(sessionDictClient.inboxState(any()))
            .thenReturn(CompletableFuture.completedFuture(GetInboxStateReply.newBuilder()
                .setCode(GetInboxStateReply.Code.NO_INBOX)
                .build()));

        FullHttpResponse response =
            new GetSessionInboxStateHandler(settingProvider, sessionDictClient).handle(1, "tenant", req).join();

        assertEquals(response.status(), HttpResponseStatus.NOT_FOUND);
        assertEquals(response.content().toString(StandardCharsets.UTF_8), GetInboxStateReply.Code.NO_INBOX.name());
    }

    @Test
    public void tryLaterResponse() {
        DefaultFullHttpRequest req = buildRequestWithHeaders();
        when(sessionDictClient.inboxState(any()))
            .thenReturn(CompletableFuture.completedFuture(GetInboxStateReply.newBuilder()
                .setCode(GetInboxStateReply.Code.TRY_LATER)
                .build()));

        FullHttpResponse response =
            new GetSessionInboxStateHandler(settingProvider, sessionDictClient).handle(1, "tenant", req).join();

        assertEquals(response.status(), HttpResponseStatus.CONFLICT);
        assertEquals(response.content().toString(StandardCharsets.UTF_8), GetInboxStateReply.Code.TRY_LATER.name());
    }

    @Test
    public void backPressureResponse() {
        DefaultFullHttpRequest req = buildRequestWithHeaders();
        when(sessionDictClient.inboxState(any()))
            .thenReturn(CompletableFuture.completedFuture(GetInboxStateReply.newBuilder()
                .setCode(GetInboxStateReply.Code.BACK_PRESSURE_REJECTED)
                .build()));

        FullHttpResponse response =
            new GetSessionInboxStateHandler(settingProvider, sessionDictClient).handle(1, "tenant", req).join();

        assertEquals(response.status(), HttpResponseStatus.TOO_MANY_REQUESTS);
        assertEquals(response.content().toString(StandardCharsets.UTF_8),
            GetInboxStateReply.Code.BACK_PRESSURE_REJECTED.name());
    }

    @Test
    public void errorResponse() {
        DefaultFullHttpRequest req = buildRequestWithHeaders();
        when(sessionDictClient.inboxState(any()))
            .thenReturn(CompletableFuture.completedFuture(GetInboxStateReply.newBuilder()
                .setCode(GetInboxStateReply.Code.ERROR)
                .build()));

        FullHttpResponse response =
            new GetSessionInboxStateHandler(settingProvider, sessionDictClient).handle(1, "tenant", req).join();

        assertEquals(response.status(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        assertEquals(response.content().readableBytes(), 0);
    }

    private DefaultFullHttpRequest buildRequestWithHeaders() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "user");
        req.headers().set(HEADER_CLIENT_ID.header, "client");
        return req;
    }

    private DefaultFullHttpRequest buildRequest() {
        return buildRequest(HttpMethod.GET);
    }
}
