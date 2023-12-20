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

import static com.baidu.bifromq.apiserver.Headers.HEADER_CLIENT_ID;
import static com.baidu.bifromq.apiserver.Headers.HEADER_CLIENT_META_PREFIX;
import static com.baidu.bifromq.apiserver.Headers.HEADER_CLIENT_TYPE;
import static com.baidu.bifromq.apiserver.Headers.HEADER_USER_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.baidu.bifromq.sessiondict.rpc.proto.KillReply;
import com.baidu.bifromq.type.ClientInfo;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class HTTPKickHandlerTest extends AbstractHTTPRequestHandlerTest<HTTPKickHandler> {
    @Mock
    private ISessionDictClient sessionDictClient;

    @Override
    protected Class<HTTPKickHandler> handlerClass() {
        return HTTPKickHandler.class;
    }

    @Test
    public void missingHeaders() {
        DefaultFullHttpRequest req = buildRequest();
        HTTPKickHandler handler = new HTTPKickHandler(sessionDictClient);
        assertThrows(() -> handler.handle(123, "fakeTenant", req).join());
    }

    @Test
    public void kick() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "admin_user");
        req.headers().set(HEADER_CLIENT_ID.header, "admin_client");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPKickHandler handler = new HTTPKickHandler(sessionDictClient);
        handler.handle(reqId, tenantId, req);
        ArgumentCaptor<Long> reqIdCap = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<String> tenantIdCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> userIdCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> clientIdCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ClientInfo> killerCap = ArgumentCaptor.forClass(ClientInfo.class);
        verify(sessionDictClient).kill(reqIdCap.capture(), tenantIdCap.capture(),
                userIdCap.capture(), clientIdCap.capture(), killerCap.capture());
        assertEquals(reqIdCap.getValue(), reqId);
        assertEquals(userIdCap.getValue(), req.headers().get(HEADER_USER_ID.header));
        assertEquals(clientIdCap.getValue(), req.headers().get(HEADER_CLIENT_ID.header));
        ClientInfo killer = killerCap.getValue();
        assertEquals(killer.getTenantId(), tenantId);
        assertEquals(killer.getType(), req.headers().get(HEADER_CLIENT_TYPE.header));
        assertEquals(killer.getMetadataCount(), 1);
        assertEquals(killer.getMetadataMap().get("age"), "4");
    }

    @Test
    public void kickSucceed() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "admin_user");
        req.headers().set(HEADER_CLIENT_ID.header, "admin_client");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPKickHandler handler = new HTTPKickHandler(sessionDictClient);

        when(sessionDictClient.kill(anyLong(), anyString(), anyString(), anyString(), any()))
            .thenReturn(CompletableFuture.completedFuture(KillReply.newBuilder()
                .setResult(KillReply.Result.OK).build()));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.OK);
        assertEquals(response.content().readableBytes(), 0);
    }

    @Test
    public void kickNothing() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "admin_user");
        req.headers().set(HEADER_CLIENT_ID.header, "admin_client");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPKickHandler handler = new HTTPKickHandler(sessionDictClient);

        when(sessionDictClient.kill(anyLong(), anyString(), anyString(), anyString(), any()))
            .thenReturn(CompletableFuture.completedFuture(KillReply.newBuilder()
                .setResult(KillReply.Result.ERROR).build()));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.NOT_FOUND);
        assertEquals(response.content().readableBytes(), 0);
    }

    private DefaultFullHttpRequest buildRequest() {
        return buildRequest(HttpMethod.DELETE);
    }
}
