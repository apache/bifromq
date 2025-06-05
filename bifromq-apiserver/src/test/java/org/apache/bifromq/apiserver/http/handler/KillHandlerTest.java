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
import static org.apache.bifromq.apiserver.Headers.HEADER_CLIENT_META_PREFIX;
import static org.apache.bifromq.apiserver.Headers.HEADER_CLIENT_TYPE;
import static org.apache.bifromq.apiserver.Headers.HEADER_SERVER_REDIRECT;
import static org.apache.bifromq.apiserver.Headers.HEADER_SERVER_REFERENCE;
import static org.apache.bifromq.apiserver.Headers.HEADER_USER_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;
import org.apache.bifromq.sessiondict.rpc.proto.KillReply;
import org.apache.bifromq.sessiondict.rpc.proto.ServerRedirection;
import org.apache.bifromq.type.ClientInfo;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class KillHandlerTest extends AbstractHTTPRequestHandlerTest<KillHandler> {
    @Mock
    private ISessionDictClient sessionDictClient;

    @Override
    protected Class<KillHandler> handlerClass() {
        return KillHandler.class;
    }

    @Test
    public void missingHeaders() {
        DefaultFullHttpRequest req = buildRequest();
        KillHandler handler = new KillHandler(settingProvider, sessionDictClient);
        assertThrows(() -> handler.handle(123, "fakeTenant", req).join());
    }

    @Test
    public void kill() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "admin_user");
        req.headers().set(HEADER_CLIENT_ID.header, "admin_client");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        when(sessionDictClient.kill(anyLong(), anyString(), anyString(), anyString(), any(), any()))
            .thenReturn(new CompletableFuture<>());
        KillHandler handler = new KillHandler(settingProvider, sessionDictClient);
        handler.handle(reqId, tenantId, req);
        ArgumentCaptor<ClientInfo> killerCap = ArgumentCaptor.forClass(ClientInfo.class);
        verify(sessionDictClient).kill(
            eq(reqId),
            eq(tenantId),
            eq(req.headers().get(HEADER_USER_ID.header)),
            eq(req.headers().get(HEADER_CLIENT_ID.header)),
            killerCap.capture(),
            argThat(serverRedirect -> serverRedirect.getType() == ServerRedirection.Type.NO_MOVE));
        ClientInfo killer = killerCap.getValue();
        assertEquals(killer.getTenantId(), tenantId);
        assertEquals(killer.getType(), req.headers().get(HEADER_CLIENT_TYPE.header));
        assertEquals(killer.getMetadataCount(), 1);
        assertEquals(killer.getMetadataMap().get("age"), "4");
    }

    @Test
    public void killWithMove() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "admin_user");
        req.headers().set(HEADER_CLIENT_ID.header, "admin_client");
        req.headers().set(HEADER_SERVER_REDIRECT.header, "move");
        req.headers().set(HEADER_SERVER_REFERENCE.header, "newServer");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";
        when(sessionDictClient.kill(anyLong(), anyString(), anyString(), anyString(), any(), any()))
            .thenReturn(new CompletableFuture<>());
        KillHandler handler = new KillHandler(settingProvider, sessionDictClient);
        handler.handle(reqId, tenantId, req);
        ArgumentCaptor<ClientInfo> killerCap = ArgumentCaptor.forClass(ClientInfo.class);
        verify(sessionDictClient).kill(
            eq(reqId),
            eq(tenantId),
            eq(req.headers().get(HEADER_USER_ID.header)),
            eq(req.headers().get(HEADER_CLIENT_ID.header)),
            killerCap.capture(),
            argThat(serverRedirect -> serverRedirect.getType() == ServerRedirection.Type.PERMANENT_MOVE
                && serverRedirect.getServerReference().equals("newServer")));
        ClientInfo killer = killerCap.getValue();
        assertEquals(killer.getTenantId(), tenantId);
        assertEquals(killer.getType(), req.headers().get(HEADER_CLIENT_TYPE.header));
        assertEquals(killer.getMetadataCount(), 1);
        assertEquals(killer.getMetadataMap().get("age"), "4");
    }

    @Test
    public void killWithTempUse() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "admin_user");
        req.headers().set(HEADER_CLIENT_ID.header, "admin_client");
        req.headers().set(HEADER_SERVER_REDIRECT.header, "temp_use");
        req.headers().set(HEADER_SERVER_REFERENCE.header, "newServer");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";
        when(sessionDictClient.kill(anyLong(), anyString(), anyString(), anyString(), any(), any()))
            .thenReturn(new CompletableFuture<>());
        KillHandler handler = new KillHandler(settingProvider, sessionDictClient);
        handler.handle(reqId, tenantId, req);
        ArgumentCaptor<ClientInfo> killerCap = ArgumentCaptor.forClass(ClientInfo.class);
        verify(sessionDictClient).kill(
            eq(reqId),
            eq(tenantId),
            eq(req.headers().get(HEADER_USER_ID.header)),
            eq(req.headers().get(HEADER_CLIENT_ID.header)),
            killerCap.capture(),
            argThat(serverRedirect -> serverRedirect.getType() == ServerRedirection.Type.TEMPORARY_MOVE
                && serverRedirect.getServerReference().equals("newServer")));
        ClientInfo killer = killerCap.getValue();
        assertEquals(killer.getTenantId(), tenantId);
        assertEquals(killer.getType(), req.headers().get(HEADER_CLIENT_TYPE.header));
        assertEquals(killer.getMetadataCount(), 1);
        assertEquals(killer.getMetadataMap().get("age"), "4");
    }

    @Test
    public void killUser() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "admin_user");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";
        when(sessionDictClient.killAll(anyLong(), anyString(), anyString(), any(), any()))
            .thenReturn(new CompletableFuture<>());
        KillHandler handler = new KillHandler(settingProvider, sessionDictClient);
        handler.handle(reqId, tenantId, req);
        ArgumentCaptor<ClientInfo> killerCap = ArgumentCaptor.forClass(ClientInfo.class);
        verify(sessionDictClient).killAll(eq(reqId), eq(tenantId),
            eq(req.headers().get(HEADER_USER_ID.header)), killerCap.capture(),
            argThat(serverRedirect -> serverRedirect.getType() == ServerRedirection.Type.NO_MOVE));
        ClientInfo killer = killerCap.getValue();
        assertEquals(killer.getTenantId(), tenantId);
        assertEquals(killer.getType(), req.headers().get(HEADER_CLIENT_TYPE.header));
        assertEquals(killer.getMetadataCount(), 1);
        assertEquals(killer.getMetadataMap().get("age"), "4");
    }

    @Test
    public void killTenant() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";
        when(sessionDictClient.killAll(anyLong(), anyString(), isNull(), any(), any()))
            .thenReturn(new CompletableFuture<>());
        KillHandler handler = new KillHandler(settingProvider, sessionDictClient);
        handler.handle(reqId, tenantId, req);
        ArgumentCaptor<ClientInfo> killerCap = ArgumentCaptor.forClass(ClientInfo.class);
        verify(sessionDictClient).killAll(eq(reqId), eq(tenantId), isNull(), killerCap.capture(),
            argThat(serverRedirect -> serverRedirect.getType() == ServerRedirection.Type.NO_MOVE));
        ClientInfo killer = killerCap.getValue();
        assertEquals(killer.getTenantId(), tenantId);
        assertEquals(killer.getType(), req.headers().get(HEADER_CLIENT_TYPE.header));
        assertEquals(killer.getMetadataCount(), 1);
        assertEquals(killer.getMetadataMap().get("age"), "4");
    }

    @Test
    public void killSucceed() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "admin_user");
        req.headers().set(HEADER_CLIENT_ID.header, "admin_client");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        KillHandler handler = new KillHandler(settingProvider, sessionDictClient);

        when(sessionDictClient.kill(anyLong(), anyString(), anyString(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(KillReply.newBuilder()
                .setResult(KillReply.Result.OK).build()));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.OK);
        assertEquals(response.content().readableBytes(), 0);
    }

    @Test
    public void killNothing() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "admin_user");
        req.headers().set(HEADER_CLIENT_ID.header, "admin_client");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        KillHandler handler = new KillHandler(settingProvider, sessionDictClient);

        when(sessionDictClient.kill(anyLong(), anyString(), anyString(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(KillReply.newBuilder()
                .setResult(KillReply.Result.ERROR).build()));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.NOT_FOUND);
        assertEquals(response.content().readableBytes(), 0);
    }

    @Test
    public void invalidServerRedirect() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "admin_user");
        req.headers().set(HEADER_CLIENT_ID.header, "admin_client");
        req.headers().set(HEADER_SERVER_REDIRECT.header, "invalid");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        KillHandler handler = new KillHandler(settingProvider, sessionDictClient);
        handler.handle(reqId, tenantId, req);
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.BAD_REQUEST);
        assertTrue(response.content().readableBytes() > 0);
    }

    @Test
    public void tooLongServerReference() {
        String longServerReference = new String(new char[65536]).replace('\0', 'a');
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "admin_user");
        req.headers().set(HEADER_CLIENT_ID.header, "admin_client");
        req.headers().set(HEADER_SERVER_REDIRECT.header, "move");
        req.headers().set(HEADER_SERVER_REFERENCE.header, longServerReference);
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        KillHandler handler = new KillHandler(settingProvider, sessionDictClient);
        handler.handle(reqId, tenantId, req);
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.BAD_REQUEST);
        assertTrue(response.content().readableBytes() > 0);
    }

    private DefaultFullHttpRequest buildRequest() {
        return buildRequest(HttpMethod.DELETE);
    }
}
