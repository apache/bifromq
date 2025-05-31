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

package org.apache.bifromq.sessiondict.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import org.apache.bifromq.baserpc.client.IRPCClient;
import com.google.common.collect.Maps;
import io.reactivex.rxjava3.core.Observable;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.apache.bifromq.sessiondict.rpc.proto.KillAllReply;
import org.apache.bifromq.sessiondict.rpc.proto.KillReply;
import org.apache.bifromq.sessiondict.rpc.proto.ServerRedirection;
import org.apache.bifromq.sessiondict.rpc.proto.SessionDictServiceGrpc;
import org.apache.bifromq.type.ClientInfo;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KillTest {
    @Mock
    IRPCClient rpcClient;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @SneakyThrows
    @AfterMethod
    public void tearDown() {
        closeable.close();
    }

    @Test
    public void testKillTenant() {
        when(rpcClient.serverList()).thenReturn(Observable.just(new HashMap<>() {{
            put("server1", Maps.newHashMap());
            put("server2", Maps.newHashMap());
            put("server3", Maps.newHashMap());
        }}));
        when(rpcClient.invoke(any(), any(), any(), any())).thenReturn(new CompletableFuture<>());
        SessionDictClient client = new SessionDictClient(rpcClient);
        String tenantId = "tenantId";
        ClientInfo killer = ClientInfo.newBuilder().setTenantId(tenantId).setType("abc").build();
        CompletableFuture<KillAllReply> reply = client.killAll(1, tenantId, null, killer,
            ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build());
        assertFalse(reply.isDone());
        ArgumentCaptor<String> serverIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(rpcClient, times(3)).invoke(
            any(),
            serverIdCaptor.capture(),
            argThat(req -> req.getTenantId().equals(tenantId) && !req.hasUserId() && req.getKiller().equals(killer)),
            eq(SessionDictServiceGrpc.getKillAllMethod()));
    }

    @Test
    public void testKillAllFailed() {
        when(rpcClient.serverList()).thenReturn(Observable.just(new HashMap<>() {{
            put("server1", Maps.newHashMap());
            put("server2", Maps.newHashMap());
            put("server3", Maps.newHashMap());
        }}));
        when(rpcClient.invoke(any(), eq("server1"), any(), any())).thenReturn(
            CompletableFuture.completedFuture(KillAllReply.newBuilder()
                .setResult(KillAllReply.Result.ERROR)
                .build()));
        when(rpcClient.invoke(any(), eq("server2"), any(), any())).thenReturn(
            CompletableFuture.completedFuture(KillAllReply.newBuilder()
                .setResult(KillAllReply.Result.OK)
                .build()));
        when(rpcClient.invoke(any(), eq("server3"), any(), any())).thenReturn(
            CompletableFuture.completedFuture(KillAllReply.newBuilder()
                .setResult(KillAllReply.Result.OK)
                .build()));
        SessionDictClient client = new SessionDictClient(rpcClient);
        String tenantId = "tenantId";
        ClientInfo killer = ClientInfo.newBuilder().setTenantId(tenantId).setType("abc").build();
        CompletableFuture<KillAllReply> reply = client.killAll(1, tenantId, null, killer,
            ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build());
        assertEquals(reply.join().getResult(), KillAllReply.Result.ERROR);
    }

    @Test
    public void testKillAllOK() {
        when(rpcClient.serverList()).thenReturn(Observable.just(new HashMap<>() {{
            put("server1", Maps.newHashMap());
            put("server2", Maps.newHashMap());
            put("server3", Maps.newHashMap());
        }}));
        when(rpcClient.invoke(any(), eq("server1"), any(), any())).thenReturn(
            CompletableFuture.completedFuture(KillAllReply.newBuilder()
                .setResult(KillAllReply.Result.OK)
                .build()));
        when(rpcClient.invoke(any(), eq("server2"), any(), any())).thenReturn(
            CompletableFuture.completedFuture(KillAllReply.newBuilder()
                .setResult(KillAllReply.Result.OK)
                .build()));
        when(rpcClient.invoke(any(), eq("server3"), any(), any())).thenReturn(
            CompletableFuture.completedFuture(KillAllReply.newBuilder()
                .setResult(KillAllReply.Result.OK)
                .build()));
        SessionDictClient client = new SessionDictClient(rpcClient);
        String tenantId = "tenantId";
        ClientInfo killer = ClientInfo.newBuilder().setTenantId(tenantId).setType("abc").build();
        CompletableFuture<KillAllReply> reply = client.killAll(1, tenantId, null, killer,
            ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build());
        assertEquals(reply.join().getResult(), KillAllReply.Result.OK);
    }

    @Test
    public void testKillAllException() {
        when(rpcClient.serverList()).thenReturn(Observable.just(new HashMap<>() {{
            put("server1", Maps.newHashMap());
            put("server2", Maps.newHashMap());
            put("server3", Maps.newHashMap());
        }}));
        when(rpcClient.invoke(any(), eq("server1"), any(), any())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("MockedException")));
        when(rpcClient.invoke(any(), eq("server2"), any(), any())).thenReturn(
            CompletableFuture.completedFuture(KillAllReply.newBuilder()
                .setResult(KillAllReply.Result.OK)
                .build()));
        when(rpcClient.invoke(any(), eq("server3"), any(), any())).thenReturn(
            CompletableFuture.completedFuture(KillAllReply.newBuilder()
                .setResult(KillAllReply.Result.OK)
                .build()));
        SessionDictClient client = new SessionDictClient(rpcClient);
        String tenantId = "tenantId";
        ClientInfo killer = ClientInfo.newBuilder().setTenantId(tenantId).setType("abc").build();
        CompletableFuture<KillAllReply> reply = client.killAll(1, tenantId, null, killer,
            ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build());
        assertEquals(reply.join().getResult(), KillAllReply.Result.ERROR);
    }

    @Test
    public void testKillTenantUser() {
        when(rpcClient.serverList()).thenReturn(Observable.just(new HashMap<>() {{
            put("server1", Maps.newHashMap());
            put("server2", Maps.newHashMap());
            put("server3", Maps.newHashMap());
        }}));
        when(rpcClient.invoke(any(), any(), any(), any())).thenReturn(new CompletableFuture<>());
        SessionDictClient client = new SessionDictClient(rpcClient);
        String tenantId = "tenantId";
        String userId = "user1";
        ClientInfo killer = ClientInfo.newBuilder().setTenantId(tenantId).setType("abc").build();
        CompletableFuture<KillAllReply> reply = client.killAll(1, tenantId, userId, killer,
            ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build());
        assertFalse(reply.isDone());
        ArgumentCaptor<String> serverIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(rpcClient, times(3)).invoke(
            any(),
            serverIdCaptor.capture(),
            argThat(req -> req.getTenantId().equals(tenantId)
                && req.getUserId().equals(userId)
                && req.getKiller().equals(killer)),
            eq(SessionDictServiceGrpc.getKillAllMethod()));
    }

    @Test
    public void testKillTenantUserClient() {
        when(rpcClient.serverList()).thenReturn(Observable.just(new HashMap<>() {{
            put("server1", Maps.newHashMap());
            put("server2", Maps.newHashMap());
            put("server3", Maps.newHashMap());
        }}));
        when(rpcClient.invoke(any(), any(), any(), any())).thenReturn(new CompletableFuture<>());
        SessionDictClient client = new SessionDictClient(rpcClient);
        String tenantId = "tenantId";
        String userId = "user1";
        String clientId = "client1";
        ClientInfo killer = ClientInfo.newBuilder().setTenantId(tenantId).setType("abc").build();
        CompletableFuture<KillReply> reply = client.kill(1, tenantId, userId, clientId, killer,
            ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build());
        assertFalse(reply.isDone());
        ArgumentCaptor<String> serverIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(rpcClient).invoke(
            any(),
            serverIdCaptor.capture(),
            argThat(req -> req.getTenantId().equals(tenantId)
                && req.getUserId().equals(userId)
                && req.getClientId().equals(clientId)
                && req.getKiller().equals(killer)),
            eq(SessionDictServiceGrpc.getKillMethod()));
    }

    @Test
    public void testKillTenantUserClientException() {
        when(rpcClient.serverList()).thenReturn(Observable.just(new HashMap<>() {{
            put("server1", Maps.newHashMap());
            put("server2", Maps.newHashMap());
            put("server3", Maps.newHashMap());
        }}));
        when(rpcClient.invoke(any(), any(), any(), any())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("MockedException")));
        SessionDictClient client = new SessionDictClient(rpcClient);
        String tenantId = "tenantId";
        String userId = "user1";
        String clientId = "client1";
        ClientInfo killer = ClientInfo.newBuilder().setTenantId(tenantId).setType("abc").build();
        CompletableFuture<KillReply> reply = client.kill(1, tenantId, userId, clientId, killer,
            ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build());
        assertEquals(reply.join().getResult(), KillReply.Result.ERROR);
    }
}
