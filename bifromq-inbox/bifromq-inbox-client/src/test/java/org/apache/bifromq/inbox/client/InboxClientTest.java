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

package org.apache.bifromq.inbox.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.baserpc.client.IRPCClient;
import org.apache.bifromq.inbox.rpc.proto.AttachReply;
import org.apache.bifromq.inbox.rpc.proto.AttachRequest;
import org.apache.bifromq.inbox.rpc.proto.CommitReply;
import org.apache.bifromq.inbox.rpc.proto.CommitRequest;
import org.apache.bifromq.inbox.rpc.proto.DetachReply;
import org.apache.bifromq.inbox.rpc.proto.DetachRequest;
import org.apache.bifromq.inbox.rpc.proto.ExistReply;
import org.apache.bifromq.inbox.rpc.proto.ExistRequest;
import org.apache.bifromq.inbox.rpc.proto.ExpireAllReply;
import org.apache.bifromq.inbox.rpc.proto.ExpireAllRequest;
import org.apache.bifromq.inbox.rpc.proto.SubReply;
import org.apache.bifromq.inbox.rpc.proto.SubRequest;
import org.apache.bifromq.inbox.rpc.proto.UnsubReply;
import org.apache.bifromq.inbox.rpc.proto.UnsubRequest;
import org.apache.bifromq.plugin.subbroker.CheckReply;
import org.apache.bifromq.plugin.subbroker.CheckRequest;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.MatchInfo;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InboxClientTest {
    private AutoCloseable closeable;
    @Mock
    private IRPCClient rpcClient;

    private InboxClient inboxClient;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        inboxClient = new InboxClient(rpcClient);
    }

    @AfterMethod
    @SneakyThrows
    public void tearDown() {
        closeable.close();
    }

    @Test
    public void checkSubscriptionRPCException() {
        CheckRequest checkRequest = CheckRequest.newBuilder()
            .setTenantId("TenantId")
            .setDelivererKey("DelivererKey")
            .addMatchInfo(MatchInfo.newBuilder().build())
            .build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));

        CheckReply reply = inboxClient.check(checkRequest).join();

        verify(rpcClient).invoke(eq(checkRequest.getTenantId()), isNull(), eq(checkRequest), any());

        assertEquals(reply.getCodeCount(), checkRequest.getMatchInfoCount());
        for (int i = 0; i < reply.getCodeCount(); i++) {
            assertEquals(reply.getCode(i), CheckReply.Code.ERROR);
        }
    }

    @Test
    public void commitRPCException() {
        CommitRequest commitRequest = CommitRequest.newBuilder().setTenantId("TenantId").build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        CommitReply reply = inboxClient.commit(commitRequest).join();
        verify(rpcClient).invoke(eq(commitRequest.getTenantId()), isNull(), eq(commitRequest), any());
        assertEquals(reply.getCode(), CommitReply.Code.ERROR);
    }

    @Test
    public void existRPCException() {
        ExistRequest existRequest = ExistRequest.newBuilder().setTenantId("TenantId").build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        ExistReply reply = inboxClient.exist(existRequest).join();
        verify(rpcClient).invoke(eq(existRequest.getTenantId()), isNull(), eq(existRequest), any());
        assertEquals(reply.getCode(), ExistReply.Code.ERROR);
    }

    @Test
    public void attachRPCException() {
        AttachRequest attachRequest = AttachRequest.newBuilder()
            .setClient(ClientInfo.newBuilder()
                .setTenantId("TenantId")
                .build()).build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        AttachReply reply = inboxClient.attach(attachRequest).join();
        verify(rpcClient).invoke(eq(attachRequest.getClient().getTenantId()), isNull(), eq(attachRequest), any());
        assertEquals(reply.getCode(), AttachReply.Code.ERROR);
    }

    @Test
    public void detachRPCException() {
        DetachRequest detachRequest = DetachRequest.newBuilder()
            .setClient(ClientInfo.newBuilder()
                .setTenantId("TenantId")
                .build()).build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        DetachReply reply = inboxClient.detach(detachRequest).join();
        verify(rpcClient).invoke(eq(detachRequest.getClient().getTenantId()), isNull(), eq(detachRequest), any());
        assertEquals(reply.getCode(), DetachReply.Code.ERROR);
    }

    @Test
    public void subRPCException() {
        SubRequest subRequest = SubRequest.newBuilder().setTenantId("TenantId").build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        SubReply reply = inboxClient.sub(subRequest).join();
        verify(rpcClient).invoke(eq(subRequest.getTenantId()), isNull(), eq(subRequest), any());
        assertEquals(reply.getCode(), SubReply.Code.ERROR);
    }

    @Test
    public void unsubRPCException() {
        UnsubRequest unsubRequest = UnsubRequest.newBuilder().setTenantId("TenantId").build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        UnsubReply reply = inboxClient.unsub(unsubRequest).join();
        verify(rpcClient).invoke(eq(unsubRequest.getTenantId()), isNull(), eq(unsubRequest), any());
        assertEquals(reply.getCode(), UnsubReply.Code.ERROR);
    }

    @Test
    public void expireAllRPCException() {
        ExpireAllRequest expireAllRequest = ExpireAllRequest.newBuilder().setTenantId("TenantId").build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        ExpireAllReply reply = inboxClient.expireAll(expireAllRequest).join();
        verify(rpcClient).invoke(eq(expireAllRequest.getTenantId()), isNull(), eq(expireAllRequest), any());
        assertEquals(reply.getCode(), ExpireAllReply.Code.ERROR);
    }
}
