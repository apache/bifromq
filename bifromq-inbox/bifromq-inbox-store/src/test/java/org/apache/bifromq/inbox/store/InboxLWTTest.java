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

package org.apache.bifromq.inbox.store;

import static org.apache.bifromq.dist.client.PubResult.OK;
import static org.apache.bifromq.dist.client.PubResult.TRY_LATER;
import static org.apache.bifromq.plugin.settingprovider.Setting.RetainEnabled;
import static org.apache.bifromq.retain.rpc.proto.RetainReply.Result.RETAINED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.storage.proto.BatchAttachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchDetachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchSendLWTReply;
import org.apache.bifromq.inbox.storage.proto.BatchSendLWTRequest;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.retain.rpc.proto.RetainReply;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import com.google.protobuf.ByteString;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;

public class InboxLWTTest extends InboxStoreTest {
    @Test(groups = "integration")
    public void sendLWTNoInbox() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        BatchSendLWTRequest.Params sendLWTParams = BatchSendLWTRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(InboxVersion.newBuilder().build())
            .setNow(now)
            .build();

        assertEquals(requestSendLWT(sendLWTParams).get(0), BatchSendLWTReply.Code.NO_INBOX);
    }

    @Test(groups = "integration")
    public void sendLWTConflict() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        LWT lwt = LWT.newBuilder()
            .setTopic("willTopic")
            .setDelaySeconds(2)
            .setMessage(Message.newBuilder()
                .setPayload(ByteString.copyFromUtf8("willMessage"))
                .build())
            .build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setClient(client)
            .setLwt(lwt)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        BatchDetachRequest.Params detachedParams = BatchDetachRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setExpirySeconds(10)
            .setVersion(inboxVersion)
            .setNow(now)
            .build();
        requestDetach(detachedParams);

        BatchSendLWTRequest.Params sendLWTParams = BatchSendLWTRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion.toBuilder().setMod(inboxVersion.getMod() + 3).build())
            .setNow(now)
            .build();

        assertEquals(requestSendLWT(sendLWTParams).get(0), BatchSendLWTReply.Code.CONFLICT);
    }

    @Test(groups = "integration")
    public void sendLWTNoDetachError() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        LWT lwt = LWT.newBuilder()
            .setTopic("willTopic")
            .setDelaySeconds(2)
            .setMessage(Message.newBuilder()
                .setPayload(ByteString.copyFromUtf8("willMessage"))
                .build())
            .build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setClient(client)
            .setLwt(lwt)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        BatchSendLWTRequest.Params sendLWTParams = BatchSendLWTRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setNow(now)
            .build();

        assertEquals(requestSendLWT(sendLWTParams).get(0), BatchSendLWTReply.Code.ERROR);
    }

    @Test(groups = "integration")
    public void sendLWTNoLWTError() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        BatchSendLWTRequest.Params sendLWTParams = BatchSendLWTRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setNow(now)
            .build();

        assertEquals(requestSendLWT(sendLWTParams).get(0), BatchSendLWTReply.Code.ERROR);
    }

    @Test(groups = "integration")
    public void sendLWT() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        LWT lwt = LWT.newBuilder()
            .setTopic("willTopic")
            .setDelaySeconds(2)
            .setMessage(Message.newBuilder()
                .setPayload(ByteString.copyFromUtf8("willMessage"))
                .build())
            .build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setClient(client)
            .setLwt(lwt)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        BatchDetachRequest.Params detachedParams = BatchDetachRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setExpirySeconds(10)
            .setVersion(inboxVersion)
            .setNow(now)
            .build();
        requestDetach(detachedParams);

        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(OK));

        BatchSendLWTRequest.Params sendLWTParams = BatchSendLWTRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion.toBuilder().setMod(inboxVersion.getMod() + 1).build())
            .setNow(now)
            .build();

        assertEquals(requestSendLWT(sendLWTParams).get(0), BatchSendLWTReply.Code.OK);
    }

    @Test(groups = "integration")
    public void sendLWTDistTryLater() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenant-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        LWT lwt = LWT.newBuilder()
            .setTopic("t")
            .setDelaySeconds(1)
            .setMessage(Message.newBuilder().setPayload(ByteString.copyFromUtf8("p")).setIsRetain(true).build())
            .build();
        InboxVersion inboxVersion = requestAttach(BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setClient(client)
            .setLwt(lwt)
            .setNow(now)
            .build()
        ).get(0);

        requestDetach(BatchDetachRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setExpirySeconds(10)
            .setVersion(inboxVersion)
            .setNow(now)
            .build()
        );

        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(TRY_LATER));
        when(settingProvider.provide(eq(RetainEnabled), eq(tenantId))).thenReturn(true);
        when(retainClient.retain(anyLong(), any(), any(), any(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(RetainReply.newBuilder().setResult(RETAINED).build()));

        BatchSendLWTRequest.Params sendP = BatchSendLWTRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(InboxVersion.newBuilder()
                .setIncarnation(incarnation)
                .setMod(inboxVersion.getMod() + 1)
                .build())
            .setNow(now)
            .build();
        assertEquals(requestSendLWT(sendP).get(0), BatchSendLWTReply.Code.TRY_LATER);
    }

    @Test(groups = "integration")
    public void sendLWTRetainTryLater() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenant-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        LWT lwt = LWT.newBuilder()
            .setTopic("t")
            .setDelaySeconds(1)
            .setMessage(Message.newBuilder().setPayload(ByteString.copyFromUtf8("p")).setIsRetain(true).build())
            .build();
        InboxVersion inboxVersion = requestAttach(BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setClient(client)
            .setLwt(lwt)
            .setNow(now)
            .build()
        ).get(0);
        requestDetach(BatchDetachRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setExpirySeconds(10)
            .setVersion(inboxVersion)
            .setNow(now)
            .build()
        );

        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(OK));
        when(settingProvider.provide(eq(RetainEnabled), eq(tenantId)))
            .thenReturn(true);
        when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        when(retainClient.retain(anyLong(), any(), any(), any(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(
                RetainReply.newBuilder().setResult(RetainReply.Result.TRY_LATER).build()));

        BatchSendLWTRequest.Params sendP = BatchSendLWTRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(InboxVersion.newBuilder()
                .setIncarnation(incarnation)
                .setMod(inboxVersion.getMod() + 1)
                .build())
            .setNow(now)
            .build();
        assertEquals(requestSendLWT(sendP).get(0), BatchSendLWTReply.Code.TRY_LATER);
    }
}
