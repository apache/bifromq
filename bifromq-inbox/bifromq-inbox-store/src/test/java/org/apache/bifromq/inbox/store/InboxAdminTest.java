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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;

import org.apache.bifromq.plugin.eventcollector.EventType;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.storage.proto.BatchAttachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchDeleteReply;
import org.apache.bifromq.inbox.storage.proto.BatchDeleteRequest;
import org.apache.bifromq.inbox.storage.proto.BatchDetachReply;
import org.apache.bifromq.inbox.storage.proto.BatchDetachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchExistRequest;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.sessiondict.client.type.OnlineCheckResult;
import org.apache.bifromq.type.ClientInfo;
import org.testng.annotations.Test;

public class InboxAdminTest extends InboxStoreTest {
    @Test(groups = "integration")
    public void attachToCreate() {
        when(sessionDictClient.exist(any())).thenReturn(CompletableFuture.completedFuture(OnlineCheckResult.EXISTS));

        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        LWT lwt = LWT.newBuilder().setTopic("lastWill").build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setLwt(lwt)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);
        assertEquals(inboxVersion.getIncarnation(), incarnation);
        assertEquals(inboxVersion.getMod(), 0);
    }

    @Test(groups = "integration")
    public void attachToExisting() {
        when(sessionDictClient.exist(any())).thenReturn(CompletableFuture.completedFuture(OnlineCheckResult.EXISTS));
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        LWT lwt = LWT.newBuilder().setTopic("lastWill").build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setLwt(lwt)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        InboxVersion inboxVersion1 = requestAttach(attachParams).get(0);
        assertEquals(inboxVersion1.getIncarnation(), inboxVersion.getIncarnation());
        assertEquals(inboxVersion1.getMod(), inboxVersion.getMod() + 1);
    }

    @Test(groups = "integration")
    public void attachToCreatePhantomInbox() {
        when(sessionDictClient.exist(any())).thenReturn(CompletableFuture.completedFuture(OnlineCheckResult.EXISTS));
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        LWT lwt = LWT.newBuilder().setTopic("lastWill").build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(0)
            .setLwt(lwt)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);
        assertEquals(inboxVersion.getIncarnation(), 0);
        assertEquals(inboxVersion.getMod(), 0);
    }

    @Test(groups = "integration")
    public void detachNoInbox() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        BatchDetachRequest.Params detachParams = BatchDetachRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(InboxVersion.newBuilder().setIncarnation(0).setMod(0).build())
            .setExpirySeconds(5)
            .setNow(now)
            .build();
        BatchDetachReply.Code code = requestDetach(detachParams).get(0);
        assertEquals(code, BatchDetachReply.Code.NO_INBOX);
    }

    @Test(groups = "integration")
    public void detachConflict() {
        long now = 0;
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

        BatchDetachRequest.Params detachParams = BatchDetachRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion.toBuilder().setMod(inboxVersion.getMod() + 1).build())
            .setExpirySeconds(5)
            .setNow(now)
            .build();
        BatchDetachReply.Code code = requestDetach(detachParams).get(0);
        assertEquals(code, BatchDetachReply.Code.CONFLICT);
    }

    @Test(groups = "integration")
    public void detach() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        LWT lwt = LWT.newBuilder().setTopic("lastWill").build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setLwt(lwt)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        now = Duration.ofMillis(now).plusSeconds(5).toMillis();
        BatchDetachRequest.Params detachParams = BatchDetachRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setExpirySeconds(1)
            .setNow(now)
            .build();
        BatchDetachReply.Code code = requestDetach(detachParams).get(0);
        assertSame(code, BatchDetachReply.Code.OK);
    }

    @Test(groups = "integration")
    public void detachLatest() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        LWT lwt = LWT.newBuilder().setTopic("lastWill").build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setLwt(lwt)
            .setClient(client)
            .setNow(now)
            .build();
        requestAttach(attachParams);
        BatchDetachRequest.Params detachParams = BatchDetachRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setExpirySeconds(1)
            .setNow(Duration.ofMillis(now).plusSeconds(5).toMillis())
            .build();
        BatchDetachReply.Code code = requestDetach(detachParams).get(0);
        assertSame(code, BatchDetachReply.Code.OK);
    }

    @Test(groups = "integration")
    public void deleteNoInbox() {
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        BatchDeleteRequest.Params deleteParams = BatchDeleteRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(InboxVersion.newBuilder().build())
            .build();
        BatchDeleteReply.Result result = requestDelete(deleteParams).get(0);
        assertEquals(result.getCode(), BatchDeleteReply.Code.NO_INBOX);
    }

    @Test(groups = "integration")
    public void deleteConflict() {
        long now = 0;
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

        BatchDeleteRequest.Params deleteParams = BatchDeleteRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion.toBuilder().setMod(inboxVersion.getMod() + 1).build())
            .build();
        BatchDeleteReply.Result result = requestDelete(deleteParams).get(0);
        assertEquals(result.getCode(), BatchDeleteReply.Code.CONFLICT);
    }

    @Test(groups = "integration")
    public void delete() {
        long now = 0;
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
        reset(eventCollector);
        BatchDeleteRequest.Params deleteParams = BatchDeleteRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .build();
        BatchDeleteReply.Result result = requestDelete(deleteParams).get(0);
        assertEquals(result.getCode(), BatchDeleteReply.Code.OK);

        BatchExistRequest.Params getParams = BatchExistRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setNow(now)
            .build();
        assertFalse(requestExist(getParams).get(0));
        verify(eventCollector, timeout(4000).times(1))
            .report(argThat(e -> e.type() == EventType.MQTT_SESSION_STOP));
    }
}
