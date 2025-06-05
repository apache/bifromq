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

package org.apache.bifromq.inbox.server;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.inbox.rpc.proto.AttachReply;
import org.apache.bifromq.inbox.rpc.proto.AttachRequest;
import org.apache.bifromq.inbox.rpc.proto.DetachReply;
import org.apache.bifromq.inbox.rpc.proto.DetachRequest;
import org.apache.bifromq.inbox.rpc.proto.ExistReply;
import org.apache.bifromq.inbox.rpc.proto.ExistRequest;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.type.ClientInfo;
import org.testng.annotations.Test;

public class InboxAdminRPCTest extends InboxServiceTest {

    @Test(groups = "integration")
    public void attachToCreate() {
        long now = System.currentTimeMillis();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill").setDelaySeconds(5).build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        ExistReply existReply = inboxClient.exist(ExistRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setNow(0)
            .build()).join();
        assertEquals(existReply.getReqId(), reqId);
        assertEquals(existReply.getCode(), ExistReply.Code.NO_INBOX);

        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(attachReply.getReqId(), reqId);
        assertEquals(attachReply.getCode(), AttachReply.Code.OK);
        assertTrue(attachReply.hasVersion());
        assertEquals(attachReply.getVersion().getMod(), 0);

        existReply = inboxClient.exist(ExistRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setNow(0)
            .build()).join();
        assertEquals(existReply.getReqId(), reqId);
        assertEquals(existReply.getCode(), ExistReply.Code.EXIST);
    }

    @Test(groups = "integration")
    public void attachToExisting() {
        long now = System.currentTimeMillis();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill").setDelaySeconds(5).build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        ExistReply existReply = inboxClient.exist(ExistRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setNow(0)
            .build()).join();
        assertEquals(existReply.getReqId(), reqId);
        assertEquals(existReply.getCode(), ExistReply.Code.NO_INBOX);

        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(attachReply.getReqId(), reqId);
        assertEquals(attachReply.getCode(), AttachReply.Code.OK);
        assertTrue(attachReply.hasVersion());
        assertEquals(attachReply.getVersion().getMod(), 0);

        AttachReply attachReply1 = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(attachReply1.getCode(), AttachReply.Code.OK);
        assertTrue(attachReply1.hasVersion());
        assertEquals(attachReply1.getVersion().getMod(), 1);
        assertEquals(attachReply.getVersion().getIncarnation(), attachReply1.getVersion().getIncarnation());
    }

    @Test(groups = "integration")
    public void attachAfterExpired() {
        long now = 0;
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        ExistReply existReply = inboxClient.exist(ExistRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setNow(now)
            .build()).join();
        assertEquals(existReply.getReqId(), reqId);
        assertEquals(existReply.getCode(), ExistReply.Code.NO_INBOX);

        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(attachReply.getReqId(), reqId);
        assertEquals(attachReply.getCode(), AttachReply.Code.OK);
        assertTrue(attachReply.hasVersion());
        assertEquals(attachReply.getVersion().getMod(), 0);

        DetachReply detachReply = inboxClient.detach(DetachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setExpirySeconds(1)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(detachReply.getReqId(), reqId);
        assertEquals(detachReply.getCode(), DetachReply.Code.OK);

        AttachReply attachReply1 = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setClient(clientInfo)
            .setNow(now + 100000)
            .build()).join();
        assertEquals(attachReply1.getReqId(), reqId);
        assertEquals(attachReply1.getCode(), AttachReply.Code.OK);
        assertTrue(attachReply1.hasVersion());
        assertEquals(attachReply1.getVersion().getMod(), 0);
        assertTrue(attachReply1.getVersion().getIncarnation() > attachReply.getVersion().getIncarnation());
    }

    @Test(groups = "integration")
    public void attachBeforeExpired() {
        long now = 0;
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        ExistReply existReply = inboxClient.exist(ExistRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setNow(now)
            .build()).join();
        assertEquals(existReply.getReqId(), reqId);
        assertEquals(existReply.getCode(), ExistReply.Code.NO_INBOX);

        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(attachReply.getReqId(), reqId);
        assertEquals(attachReply.getCode(), AttachReply.Code.OK);
        assertTrue(attachReply.hasVersion());
        assertEquals(attachReply.getVersion().getMod(), 0);

        DetachReply detachReply = inboxClient.detach(DetachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setExpirySeconds(5)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(detachReply.getReqId(), reqId);
        assertEquals(detachReply.getCode(), DetachReply.Code.OK);

        AttachReply attachReply1 = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setClient(clientInfo)
            .setNow(now + 1000)
            .build()).join();
        assertEquals(attachReply1.getReqId(), reqId);
        assertEquals(attachReply1.getCode(), AttachReply.Code.OK);
        assertTrue(attachReply1.hasVersion());
        assertEquals(attachReply1.getVersion().getMod(), 2);
        assertEquals(attachReply1.getVersion().getIncarnation(), attachReply.getVersion().getIncarnation());
    }

    @Test(groups = "integration")
    public void detachNoInbox() {
        long now = System.currentTimeMillis();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        DetachReply detachReply = inboxClient.detach(DetachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setVersion(InboxVersion.newBuilder().build())
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(detachReply.getReqId(), reqId);
        assertEquals(detachReply.getCode(), DetachReply.Code.NO_INBOX);
    }

    @Test(groups = "integration")
    public void detachConflict() {
        long now = System.currentTimeMillis();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();

        DetachReply detachReply = inboxClient.detach(DetachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion().toBuilder().setMod(attachReply.getVersion().getMod() + 1).build())
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(detachReply.getReqId(), reqId);
        assertEquals(detachReply.getCode(), DetachReply.Code.CONFLICT);
    }

    @Test(groups = "integration")
    public void detach() {
        long now = System.currentTimeMillis();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill").setDelaySeconds(5).build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();

        DetachReply detachReply = inboxClient.detach(DetachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setExpirySeconds(1)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(detachReply.getReqId(), reqId);
        assertEquals(detachReply.getCode(), DetachReply.Code.OK);

        ExistReply existReply = inboxClient.exist(ExistRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setNow(0)
            .build()).join();
        assertEquals(existReply.getReqId(), reqId);
        assertEquals(existReply.getCode(), ExistReply.Code.EXIST);
    }
}
