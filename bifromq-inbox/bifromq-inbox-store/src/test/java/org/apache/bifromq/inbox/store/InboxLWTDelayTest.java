/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.inbox.store;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.protobuf.ByteString;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.storage.proto.BatchAttachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchDetachReply;
import org.apache.bifromq.inbox.storage.proto.BatchDetachRequest;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.sessiondict.client.type.OnlineCheckResult;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.testng.annotations.Test;

public class InboxLWTDelayTest extends InboxStoreTest {
    @Test(groups = "integration")
    public void sendLWTTaskTimeout() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        when(sessionDictClient.exist(any())).thenReturn(CompletableFuture.completedFuture(OnlineCheckResult.EXISTS));
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
            .setExpirySeconds(3)
            .setVersion(inboxVersion)
            .setNow(now)
            .build();
        assertEquals(requestDetach(detachedParams).get(0), BatchDetachReply.Code.OK);
        verify(inboxClient, timeout(4000).times(1))
            .sendLWT(argThat(r -> r.getTenantId().equals(tenantId)
                && r.getInboxId().equals(inboxId)
                && r.getVersion().getIncarnation() == inboxVersion.getIncarnation()
                && r.getVersion().getMod() == inboxVersion.getMod() + 1));
    }
}
