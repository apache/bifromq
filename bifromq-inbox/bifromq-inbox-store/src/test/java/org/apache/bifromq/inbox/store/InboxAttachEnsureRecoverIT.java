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

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.rpc.proto.DeleteReply;
import org.apache.bifromq.inbox.storage.proto.BatchAttachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchDetachReply;
import org.apache.bifromq.inbox.storage.proto.BatchDetachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchExistRequest;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.sessiondict.client.type.OnlineCheckResult;
import org.apache.bifromq.type.ClientInfo;
import org.testng.annotations.Test;

public class InboxAttachEnsureRecoverIT extends InboxStoreTest {

    @Test(groups = "integration")
    public void ensureSchedulesImmediateExpireAfterRestart() throws Exception {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenant-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long inc1 = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();

        when(sessionDictClient.exist(org.mockito.ArgumentMatchers.any()))
            .thenReturn(CompletableFuture.completedFuture(OnlineCheckResult.NOT_EXISTS));
        when(inboxClient.delete(org.mockito.ArgumentMatchers.any()))
            .thenReturn(CompletableFuture.completedFuture(DeleteReply.newBuilder()
                .setReqId(System.nanoTime())
                .setCode(DeleteReply.Code.OK)
                .build()));

        InboxVersion v1 = requestAttach(BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(inc1)
            .setExpirySeconds(3)
            .setClient(client)
            .setNow(now)
            .build())
            .get(0);

        assertEquals(requestDetach(BatchDetachRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setExpirySeconds(3)
            .setVersion(v1)
            .setNow(now)
            .build())
            .get(0), BatchDetachReply.Code.OK);

        restartStoreServer();

        await().forever().until(() -> {
            try {
                requestExist(BatchExistRequest.Params.newBuilder()
                    .setTenantId(tenantId)
                    .setInboxId(inboxId)
                    .setNow(HLC.INST.getPhysical())
                    .build());
                return true;
            } catch (Throwable e) {
                return false;
            }
        });

        Thread.sleep(3500);

        requestAttach(BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(System.nanoTime())
            .setExpirySeconds(3)
            .setClient(client)
            .setNow(HLC.INST.getPhysical())
            .build());

        long expectedInc = v1.getIncarnation();
        long expectedMod = v1.getMod() + 1;
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
            verify(inboxClient, atLeastOnce()).delete(argThat(req -> req.getTenantId().equals(tenantId)
                && req.getInboxId().equals(inboxId)
                && req.getVersion().getIncarnation() == expectedInc
                && req.getVersion().getMod() == expectedMod)));
    }
}
