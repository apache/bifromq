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

package org.apache.bifromq.inbox.store;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.inbox.rpc.proto.DeleteRequest;
import org.apache.bifromq.inbox.storage.proto.BatchAttachRequest;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.sessiondict.client.type.OnlineCheckResult;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.MQTTClientInfoConstants;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class LoadExistingTest extends InboxStoreTest {
    @Test(groups = "integration")
    public void gcJobAfterRestart() {
        when(sessionDictClient.exist(any())).thenReturn(CompletableFuture.completedFuture(OnlineCheckResult.EXISTS));
        long now = System.currentTimeMillis();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId)
            .putMetadata(MQTTClientInfoConstants.MQTT_USER_ID_KEY, "userId")
            .putMetadata(MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY, "clientId")
            .build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(3)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        restartStoreServer();
        await().until(() -> BoundaryUtil.isValidSplitSet(storeClient.latestEffectiveRouter().keySet()));
        when(sessionDictClient.exist(any())).thenReturn(CompletableFuture.completedFuture(OnlineCheckResult.NOT_EXISTS));
        ArgumentCaptor<DeleteRequest> deleteCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(inboxClient, timeout(10000)).delete(deleteCaptor.capture());
        DeleteRequest request = deleteCaptor.getValue();
        assertEquals(request.getTenantId(), tenantId);
        assertEquals(request.getInboxId(), inboxId);
        assertEquals(request.getVersion(), inboxVersion);
    }
}
