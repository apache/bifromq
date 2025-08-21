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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotSame;

import io.micrometer.core.instrument.Gauge;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.inbox.storage.proto.BatchAttachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchSubRequest;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.sessiondict.client.type.OnlineCheckResult;
import org.apache.bifromq.type.ClientInfo;
import org.testng.annotations.Test;

public class LoadSubStatsTest extends InboxStoreTest {

    @Test(groups = "integration")
    public void collectAfterRestart() {
        when(sessionDictClient.exist(any())).thenReturn(CompletableFuture.completedFuture(OnlineCheckResult.EXISTS));
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
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

        BatchSubRequest.Params subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setMaxTopicFilters(100)
            .setNow(now)
            .build();
        requestSub(subParams);
        Gauge subCountGauge = getSubCountGauge(tenantId);
        Gauge pSessionGauge = getPSessionGauge(tenantId);
        Gauge pSessionSpaceGauge = getPSessionSpaceGauge(tenantId);
        await().until(() -> subCountGauge.value() == 1);
        await().until(() -> pSessionGauge.value() == 1);
//        await().until(() -> pSessionSpaceGauge.value() > 0);

        restartStoreServer();

        await().forever().until(() -> BoundaryUtil.isValidSplitSet(storeClient.latestEffectiveRouter().keySet()));
        Gauge newSubCountGauge = getSubCountGauge(tenantId);
        Gauge newPSessionGauge = getPSessionGauge(tenantId);
        Gauge newPSessionSpaceGauge = getPSessionSpaceGauge(tenantId);
        assertNotSame(subCountGauge, newSubCountGauge);
        assertNotSame(pSessionGauge, newPSessionGauge);
        assertNotSame(pSessionSpaceGauge, newPSessionSpaceGauge);
        await().until(() -> newSubCountGauge.value() == 1);
        await().until(() -> newPSessionGauge.value() == 1);
        await().until(() -> newPSessionSpaceGauge.value() > 0);
    }
}
