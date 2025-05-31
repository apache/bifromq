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
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Gauge;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.storage.proto.BatchAttachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchDeleteRequest;
import org.apache.bifromq.inbox.storage.proto.BatchSubRequest;
import org.apache.bifromq.inbox.storage.proto.BatchUnsubRequest;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.metrics.TenantMetric;
import org.apache.bifromq.sessiondict.client.type.OnlineCheckResult;
import org.apache.bifromq.type.ClientInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SubStatsTest extends InboxStoreTest {
    @BeforeMethod(alwaysRun = true)
    @Override
    public void beforeCastStart(Method method) {
        super.beforeCastStart(method);
        when(sessionDictClient.exist(any())).thenReturn(CompletableFuture.completedFuture(OnlineCheckResult.EXISTS));
    }

    @Test(groups = "integration")
    public void collectAfterSub() {
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
        await().until(() -> subCountGauge.value() == 1);
        await().until(() -> pSessionGauge.value() == 1);
    }

    @Test(groups = "integration")
    public void collectAfterSubExisting() {
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
        requestSub(subParams);

        Gauge subCountGauge = getSubCountGauge(tenantId);
        Gauge pSessionGauge = getPSessionGauge(tenantId);
        await().until(() -> subCountGauge.value() == 1);
        await().until(() -> pSessionGauge.value() == 1);
    }

    @Test(groups = "integration")
    public void collectAfterUnSub() {
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

        BatchUnsubRequest.Params unsubParams = BatchUnsubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        requestUnsub(unsubParams);

        Gauge subCountGauge = getSubCountGauge(tenantId);
        Gauge pSessionGauge = getPSessionGauge(tenantId);
        await().until(() -> subCountGauge.value() == 0);
        await().until(() -> pSessionGauge.value() == 1);
    }

    @Test(groups = "integration")
    public void collectAfterUnSubNonExists() {
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

        BatchUnsubRequest.Params unsubParams = BatchUnsubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        requestUnsub(unsubParams);
        // unsub again
        requestUnsub(unsubParams);

        Gauge subCountGauge = getSubCountGauge(tenantId);
        Gauge pSessionGauge = getPSessionGauge(tenantId);
        await().until(() -> subCountGauge.value() == 0);
        await().until(() -> pSessionGauge.value() == 1);
    }

    @Test(groups = "integration")
    public void collectAfterDelete() {
        long now = 0;
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

        BatchDeleteRequest.Params deleteParams = BatchDeleteRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .build();
        requestDelete(deleteParams);

        assertNoGauge(tenantId, TenantMetric.MqttPersistentSessionSpaceGauge);
        assertNoGauge(tenantId, TenantMetric.MqttPersistentSessionNumGauge);
        assertNoGauge(tenantId, TenantMetric.MqttPersistentSubCountGauge);
    }
}
