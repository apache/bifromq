/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.retain.store;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.metrics.TenantMetric;
import org.apache.bifromq.type.TopicMessage;
import io.micrometer.core.instrument.Gauge;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class GCTest extends RetainStoreTest {
    private String tenantId;

    @BeforeMethod(alwaysRun = true)
    public void reset() {
        tenantId = "tenantA-" + System.nanoTime();
    }

    @Test(groups = "integration")
    public void gc() {
        String topic = "/a";
        TopicMessage message = message(topic, "hello", 0, 1);

        requestRetain(tenantId, message);

        requestGC(0, null, null);
        assertTrue(requestMatch(tenantId, 0L, topic, 1).getMessagesCount() > 0);

        requestGC(1100L, null, null);

        assertEquals(requestMatch(tenantId, 1100L, topic, 1).getMessagesCount(), 0);
    }

    @Test(groups = "integration")
    public void gcTenant() {
        String tenantId1 = "tenantB-" + System.nanoTime();
        String topic = "/a";
        requestRetain(tenantId, message(topic, "hello", 0, 1));
        requestRetain(tenantId1, message(topic, "hello", 0, 1));

        requestGC(1100L, tenantId, null);

        assertNoGauge(tenantId, TenantMetric.MqttRetainNumGauge);
        assertNoGauge(tenantId, TenantMetric.MqttRetainSpaceGauge);

        getRetainCountGauge(tenantId1);
        getSpaceUsageGauge(tenantId1);
    }

    @Test(groups = "integration")
    public void gcTenantWithExpirySeconds() {
        requestRetain(tenantId, message("/a", "hello", 0, 2));
        requestRetain(tenantId, message("/b", "hello", Duration.ofSeconds(1).toMillis() << 16, 3));
        Gauge retainCountGauge = getRetainCountGauge(tenantId);
        await().until(() -> retainCountGauge.value() == 2);
        requestGC(1100L, tenantId, 1);
        await().until(() -> retainCountGauge.value() == 1);
    }
}
