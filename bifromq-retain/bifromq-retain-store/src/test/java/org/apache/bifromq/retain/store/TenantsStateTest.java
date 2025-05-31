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

package org.apache.bifromq.retain.store;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.store.api.IKVReader;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.metrics.TenantMetric;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TenantsStateTest {
    String tenantId = "tenant-" + System.nanoTime();
    private SimpleMeterRegistry meterRegistry;
    private IKVReader reader;


    @BeforeMethod
    public void setup() {
        reader = mock(IKVReader.class);
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
    }

    @AfterMethod
    public void tearDown() {
        Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
        Metrics.globalRegistry.remove(meterRegistry);
    }

    @Test
    public void increaseTopicCount() {
        TenantsState tenantsState = new TenantsState(reader);
        assertNoGauge(tenantId, TenantMetric.MqttRetainNumGauge);
        assertNoGauge(tenantId, TenantMetric.MqttRetainSpaceGauge);
        tenantsState.increaseTopicCount(tenantId, 1);
        assertGauge(tenantId, TenantMetric.MqttRetainNumGauge);
        assertGauge(tenantId, TenantMetric.MqttRetainSpaceGauge);
    }

    @Test
    public void decreaseTopicCount() {
        TenantsState tenantsState = new TenantsState(reader);
        tenantsState.increaseTopicCount(tenantId, 1);
        assertGauge(tenantId, TenantMetric.MqttRetainNumGauge);
        assertGauge(tenantId, TenantMetric.MqttRetainSpaceGauge);

        tenantsState.increaseTopicCount(tenantId, -1);

        assertNoGauge(tenantId, TenantMetric.MqttRetainNumGauge);
        assertNoGauge(tenantId, TenantMetric.MqttRetainSpaceGauge);
    }

    @Test
    public void destroy() {
        TenantsState tenantsState = new TenantsState(reader);
        tenantsState.increaseTopicCount(tenantId, 1);
        assertGauge(tenantId, TenantMetric.MqttRetainNumGauge);
        assertGauge(tenantId, TenantMetric.MqttRetainSpaceGauge);

        tenantsState.destroy();
        assertNoGauge(tenantId, TenantMetric.MqttRetainNumGauge);
        assertNoGauge(tenantId, TenantMetric.MqttRetainSpaceGauge);
    }

    private void assertGauge(String tenantId, TenantMetric tenantMetric) {
        Optional<Meter> gauge = getGauge(tenantId, tenantMetric);
        assertTrue(gauge.isPresent());
        assertEquals(gauge.get().getId().getType(), Meter.Type.GAUGE);
        assertEquals(gauge.get().getId().getTag(ITenantMeter.TAG_TENANT_ID), tenantId);
    }

    private void assertNoGauge(String tenantId, TenantMetric tenantMetric) {
        Optional<Meter> gauge = getGauge(tenantId, tenantMetric);
        gauge.ifPresent(meter -> assertEquals(meter.getId().getTag(ITenantMeter.TAG_TENANT_ID), tenantId));
    }

    private Optional<Meter> getGauge(String tenantId, TenantMetric tenantMetric) {
        return meterRegistry.getMeters().stream()
            .filter(m -> m.getId().getName().equals(tenantMetric.metricName)
                && tenantId.equals(m.getId().getTag(ITenantMeter.TAG_TENANT_ID))).findFirst();
    }
}