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

package org.apache.bifromq.retain.store;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.intersect;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.retain.store.schema.KVSchemaUtil.tenantBeginKey;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import org.apache.bifromq.basekv.store.api.IKVReader;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.metrics.TenantMetric;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TenantStatsTest {
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
    public void metricValue() {
        String tenantId = "tenant" + System.nanoTime();
        when(reader.boundary()).thenReturn(FULL_BOUNDARY);
        when(reader.size(
            eq(intersect(FULL_BOUNDARY, toBoundary(tenantBeginKey(tenantId), upperBound(tenantBeginKey(tenantId)))))))
            .thenReturn(10L);
        TenantStats tenantStats = new TenantStats(tenantId, reader);
        tenantStats.incrementTopicCount(10);
        assertGaugeValue(tenantId, TenantMetric.MqttRetainSpaceGauge, 10);
        assertNoGauge(tenantId, TenantMetric.MqttRetainNumGauge);
        tenantStats.toggleMetering(true);
        assertGaugeValue(tenantId, TenantMetric.MqttRetainNumGauge, 10);
        tenantStats.toggleMetering(false);
        assertNoGauge(tenantId, TenantMetric.MqttRetainNumGauge);
    }

    protected void assertGaugeValue(String tenantId, TenantMetric tenantMetric, double value) {
        Optional<Meter> meter = getGauge(tenantId, tenantMetric);
        assertTrue(meter.isPresent());
        assertEquals(((Gauge) meter.get()).value(), value);
    }

    protected void assertNoGauge(String tenantId, TenantMetric tenantMetric) {
        await().untilAsserted(() -> {
            Optional<Meter> gauge = getGauge(tenantId, tenantMetric);
            assertTrue(gauge.isEmpty());
        });
    }

    protected Optional<Meter> getGauge(String tenantId, TenantMetric tenantMetric) {
        return meterRegistry.getMeters().stream()
            .filter(m -> m.getId().getName().equals(tenantMetric.metricName)
                && tenantId.equals(m.getId().getTag(ITenantMeter.TAG_TENANT_ID))).findFirst();
    }

}
