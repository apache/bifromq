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

package org.apache.bifromq.dist.worker;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.metrics.TenantMetric;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class MeterTest {
    private SimpleMeterRegistry meterRegistry;

    @BeforeMethod
    public void setup() {
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
    }

    @AfterMethod
    public void tearDown() {
        Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
        Metrics.globalRegistry.remove(meterRegistry);
    }

    protected void assertGauge(String tenantId, TenantMetric tenantMetric) {
        Optional<Meter> gauge = getGauge(tenantId, tenantMetric);
        assertTrue(gauge.isPresent());
        assertEquals(gauge.get().getId().getType(), Meter.Type.GAUGE);
        assertEquals(gauge.get().getId().getTag(ITenantMeter.TAG_TENANT_ID), tenantId);
    }

    protected void assertNoGauge(String tenantId, TenantMetric tenantMetric) {
        Optional<Meter> gauge = getGauge(tenantId, tenantMetric);
        gauge.ifPresent(meter -> assertEquals(meter.getId().getTag(ITenantMeter.TAG_TENANT_ID), tenantId));
    }

    protected void assertGaugeValue(String tenantId, TenantMetric tenantMetric, double value) {
        Optional<Meter> meter = getGauge(tenantId, tenantMetric);
        assertTrue(meter.isPresent());
        assertEquals(((Gauge) meter.get()).value(), value);
    }

    protected Optional<Meter> getGauge(String tenantId, TenantMetric tenantMetric) {
        return meterRegistry.getMeters().stream()
            .filter(m -> m.getId().getName().equals(tenantMetric.metricName)
                && tenantId.equals(m.getId().getTag(ITenantMeter.TAG_TENANT_ID))).findFirst();
    }
}
