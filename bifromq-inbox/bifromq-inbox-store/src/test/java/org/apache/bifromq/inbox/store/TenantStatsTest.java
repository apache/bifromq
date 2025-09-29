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

import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentSessionNumGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentSessionSpaceGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentSubCountGauge;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.metrics.ITenantMeter;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TenantStatsTest {
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

    @Test
    public void testGaugeRegistrationAndValues() {
        String tenantId = "tenant-" + System.nanoTime();
        AtomicReference<Integer> usedSpace = new AtomicReference<>(100);
        TenantStats stats = new TenantStats(tenantId, usedSpace::get, "clusterId", "c1", "storeId", "s1", "rangeId", "r1");

        // promote to leader to register session/sub gauges; space gauge comes from supplier
        stats.toggleMetering(true);
        assertEquals(findGaugeValue(MqttPersistentSessionNumGauge.metricName, tenantId), 0.0, 0.0001);
        assertEquals(findGaugeValue(MqttPersistentSubCountGauge.metricName, tenantId), 0.0, 0.0001);
        assertEquals(findGaugeValue(MqttPersistentSessionSpaceGauge.metricName, tenantId), 100.0, 0.0001);

        stats.addSessionCount(2);
        stats.addSessionCount(4);
        assertEquals(findGaugeValue(MqttPersistentSessionNumGauge.metricName, tenantId), 6.0, 0.0001);

        stats.addSubCount(5);
        stats.addSubCount(1);
        assertEquals(findGaugeValue(MqttPersistentSubCountGauge.metricName, tenantId), 6.0, 0.0001);

        // update space supplier
        usedSpace.set(4321);
        assertEquals(findGaugeValue(MqttPersistentSessionSpaceGauge.metricName, tenantId), 4321.0, 0.0001);

        // destroy should unregister all gauges
        stats.destroy();
        assertNull(findGauge(MqttPersistentSessionNumGauge.metricName, tenantId));
        assertNull(findGauge(MqttPersistentSubCountGauge.metricName, tenantId));
        assertNull(findGauge(MqttPersistentSessionSpaceGauge.metricName, tenantId));
    }

    private Double findGaugeValue(String metricName, String tenantId) {
        Meter m = findGauge(metricName, tenantId);
        assertNotNull(m);
        return meterRegistry.find(metricName).tag(ITenantMeter.TAG_TENANT_ID, tenantId).gauge().value();
    }

    private Meter findGauge(String metricName, String tenantId) {
        return meterRegistry.getMeters().stream()
            .filter(m -> m.getId().getName().equals(metricName)
                && m.getId().getType() == Meter.Type.GAUGE
                && tenantId.equals(m.getId().getTag(ITenantMeter.TAG_TENANT_ID)))
            .findFirst().orElse(null);
    }

    @Test
    public void testTogglePromotionAndDemotion() {
        String tenantId = "tenant-toggle-" + System.nanoTime();
        AtomicReference<Integer> usedSpace = new AtomicReference<>(10);
        TenantStats stats = new TenantStats(tenantId, usedSpace::get, "clusterId", "c1", "storeId", "s1", "rangeId", "r1");

        // before promotion, only space gauge should exist
        assertNotNull(findGauge(MqttPersistentSessionSpaceGauge.metricName, tenantId));
        assertNull(findGauge(MqttPersistentSessionNumGauge.metricName, tenantId));
        assertNull(findGauge(MqttPersistentSubCountGauge.metricName, tenantId));

        stats.addSessionCount(3);
        stats.addSubCount(5);

        // promote registers session/sub gauges reflecting counts
        stats.toggleMetering(true);
        assertEquals(findGaugeValue(MqttPersistentSessionNumGauge.metricName, tenantId), 3.0, 0.0001);
        assertEquals(findGaugeValue(MqttPersistentSubCountGauge.metricName, tenantId), 5.0, 0.0001);

        // demote unregisters session/sub gauges, but space gauge remains
        stats.toggleMetering(false);
        assertNull(findGauge(MqttPersistentSessionNumGauge.metricName, tenantId));
        assertNull(findGauge(MqttPersistentSubCountGauge.metricName, tenantId));
        assertNotNull(findGauge(MqttPersistentSessionSpaceGauge.metricName, tenantId));
    }

    @Test
    public void testToggleIdempotentAndRePromotion() {
        String tenantId = "tenant-toggle-idem-" + System.nanoTime();
        AtomicReference<Integer> usedSpace = new AtomicReference<>(10);
        TenantStats stats = new TenantStats(tenantId, usedSpace::get, "clusterId", "c1", "storeId", "s1", "rangeId", "r1");

        stats.addSessionCount(2);
        stats.addSubCount(7);

        // double promotion should be idempotent
        stats.toggleMetering(true);
        stats.toggleMetering(true);
        assertEquals(findGaugeValue(MqttPersistentSessionNumGauge.metricName, tenantId), 2.0, 0.0001);
        assertEquals(findGaugeValue(MqttPersistentSubCountGauge.metricName, tenantId), 7.0, 0.0001);

        // demote then promote again should restore gauges
        stats.toggleMetering(false);
        assertNull(findGauge(MqttPersistentSessionNumGauge.metricName, tenantId));
        assertNull(findGauge(MqttPersistentSubCountGauge.metricName, tenantId));
        stats.toggleMetering(true);
        assertEquals(findGaugeValue(MqttPersistentSessionNumGauge.metricName, tenantId), 2.0, 0.0001);
        assertEquals(findGaugeValue(MqttPersistentSubCountGauge.metricName, tenantId), 7.0, 0.0001);
    }
}
