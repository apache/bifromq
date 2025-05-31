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

package org.apache.bifromq.mqtt.session;

import static org.apache.bifromq.metrics.TenantMetric.MqttSessionWorkingMemoryGauge;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.metrics.TenantMetric;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class TenantGaugeTest {
    private SimpleMeterRegistry meterRegistry;
    private TenantGauge tenantMemGauge;

    @BeforeMethod
    public void setup() {
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
        tenantMemGauge = new TenantGauge(MqttSessionWorkingMemoryGauge);
    }

    @AfterMethod
    public void tearDown() {
        Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
        Metrics.globalRegistry.remove(meterRegistry);
    }

    @Test
    public void testGaugingFromMultiThread() {
        String tenantId = "tenantId" + System.nanoTime();
        TenantGauge tenantMemGauge = new TenantGauge(MqttSessionWorkingMemoryGauge);
        CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            AtomicLong gauge1 = tenantMemGauge.get(tenantId);
            gauge1.addAndGet(100);
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
        AtomicLong gauge = tenantMemGauge.get(tenantId);
        gauge.addAndGet(100);
        assertMemGauge(tenantId);
        await().until(() -> {
            try {
                assertMemGaugeValue(tenantId, 200);
                latch.countDown();
                return true;
            } catch (AssertionError e) {
                return false;
            }
        });
    }

    @Test
    public void testStopGauging() {
        String tenantId = "tenantId" + System.nanoTime();
        AtomicLong gauge = tenantMemGauge.get(tenantId);
        gauge.addAndGet(100);
        assertMemGaugeValue(tenantId, 100);
        gauge = null;
        await().until(() -> {
            System.gc();
            try {
                assertMemGaugeValue(tenantId, 0);
                assertNoGauge(tenantId);
                return true;
            } catch (AssertionError e) {
                return false;
            }
        });
    }

    @Test
    public void testStopGaugingFromMultiThread() {
        String tenantId = "tenantId" + System.nanoTime();
        TenantGauge tenantMemGauge = new TenantGauge(MqttSessionWorkingMemoryGauge);
        CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            AtomicLong gauge1 = tenantMemGauge.get(tenantId);
            gauge1.addAndGet(100);
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
        AtomicLong gauge = tenantMemGauge.get(tenantId);
        gauge.addAndGet(100);
        assertMemGauge(tenantId);
        await().until(() -> {
            try {
                assertMemGaugeValue(tenantId, 200);
                latch.countDown();
                return true;
            } catch (AssertionError e) {
                return false;
            }
        });
        await().until(() -> {
            try {
                System.gc();
                assertMemGaugeValue(tenantId, 100);
                return true;
            } catch (AssertionError e) {
                return false;
            }
        });
        gauge = null;
        await().until(() -> {
            try {
                System.gc();
                assertMemGaugeValue(tenantId, 0);
                assertNoGauge(tenantId);
                return true;
            } catch (AssertionError e) {
                return false;
            }
        });
    }

    protected void assertMemGauge(String tenantId) {
        Optional<Meter> gauge = getGauge(tenantId, MqttSessionWorkingMemoryGauge);
        assertTrue(gauge.isPresent());
        assertEquals(gauge.get().getId().getType(), Meter.Type.GAUGE);
    }

    protected void assertNoGauge(String tenantId) {
        Optional<Meter> gauge = getGauge(tenantId, MqttSessionWorkingMemoryGauge);
        assertTrue(gauge.isEmpty());
    }

    protected void assertMemGaugeValue(String tenantId, double value) {
        Optional<Meter> meter = getGauge(tenantId, MqttSessionWorkingMemoryGauge);
        assertTrue(meter.isPresent());
        assertEquals(((Gauge) meter.get()).value(), value);
    }

    protected Optional<Meter> getGauge(String tenantId, TenantMetric tenantMetric) {
        return meterRegistry.getMeters().stream()
            .filter(m -> m.getId().getName().equals(tenantMetric.metricName)
                && tenantId.equals(m.getId().getTag(ITenantMeter.TAG_TENANT_ID))).findFirst();
    }
}
