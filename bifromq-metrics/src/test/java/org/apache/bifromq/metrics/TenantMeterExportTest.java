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

package org.apache.bifromq.metrics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

public class TenantMeterExportTest {
    @Test
    public void exportsTenantMetricNamesUnitsAndLabels() {
        String tenantId = "prometheus-compat";
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Metrics.addRegistry(registry);
        try {
            ITenantMeter meter = ITenantMeter.get(tenantId);
            meter.recordCount(TenantMetric.MqttConnectCount, 2);
            meter.timer(TenantMetric.MqttChannelLatency).record(Duration.ofMillis(25));
            meter.recordSummary(TenantMetric.MqttIngressBytes, 42);
            ITenantMeter.gauging(tenantId, TenantMetric.MqttConnectionGauge, () -> 3);
            AtomicLong hits = new AtomicLong(7);
            ITenantMeter.counting(tenantId, TenantMetric.MqttRouteCacheHitCount, hits, AtomicLong::doubleValue);

            String scrape = registry.scrape();
            assertEquals(sample(scrape, "mqtt_connect_count_total", tenantId), 2.0);
            assertEquals(sample(scrape, "mqtt_channel_latency_seconds_count", tenantId), 1.0);
            assertEquals(sample(scrape, "mqtt_channel_latency_seconds_sum", tenantId), 0.025, 0.000001);
            assertEquals(sample(scrape, "mqtt_ingress_bytes_count", tenantId), 1.0);
            assertEquals(sample(scrape, "mqtt_ingress_bytes_sum", tenantId), 42.0);
            assertEquals(sample(scrape, "mqtt_connection_num_gauge", tenantId), 3.0);
            assertEquals(sample(scrape, "mqtt_route_cache_hit_count_total", tenantId), 7.0);

            ITenantMeter.stopGauging(tenantId, TenantMetric.MqttConnectionGauge);
            ITenantMeter.stopCounting(tenantId, TenantMetric.MqttRouteCacheHitCount);
            scrape = registry.scrape();
            assertFalse(scrape.contains("mqtt_connection_num_gauge{"));
            assertFalse(scrape.contains("mqtt_route_cache_hit_count_total{"));
            assertTrue(scrape.contains("mqtt_connect_count_total{"));
        } finally {
            ITenantMeter.stopGauging(tenantId, TenantMetric.MqttConnectionGauge);
            ITenantMeter.stopCounting(tenantId, TenantMetric.MqttRouteCacheHitCount);
            for (Meter meter : Metrics.globalRegistry.getMeters()) {
                if (tenantId.equals(meter.getId().getTag(ITenantMeter.TAG_TENANT_ID))) {
                    Metrics.globalRegistry.remove(meter);
                }
            }
            Metrics.removeRegistry(registry);
            registry.close();
        }
    }

    private static double sample(String scrape, String name, String tenantId) {
        return scrape.lines()
            .filter(line -> line.startsWith(name + "{") && line.contains("tenantId=\"" + tenantId + "\""))
            .mapToDouble(line -> Double.parseDouble(line.substring(line.indexOf('}') + 2)))
            .findFirst().orElseThrow(() -> new AssertionError("Missing sample: " + name));
    }
}
