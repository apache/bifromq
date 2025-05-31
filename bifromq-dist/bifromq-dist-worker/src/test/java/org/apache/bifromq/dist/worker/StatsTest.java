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

package org.apache.bifromq.dist.worker;

import static org.apache.bifromq.metrics.TenantMetric.MqttRouteSpaceGauge;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.Meter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class StatsTest extends DistWorkerTest {
    @SneakyThrows
    @Test(groups = "integration")
    public void reportRangeMetrics() {
        match(tenantA, "/a/b/c", MqttBroker, "inbox1", "server1");

        match(tenantB, "/#", InboxService, "inbox2", "server2");

        await().until(() -> {
            for (Meter meter : meterRegistry.getMeters()) {
                if (meter.getId().getType() == Meter.Type.GAUGE &&
                    meter.getId().getName().equals(MqttRouteSpaceGauge.metricName)) {
                    return true;
                }
            }
            return false;
        });
    }
}
