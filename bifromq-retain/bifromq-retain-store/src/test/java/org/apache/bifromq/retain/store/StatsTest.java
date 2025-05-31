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

import static org.apache.bifromq.metrics.TenantMetric.MqttRetainSpaceGauge;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.retain.rpc.proto.RetainResult;
import org.apache.bifromq.type.TopicMessage;
import io.micrometer.core.instrument.Meter;
import lombok.SneakyThrows;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StatsTest extends RetainStoreTest {
    private String tenantId;

    @BeforeMethod(alwaysRun = true)
    private void reset() {
        tenantId = "tenantA-" + System.nanoTime();
    }

    @SneakyThrows
    @Test(groups = "integration")
    public void reportTenantMetrics() {
        String topic = "/a/b/c";
        TopicMessage message = message(topic, "hello");

        RetainResult.Code reply = requestRetain(tenantId, message);
        assertEquals(reply, RetainResult.Code.RETAINED);

        await().until(() -> {
            for (Meter meter : meterRegistry.getMeters()) {
                if (meter.getId().getType() == Meter.Type.GAUGE &&
                    meter.getId().getName().equals(MqttRetainSpaceGauge.metricName)) {
                    return true;
                }
            }
            return false;
        });
    }
}
