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

import static org.apache.bifromq.metrics.TenantMeter.TAG_TENANT_ID;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertTrue;

import io.micrometer.core.instrument.Metrics;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class TenantMeterTest {
    @Test
    public void get() throws InterruptedException {
        String tenantId = "testing_traffic";
        ITenantMeter meter = ITenantMeter.get(tenantId);
        meter.recordCount(TenantMetric.MqttConnectCount);
        assertTrue(Metrics.globalRegistry.getMeters().stream()
            .anyMatch(m -> tenantId.equals(m.getId().getTag(TAG_TENANT_ID))));
        meter = null;
        System.gc();
        TenantMeterCache.cleanUp();
        System.gc();
        Thread.sleep(100);
        await().until(() -> {
            Thread.sleep(100);
            return Metrics.globalRegistry.getMeters().stream()
                .noneMatch(m -> tenantId.equals(m.getId().getTag(TAG_TENANT_ID)));
        });
        TenantMeterCache.cleanUp();
        log.info("get: success");
    }
}
