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

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertNotSame;

import org.apache.bifromq.basekv.utils.BoundaryUtil;
import io.micrometer.core.instrument.Gauge;
import java.time.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LoadMetadataTest extends RetainStoreTest {
    private String tenantId;

    @BeforeMethod(alwaysRun = true)
    private void reset() {
        tenantId = "tenantA-" + System.nanoTime();
    }

    @Test(groups = "integration")
    public void testLoadMetadata() {
        requestRetain(tenantId, message("/a", "hello"));
        requestRetain(tenantId, message("/b", "hello"));
        await().atMost(Duration.ofSeconds(30)).until(() -> getSpaceUsageGauge(tenantId).value() > 0);
        await().atMost(Duration.ofSeconds(30)).until(() -> getRetainCountGauge(tenantId).value() == 2);

        Gauge spaceUsageGauge = getSpaceUsageGauge(tenantId);
        Gauge retainCountGauge = getRetainCountGauge(tenantId);

        restartStoreServer();
        await().until(() -> BoundaryUtil.isValidSplitSet(storeClient.latestEffectiveRouter().keySet()));

        Gauge newSpaceUsageGauge = getSpaceUsageGauge(tenantId);
        Gauge newRetainCountGauge = getRetainCountGauge(tenantId);
        assertNotSame(spaceUsageGauge, newSpaceUsageGauge);
        assertNotSame(retainCountGauge, newRetainCountGauge);
        await().atMost(Duration.ofSeconds(30)).until(() -> newSpaceUsageGauge.value() > 0);
        await().atMost(Duration.ofSeconds(30)).until(() -> newRetainCountGauge.value() == 2);
    }
}
