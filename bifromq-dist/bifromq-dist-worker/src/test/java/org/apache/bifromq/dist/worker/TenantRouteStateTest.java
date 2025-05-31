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

import static org.apache.bifromq.metrics.TenantMetric.MqttRouteSpaceGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttSharedSubNumGauge;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.function.Supplier;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class TenantRouteStateTest extends MeterTest {

    @Test
    public void testMeterSetupAndDestroy() {
        String tenantId = "tenant" + System.nanoTime();
        assertNoGauge(tenantId, MqttRouteSpaceGauge);
        assertNoGauge(tenantId, MqttSharedSubNumGauge);
        TenantRouteState tenantRouteState = new TenantRouteState(tenantId, () -> 0, "tags", "values");
        assertGauge(tenantId, MqttRouteSpaceGauge);
        assertGauge(tenantId, MqttSharedSubNumGauge);
        tenantRouteState.destroy();
        assertNoGauge(tenantId, MqttRouteSpaceGauge);
        assertNoGauge(tenantId, MqttSharedSubNumGauge);
    }

    @Test
    public void testAdd() {
        String tenantId = "tenant" + System.nanoTime();
        TenantRouteState tenantRouteState = new TenantRouteState(tenantId, () -> 0, "tags", "values");
        assertTrue(tenantRouteState.isNoRoutes());
        tenantRouteState.addNormalRoutes(1);
        tenantRouteState.addSharedRoutes(1);
        assertGaugeValue(tenantId, MqttSharedSubNumGauge, 1);
        tenantRouteState.addSharedRoutes(-1);
        assertGaugeValue(tenantId, MqttSharedSubNumGauge, 0);

        assertFalse(tenantRouteState.isNoRoutes());
        tenantRouteState.addNormalRoutes(-1);
        assertTrue(tenantRouteState.isNoRoutes());
    }

    @Test
    public void testRouteSpace() {
        String tenantId = "tenant" + System.nanoTime();

        Supplier<Number> routeSpaceProvider = Mockito.mock(Supplier.class);
        when(routeSpaceProvider.get()).thenReturn(3);
        TenantRouteState tenantRouteState = new TenantRouteState(tenantId, routeSpaceProvider, "tags", "values");
        assertGaugeValue(tenantId, MqttRouteSpaceGauge, 3);
    }
}
