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

package org.apache.bifromq.dist.worker;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.metrics.TenantMetric.MqttRouteNumGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttRouteSpaceGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttSharedSubNumGauge;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Supplier;
import lombok.SneakyThrows;
import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TenantsStatesTest extends MeterTest {
    @Mock
    private Supplier<IKVCloseableReader> readerSupplier;
    @Mock
    private IKVCloseableReader reader;
    @Mock
    private IKVIterator iterator;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        super.setup();
        closeable = MockitoAnnotations.openMocks(this);
        when(readerSupplier.get()).thenReturn(reader);
        when(reader.boundary()).thenReturn(FULL_BOUNDARY);
        when(reader.iterator()).thenReturn(iterator);
    }

    @SneakyThrows
    @AfterMethod
    public void tearDown() {
        closeable.close();
        super.tearDown();
    }

    @Test
    public void incShareRoute() {
        when(reader.size(any())).thenReturn(1L);
        String tenantId = "tenant" + System.nanoTime();
        TenantsStats tenantsState = new TenantsStats(readerSupplier);
        tenantsState.incSharedRoutes(tenantId);
        tenantsState.toggleMetering(true);
        assertGaugeValue(tenantId, MqttSharedSubNumGauge, 1);
    }

    @Test
    public void testRemove() {
        when(reader.size(any())).thenReturn(1L);
        String tenantId = "tenant" + System.nanoTime();
        TenantsStats tenantsState = new TenantsStats(readerSupplier);
        tenantsState.incSharedRoutes(tenantId);
        tenantsState.incNormalRoutes(tenantId);
        tenantsState.toggleMetering(true);
        assertGaugeValue(tenantId, MqttRouteNumGauge, 1);
        assertGaugeValue(tenantId, MqttSharedSubNumGauge, 1);
        assertGauge(tenantId, MqttRouteSpaceGauge);

        tenantsState.decSharedRoutes(tenantId);
        assertGaugeValue(tenantId, MqttSharedSubNumGauge, 0);
        assertGauge(tenantId, MqttRouteSpaceGauge);
        tenantsState.decNormalRoutes(tenantId);

        assertNoGauge(tenantId, MqttRouteNumGauge);
        assertNoGauge(tenantId, MqttRouteSpaceGauge);
        assertNoGauge(tenantId, MqttSharedSubNumGauge);
    }

    @Test
    public void testReset() {
        when(reader.size(any())).thenReturn(1L);
        String tenantId = "tenant" + System.nanoTime();
        TenantsStats tenantsState = new TenantsStats(readerSupplier);
        tenantsState.incNormalRoutes(tenantId);
        tenantsState.toggleMetering(true);
        assertGauge(tenantId, MqttRouteSpaceGauge);
        assertGauge(tenantId, MqttRouteNumGauge);
        assertGauge(tenantId, MqttSharedSubNumGauge);

        tenantsState.reset();
        assertNoGauge(tenantId, MqttRouteSpaceGauge);
        assertNoGauge(tenantId, MqttRouteNumGauge);
        assertNoGauge(tenantId, MqttSharedSubNumGauge);
    }

    @Test
    public void testClose() {
        when(reader.size(any())).thenReturn(1L);
        String tenantId = "tenant" + System.nanoTime();
        TenantsStats tenantsState = new TenantsStats(readerSupplier);
        tenantsState.incNormalRoutes(tenantId);
        tenantsState.toggleMetering(true);
        assertGauge(tenantId, MqttRouteSpaceGauge);
        assertGauge(tenantId, MqttRouteNumGauge);
        assertGauge(tenantId, MqttSharedSubNumGauge);

        tenantsState.close();
        assertNoGauge(tenantId, MqttRouteSpaceGauge);
        assertNoGauge(tenantId, MqttRouteNumGauge);
        assertNoGauge(tenantId, MqttSharedSubNumGauge);
        verify(reader).close();
    }

    @Test
    public void testTogglePromotionRegistersGauges() {
        when(reader.size(any())).thenReturn(1L);
        String tenantId = "tenantToggleProm-" + System.nanoTime();
        TenantsStats tenantsState = new TenantsStats(readerSupplier);

        // create tenant stats with some routes in follower state
        tenantsState.incNormalRoutes(tenantId, 2);
        tenantsState.incSharedRoutes(tenantId, 3);

        // before promotion, only space gauge should exist; route gauges should be absent
        assertGauge(tenantId, MqttRouteSpaceGauge);
        assertNoGauge(tenantId, MqttRouteNumGauge);
        assertNoGauge(tenantId, MqttSharedSubNumGauge);

        // promote to leader registers gauges and reflects current counts
        tenantsState.toggleMetering(true);
        assertGaugeValue(tenantId, MqttRouteNumGauge, 2);
        assertGaugeValue(tenantId, MqttSharedSubNumGauge, 3);
    }

    @Test
    public void testToggleDemotionUnregistersRouteGauges() {
        when(reader.size(any())).thenReturn(1L);
        String tenantId = "tenantToggleDem-" + System.nanoTime();
        TenantsStats tenantsState = new TenantsStats(readerSupplier);

        tenantsState.incNormalRoutes(tenantId, 1);
        tenantsState.incSharedRoutes(tenantId, 1);
        tenantsState.toggleMetering(true);
        assertGauge(tenantId, MqttRouteNumGauge);
        assertGauge(tenantId, MqttSharedSubNumGauge);
        assertGauge(tenantId, MqttRouteSpaceGauge);

        // demote to follower removes route gauges but keeps space gauge
        tenantsState.toggleMetering(false);
        assertNoGauge(tenantId, MqttRouteNumGauge);
        assertNoGauge(tenantId, MqttSharedSubNumGauge);
        assertGauge(tenantId, MqttRouteSpaceGauge);
    }

    @Test
    public void testToggleIdempotentAndRePromotion() {
        when(reader.size(any())).thenReturn(1L);
        String tenantId = "tenantToggleIdem-" + System.nanoTime();
        TenantsStats tenantsState = new TenantsStats(readerSupplier);

        tenantsState.incNormalRoutes(tenantId, 4);
        tenantsState.incSharedRoutes(tenantId, 5);

        // promote twice should not duplicate gauges
        tenantsState.toggleMetering(true);
        tenantsState.toggleMetering(true);
        assertGaugeValue(tenantId, MqttRouteNumGauge, 4);
        assertGaugeValue(tenantId, MqttSharedSubNumGauge, 5);

        // demote then promote again should restore gauges with correct values
        tenantsState.toggleMetering(false);
        assertNoGauge(tenantId, MqttRouteNumGauge);
        assertNoGauge(tenantId, MqttSharedSubNumGauge);
        tenantsState.toggleMetering(true);
        assertGaugeValue(tenantId, MqttRouteNumGauge, 4);
        assertGaugeValue(tenantId, MqttSharedSubNumGauge, 5);
    }

    @Test
    public void testToggleNoopWhenAlreadyFollower() {
        when(reader.size(any())).thenReturn(1L);
        String tenantId = "tenantToggleNoop-" + System.nanoTime();
        TenantsStats tenantsState = new TenantsStats(readerSupplier);

        tenantsState.incNormalRoutes(tenantId, 1);
        tenantsState.incSharedRoutes(tenantId, 1);
        // already follower, demote again is no-op
        tenantsState.toggleMetering(false);
        assertNoGauge(tenantId, MqttRouteNumGauge);
        assertNoGauge(tenantId, MqttSharedSubNumGauge);
        assertGauge(tenantId, MqttRouteSpaceGauge);
    }

    @Test
    public void testToggleBroadcastToMultipleTenants() {
        when(reader.size(any())).thenReturn(1L);
        String tenantA = "tenantA-" + System.nanoTime();
        String tenantB = "tenantB-" + System.nanoTime();
        TenantsStats tenantsState = new TenantsStats(readerSupplier);

        tenantsState.incNormalRoutes(tenantA, 2);
        tenantsState.incSharedRoutes(tenantA, 1);
        tenantsState.incNormalRoutes(tenantB, 3);
        tenantsState.incSharedRoutes(tenantB, 4);

        // promote once should affect all tenants
        tenantsState.toggleMetering(true);
        assertGaugeValue(tenantA, MqttRouteNumGauge, 2);
        assertGaugeValue(tenantA, MqttSharedSubNumGauge, 1);
        assertGaugeValue(tenantB, MqttRouteNumGauge, 3);
        assertGaugeValue(tenantB, MqttSharedSubNumGauge, 4);

        // demote once should remove for all
        tenantsState.toggleMetering(false);
        assertNoGauge(tenantA, MqttRouteNumGauge);
        assertNoGauge(tenantA, MqttSharedSubNumGauge);
        assertGauge(tenantA, MqttRouteSpaceGauge);
        assertNoGauge(tenantB, MqttRouteNumGauge);
        assertNoGauge(tenantB, MqttSharedSubNumGauge);
        assertGauge(tenantB, MqttRouteSpaceGauge);
    }
}
