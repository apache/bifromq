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
        assertGaugeValue(tenantId, MqttSharedSubNumGauge, 1);
    }

    @Test
    public void testRemove() {
        when(reader.size(any())).thenReturn(1L);
        String tenantId = "tenant" + System.nanoTime();
        TenantsStats tenantsState = new TenantsStats(readerSupplier);
        tenantsState.incSharedRoutes(tenantId);
        tenantsState.incNormalRoutes(tenantId);
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
        assertGauge(tenantId, MqttRouteSpaceGauge);
        assertGauge(tenantId, MqttRouteNumGauge);
        assertGauge(tenantId, MqttSharedSubNumGauge);

        tenantsState.close();
        assertNoGauge(tenantId, MqttRouteSpaceGauge);
        assertNoGauge(tenantId, MqttRouteNumGauge);
        assertNoGauge(tenantId, MqttSharedSubNumGauge);
        verify(reader).close();
    }
}
