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
import static org.apache.bifromq.metrics.TenantMetric.MqttRouteSpaceGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttSharedSubNumGauge;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TenantsStatesTest extends MeterTest {
    @Mock
    private IKVCloseableReader reader;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        super.setup();
        closeable = MockitoAnnotations.openMocks(this);
        when(reader.boundary()).thenReturn(FULL_BOUNDARY);
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
        TenantsState tenantsState = new TenantsState(reader);
        tenantsState.incSharedRoutes(tenantId);
        assertGaugeValue(tenantId, MqttSharedSubNumGauge, 1);
    }

    @Test
    public void testRemove() {
        when(reader.size(any())).thenReturn(1L);
        String tenantId = "tenant" + System.nanoTime();
        TenantsState tenantsState = new TenantsState(reader);
        tenantsState.incSharedRoutes(tenantId);
        tenantsState.incNormalRoutes(tenantId);
        assertGaugeValue(tenantId, MqttSharedSubNumGauge, 1);
        assertGauge(tenantId, MqttRouteSpaceGauge);

        tenantsState.decSharedRoutes(tenantId);
        assertGaugeValue(tenantId, MqttSharedSubNumGauge, 0);
        assertGauge(tenantId, MqttRouteSpaceGauge);
        tenantsState.decNormalRoutes(tenantId);
        assertNoGauge(tenantId, MqttRouteSpaceGauge);
        assertNoGauge(tenantId, MqttSharedSubNumGauge);
    }

    @Test
    public void testReset() {
        when(reader.size(any())).thenReturn(1L);
        String tenantId = "tenant" + System.nanoTime();
        TenantsState tenantsState = new TenantsState(reader);
        tenantsState.incNormalRoutes(tenantId);
        assertGauge(tenantId, MqttRouteSpaceGauge);
        assertGauge(tenantId, MqttSharedSubNumGauge);

        tenantsState.reset();
        assertNoGauge(tenantId, MqttRouteSpaceGauge);
        assertNoGauge(tenantId, MqttSharedSubNumGauge);
        verify(reader, never()).close();
    }

    @Test
    public void testClose() {
        when(reader.size(any())).thenReturn(1L);
        String tenantId = "tenant" + System.nanoTime();
        TenantsState tenantsState = new TenantsState(reader);
        tenantsState.incNormalRoutes(tenantId);
        assertGauge(tenantId, MqttRouteSpaceGauge);
        assertGauge(tenantId, MqttSharedSubNumGauge);

        tenantsState.close();
        assertNoGauge(tenantId, MqttRouteSpaceGauge);
        assertNoGauge(tenantId, MqttSharedSubNumGauge);
        verify(reader).close();
    }
}
