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

package org.apache.bifromq.plugin.resourcethrottler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pf4j.PluginManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class ResourceThrottlerManagerTest {
    @Mock
    private PluginManager pluginManager;
    @Mock
    private IResourceThrottler mockResourceThrottler;
    private MeterRegistry meterRegistry;
    private final String tenantId = "testTenant";
    private final TenantResourceType resourceType = TenantResourceType.TotalConnections;
    private ResourceThrottlerManager manager;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
        closeable = MockitoAnnotations.openMocks(this);
        when(pluginManager.getExtensions(IResourceThrottler.class)).thenReturn(
            Collections.singletonList(mockResourceThrottler));
    }

    @AfterMethod
    public void tearDown() throws Exception {
        closeable.close();
        meterRegistry.clear();
        Metrics.globalRegistry.clear();
    }

    @Test
    public void devOnlyMode() {
        when(pluginManager.getExtensions(IResourceThrottler.class)).thenReturn(Collections.emptyList());
        manager = new ResourceThrottlerManager(null, pluginManager);
        for (TenantResourceType type : TenantResourceType.values()) {
            assertTrue(manager.hasResource(tenantId, type));
        }
        manager.close();
    }

    @Test
    public void pluginSpecified() {
        manager = new ResourceThrottlerManager(mockResourceThrottler.getClass().getName(), pluginManager);
        when(mockResourceThrottler.hasResource(anyString(), any(TenantResourceType.class))).thenReturn(false);
        boolean hasResource = manager.hasResource(tenantId, resourceType);
        assertFalse(hasResource);
        manager.close();
    }

    @Test
    public void pluginNotSpecifiedWithSingleProvider() {
        manager = new ResourceThrottlerManager(null, pluginManager);
        when(mockResourceThrottler.hasResource(anyString(), any(TenantResourceType.class))).thenReturn(false);
        boolean hasResource = manager.hasResource(tenantId, resourceType);
        assertFalse(hasResource);
        manager.close();
    }

    @Test
    public void pluginNotSpecifiedWithMultipleProviders() {
        IResourceThrottler provider1 = new FirstTestResourceThrottler();
        IResourceThrottler provider2 = new SecondTestResourceThrottler();

        when(pluginManager.getExtensions(IResourceThrottler.class)).thenReturn(
            Arrays.asList(provider1, provider2));
        manager = new ResourceThrottlerManager(null, pluginManager);

        boolean hasResource = manager.hasResource(tenantId, resourceType);
        // Should use one of the providers (order depends on TreeMap keySet iteration)
        assertTrue(hasResource);
        manager.close();
    }

    @Test
    public void pluginNotSpecifiedWithMultipleSortByKeyProviders() {
        IResourceThrottler provider1 = new FirstTestResourceThrottler();
        IResourceThrottler provider2 = new SecondTestResourceThrottler();

        when(pluginManager.getExtensions(IResourceThrottler.class)).thenReturn(
            Arrays.asList(provider2, provider1));
        manager = new ResourceThrottlerManager(null, pluginManager);

        boolean hasResource = manager.hasResource(tenantId, resourceType);
        // Should use one of the providers (order depends on TreeMap keySet iteration)
        assertTrue(hasResource);
        manager.close();
    }

    @Test(expectedExceptions = ResourceThrottlerException.class)
    public void pluginNotFound() {
        manager = new ResourceThrottlerManager("Fake", pluginManager);
        manager.close();
    }

    @Test
    public void hasResourceOK() {
        manager = new ResourceThrottlerManager(mockResourceThrottler.getClass().getName(), pluginManager);
        when(mockResourceThrottler.hasResource(anyString(), any(TenantResourceType.class)))
            .thenReturn(true);
        boolean hasResource = manager.hasResource(tenantId, resourceType);
        assertTrue(hasResource);
        assertEquals(meterRegistry.find("call.exec.timer")
            .tag("method", "ResourceThrottler/hasResource")
            .timer()
            .count(), 1);
        assertEquals(meterRegistry.find("call.exec.fail.count")
            .tag("method", "ResourceThrottler/hasResource")
            .counter()
            .count(), 0);
        manager.close();
    }

    @Test
    public void hasResourceReturnsFalse() {
        manager = new ResourceThrottlerManager(mockResourceThrottler.getClass().getName(), pluginManager);
        when(mockResourceThrottler.hasResource(anyString(), any(TenantResourceType.class)))
            .thenReturn(false);
        boolean hasResource = manager.hasResource(tenantId, resourceType);
        assertFalse(hasResource);
        assertEquals(meterRegistry.find("call.exec.timer")
            .tag("method", "ResourceThrottler/hasResource")
            .timer()
            .count(), 1);
        assertEquals(meterRegistry.find("call.exec.fail.count")
            .tag("method", "ResourceThrottler/hasResource")
            .counter()
            .count(), 0);
        manager.close();
    }

    @Test
    public void hasResourceThrowsException() {
        manager = new ResourceThrottlerManager(mockResourceThrottler.getClass().getName(), pluginManager);
        when(mockResourceThrottler.hasResource(anyString(), any(TenantResourceType.class)))
            .thenThrow(new RuntimeException("Intend Error"));
        boolean hasResource = manager.hasResource(tenantId, resourceType);
        // Should return true when exception occurs (fail-safe)
        assertTrue(hasResource);
        assertEquals(meterRegistry.find("call.exec.timer")
            .tag("method", "ResourceThrottler/hasResource")
            .timer()
            .count(), 0);
        assertEquals(meterRegistry.find("call.exec.fail.count")
            .tag("method", "ResourceThrottler/hasResource")
            .counter()
            .count(), 1);
        manager.close();
    }

    @Test
    public void close() {
        manager = new ResourceThrottlerManager(mockResourceThrottler.getClass().getName(), pluginManager);
        manager.close();
        // Should be idempotent
        manager.close();
    }

    @Test
    public void testAllResourceTypes() {
        when(pluginManager.getExtensions(IResourceThrottler.class)).thenReturn(Collections.emptyList());
        manager = new ResourceThrottlerManager(null, pluginManager);
        // Test all resource types in dev only mode
        for (TenantResourceType type : TenantResourceType.values()) {
            assertTrue(manager.hasResource(tenantId, type));
        }
        manager.close();
    }

    static class FirstTestResourceThrottler implements IResourceThrottler {
        @Override
        public boolean hasResource(String tenantId, TenantResourceType type) {
            return true;
        }
    }

    static class SecondTestResourceThrottler implements IResourceThrottler {
        @Override
        public boolean hasResource(String tenantId, TenantResourceType type) {
            return false;
        }
    }
}
