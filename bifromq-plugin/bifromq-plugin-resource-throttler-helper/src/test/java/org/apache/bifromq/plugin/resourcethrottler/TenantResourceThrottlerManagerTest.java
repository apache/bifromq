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

package org.apache.bifromq.plugin.resourcethrottler;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.pf4j.DefaultPluginManager;
import org.pf4j.PluginManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TenantResourceThrottlerManagerTest {
    private final String tenantId = "tenantA";
    private ResourceThrottlerManager manager;
    private PluginManager pluginManager;

    @BeforeMethod
    public void setup() {
        pluginManager = new DefaultPluginManager();
        pluginManager.loadPlugins();
        pluginManager.startPlugins();
    }

    @AfterMethod
    public void teardown() {
        pluginManager.stopPlugins();
        pluginManager.unloadPlugins();
    }

    @Test
    public void devOnlyMode() {
        manager = new ResourceThrottlerManager(null, pluginManager);
        for (TenantResourceType type : TenantResourceType.values()) {
            assertTrue(manager.hasResource(tenantId, type));
        }
        manager.close();
    }

    @Test
    public void pluginSpecified() {
        manager = new ResourceThrottlerManager(ResourceThrottlerTestStub.class.getName(), pluginManager);
        ResourceThrottlerTestStub stub = (ResourceThrottlerTestStub) manager.getDelegate();
        stub.setResource(tenantId, TenantResourceType.TotalConnections, false);
        await().until(() -> !manager.hasResource(tenantId, TenantResourceType.TotalConnections));
        manager.close();
    }

    @Test
    public void pluginNotFound() {
        ResourceThrottlerManager devOnlyManager = new ResourceThrottlerManager(null, pluginManager);
        manager = new ResourceThrottlerManager("Fake", pluginManager);
        for (TenantResourceType type : TenantResourceType.values()) {
            assertEquals(devOnlyManager.hasResource(tenantId, type), manager.hasResource(tenantId, type));
        }
        devOnlyManager.close();
    }

    @Test
    public void stop() {
        manager = new ResourceThrottlerManager(ResourceThrottlerTestStub.class.getName(), pluginManager);
        manager.close();
    }
}
