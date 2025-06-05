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

package org.apache.bifromq.plugin.clientbalancer;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.type.ClientInfo;
import org.pf4j.DefaultPluginManager;
import org.pf4j.PluginManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ClientBalancerManagerTest {
    private ClientBalancerManager manager;
    private PluginManager pluginManager;

    @BeforeMethod
    public void setup() {
        pluginManager = new DefaultPluginManager();
        pluginManager.loadPlugins();
        pluginManager.startPlugins();
    }

    @AfterMethod
    public void tearDown() {
        pluginManager.stopPlugins();
        pluginManager.unloadPlugins();
    }

    @Test
    public void needRedirect() {
        manager = new ClientBalancerManager(pluginManager);
        assertFalse(manager.needRedirect(ClientInfo.newBuilder().build()).isPresent());
        assertTrue(ClientBalancerTestStub.currentInstance.isNeedRedirectCalled());
        manager.close();
    }

    @Test
    public void stop() {
        manager = new ClientBalancerManager(pluginManager);
        manager.close();
        assertTrue(ClientBalancerTestStub.currentInstance.isClosed());
    }
}
