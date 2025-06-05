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

package org.apache.bifromq.plugin.eventcollector;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.plugin.eventcollector.mqttbroker.PingReq;
import org.apache.bifromq.type.ClientInfo;
import org.pf4j.DefaultPluginManager;
import org.pf4j.PluginManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EventCollectorManagerTest {
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
    public void ReportEvent() {
        EventCollectorManager manager = new EventCollectorManager(pluginManager);
        EventCollectorTestStub collector = (EventCollectorTestStub) manager.get(EventCollectorTestStub.class.getName());
        assertEquals(collector.events.size(), 0);
        manager.report(ThreadLocalEventPool.getLocal(PingReq.class).clientInfo(ClientInfo.getDefaultInstance()));
        await().until(() -> collector.events.size() == 1);
    }
}
