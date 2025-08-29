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

package org.apache.bifromq.basekv.metaservice;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;

import java.util.Map;
import org.apache.bifromq.basecluster.AgentHostOptions;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basecrdt.service.CRDTServiceOptions;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BaseKVLandscapeReportTest {
    private IAgentHost agentHost;
    private ICRDTService crdtService;
    private IBaseKVMetaService metaService;

    @BeforeMethod
    void setup() {
        agentHost = IAgentHost.newInstance(AgentHostOptions.builder().addr("127.0.0.1").build());
        crdtService = ICRDTService.newInstance(agentHost, CRDTServiceOptions.builder().build());
        metaService = new BaseKVMetaService(crdtService);
    }

    @AfterMethod
    void tearDown() {
        metaService.close();
        crdtService.close();
        agentHost.close();
    }

    @Test
    public void reportStoreDescriptor() {
        KVRangeStoreDescriptor descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("testStoreId")
            .setHlc(HLC.INST.get())
            .build();
        IBaseKVLandscapeObserver observer = metaService.landscapeObserver("test");
        IBaseKVLandscapeReporter reporter = metaService.landscapeReporter("test", descriptor.getId());
        reporter.report(descriptor).join();
        assertEquals(Map.of(descriptor.getId(), descriptor), observer.landscape().blockingFirst());
        assertEquals(descriptor, observer.getStoreDescriptor(descriptor.getId()).get());
    }

    @Test
    public void stop() {
        IBaseKVLandscapeObserver observer = metaService.landscapeObserver("test");
        IBaseKVLandscapeReporter reporter = metaService.landscapeReporter("test", "testStoreId");

        KVRangeStoreDescriptor descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("testStoreId")
            .setHlc(HLC.INST.get())
            .build();
        reporter.report(descriptor).join();
        await().until(() -> observer.getStoreDescriptor(descriptor.getId()).isPresent());

        reporter.stop();
        await().until(() -> observer.landscape().blockingFirst().isEmpty());
    }
}
