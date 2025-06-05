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

import org.apache.bifromq.basecluster.AgentHostOptions;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basecrdt.service.CRDTServiceOptions;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LandscapeCleanupTest {
    private IAgentHost agentHost1;
    private IAgentHost agentHost2;
    private ICRDTService crdtService1;
    private ICRDTService crdtService2;
    private IBaseKVMetaService metaService1;
    private IBaseKVMetaService metaService2;

    @BeforeMethod
    void setup() {
        agentHost1 = IAgentHost.newInstance(AgentHostOptions.builder().addr("127.0.0.1").build());
        crdtService1 = ICRDTService.newInstance(agentHost1, CRDTServiceOptions.builder().build());
        metaService1 = new BaseKVMetaService(crdtService1);

        agentHost2 = IAgentHost.newInstance(AgentHostOptions.builder().addr("127.0.0.1").build());

        agentHost1.join(Set.of(new InetSocketAddress(agentHost2.local().getAddress(), agentHost2.local().getPort())))
            .join();
        crdtService2 = ICRDTService.newInstance(agentHost2, CRDTServiceOptions.builder().build());
        metaService2 = new BaseKVMetaService(crdtService2);
    }

    @AfterMethod
    void tearDown() {
        metaService1.close();
        crdtService1.close();
        agentHost1.close();
    }

    @Test
    public void testCleanup() {
        KVRangeStoreDescriptor descriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId("testStoreId1")
            .setHlc(HLC.INST.get())
            .build();
        KVRangeStoreDescriptor descriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("testStoreId2")
            .setHlc(HLC.INST.get())
            .build();
        Map<String, KVRangeStoreDescriptor> landscape =
            Map.of(descriptor1.getId(), descriptor1, descriptor2.getId(), descriptor2);
        IBaseKVClusterMetadataManager manager1 = metaService1.metadataManager("test");
        IBaseKVClusterMetadataManager manager2 = metaService2.metadataManager("test");
        manager1.report(descriptor1).join();
        manager2.report(descriptor2).join();

        await().until(() -> landscape.equals(manager1.landscape().blockingFirst())
            && landscape.equals(manager2.landscape().blockingFirst()));

        metaService2.close();
        crdtService2.close();
        agentHost2.close();

        Map<String, KVRangeStoreDescriptor> landscape1 = Map.of(descriptor1.getId(), descriptor1);
        await().until(() -> landscape1.equals(manager1.landscape().blockingFirst()));
    }
}
