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

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import org.apache.bifromq.basecluster.AgentHostOptions;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basecrdt.service.CRDTServiceOptions;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.basekv.proto.BalancerStateSnapshot;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BalancerStatesCleanupTest {
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
        String balancerFQN = "testBalancer";
        Struct loadRule1 = Struct.newBuilder()
            .putFields("key1", Value.newBuilder().setStringValue("value1").build())
            .build();
        Struct loadRule2 = Struct.newBuilder()
            .putFields("key2", Value.newBuilder().setStringValue("value1").build())
            .build();

        IBaseKVStoreBalancerStatesObserver observer1 = metaService1.balancerStatesObserver("test");
        IBaseKVStoreBalancerStatesReporter reporter1 = metaService1.balancerStatesReporter("test", "testStoreId1");
        IBaseKVStoreBalancerStatesObserver observer2 = metaService2.balancerStatesObserver("test");
        IBaseKVStoreBalancerStatesReporter reporter2 = metaService2.balancerStatesReporter("test", "testStoreId2");
        reporter1.reportBalancerState(balancerFQN, true, loadRule1).join();
        reporter2.reportBalancerState(balancerFQN, false, loadRule2).join();

        await().until(() -> {
            Map<String, Map<String, BalancerStateSnapshot>> observed1 = observer1.currentBalancerStates()
                .blockingFirst();
            Map<String, Map<String, BalancerStateSnapshot>> observed2 = observer2.currentBalancerStates()
                .blockingFirst();
            return observed1.size() == 2 && observed1.equals(observed2);
        });

        metaService2.close();
        crdtService2.close();
        agentHost2.close();

        await().until(() -> {
            Map<String, Map<String, BalancerStateSnapshot>> observed1 = observer1.currentBalancerStates()
                .blockingFirst();
            return observed1.size() == 1 && observed1.containsKey("testStoreId1");
        });
    }
}
