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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.reactivex.rxjava3.observers.TestObserver;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bifromq.basecluster.AgentHostOptions;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basecrdt.service.CRDTServiceOptions;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.basekv.proto.BalancerStateSnapshot;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BaseKVBalancerStateProposeTest {
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
    public void noLoadRules() throws InterruptedException {
        IBaseKVStoreBalancerStatesProposal statesProposal = metaService.balancerStatesProposal("test");
        TestObserver<Map<String, BalancerStateSnapshot>> testObserver = statesProposal.expectedBalancerStates()
            .timeout(100, TimeUnit.MILLISECONDS).test();
        testObserver.await();
        testObserver.assertError(TimeoutException.class);
    }

    @Test
    public void proposeRunState() {
        IBaseKVStoreBalancerStatesProposal statesProposal = metaService.balancerStatesProposal("test");
        IBaseKVStoreBalancerStatesProposer statesProposer = metaService.balancerStatesProposer("test");
        statesProposer.proposeRunState("balancer1", true).join();

        Map<String, BalancerStateSnapshot> expected = statesProposal.expectedBalancerStates().blockingFirst();
        assertTrue(expected.get("balancer1").getDisable());
    }

    @Test
    public void proposeLoadRules() {
        Struct loadRules = Struct.newBuilder()
            .putFields("key", Value.newBuilder().setStringValue("value").build())
            .build();
        IBaseKVStoreBalancerStatesProposal statesProposal = metaService.balancerStatesProposal("test");
        IBaseKVStoreBalancerStatesProposer statesProposer = metaService.balancerStatesProposer("test");
        statesProposer.proposeLoadRules("balancer1", loadRules).join();
        Map<String, BalancerStateSnapshot> expected = statesProposal.expectedBalancerStates().blockingFirst();
        assertFalse(expected.get("balancer1").getDisable());
        assertEquals(expected.get("balancer1").getLoadRules(), loadRules);
    }

    @Test
    public void clearProposedState() {
        IBaseKVStoreBalancerStatesProposal statesProposal = metaService.balancerStatesProposal("test");
        IBaseKVStoreBalancerStatesProposer statesProposer = metaService.balancerStatesProposer("test");
        statesProposer.proposeRunState("balancer1", true).join();
        statesProposer.clearProposedState("balancer1").join();

        Map<String, BalancerStateSnapshot> expected = statesProposal.expectedBalancerStates().blockingFirst();
        assertFalse(expected.containsKey("balancer1"));
    }
}
