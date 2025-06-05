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

import static org.apache.bifromq.basekv.metaservice.LoadRulesProposalHandler.Result.ACCEPTED;
import static org.apache.bifromq.basekv.metaservice.LoadRulesProposalHandler.Result.NO_BALANCER;
import static org.apache.bifromq.basekv.metaservice.LoadRulesProposalHandler.Result.REJECTED;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basecluster.AgentHostOptions;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basecrdt.service.CRDTServiceOptions;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.reactivex.rxjava3.observers.TestObserver;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BaseKVClusterMetadataManagerTest {
    private IAgentHost agentHost;
    private ICRDTService crdtService;
    private IBaseKVMetaService metaService;

    @BeforeMethod
    void setup() {
        agentHost = IAgentHost.newInstance(AgentHostOptions.builder().addr("127.0.0.1").build());
        crdtService = ICRDTService.newInstance(agentHost, CRDTServiceOptions.builder().build());
        metaService = new BaseKVMetaService(crdtService, Duration.ofMillis(1000));
    }

    @AfterMethod
    void tearDown() {
        metaService.close();
        crdtService.close();
        agentHost.close();
    }


    @Test
    public void noLoadRules() throws InterruptedException {
        IBaseKVClusterMetadataManager manager = metaService.metadataManager("test");
        TestObserver<Map<String, Struct>> testObserver = manager.loadRules().timeout(100, TimeUnit.MILLISECONDS).test();
        testObserver.await();
        testObserver.assertError(TimeoutException.class);
    }

    @Test
    public void proposeLoadRulesTimeout() {
        Struct loadRulesJSON =
            Struct.newBuilder().putFields("key", Value.newBuilder().setStringValue("value").build()).build();
        IBaseKVClusterMetadataManager manager = metaService.metadataManager("test");

        try {
            manager.proposeLoadRules("balancer1", loadRulesJSON).join();
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    @Test
    public void proposeLoadRules() {
        Struct loadRulesJSON =
            Struct.newBuilder().putFields("key", Value.newBuilder().setStringValue("value").build()).build();
        IBaseKVClusterMetadataManager manager = metaService.metadataManager("test");
        manager.setLoadRulesProposalHandler((balancerClassFQN, loadRules) -> ACCEPTED);

        IBaseKVClusterMetadataManager.ProposalResult result =
            manager.proposeLoadRules("balancer1", loadRulesJSON).join();
        assertEquals(result, IBaseKVClusterMetadataManager.ProposalResult.ACCEPTED);

        TestObserver<Map<String, Struct>> observer = manager.loadRules().test();
        await().until(() ->
            !observer.values().isEmpty() && observer.values().get(0).equals(Map.of("balancer1", loadRulesJSON)));
    }

    @Test
    public void proposeLoadRulesRejected() {
        Struct loadRulesJSON =
            Struct.newBuilder().putFields("key", Value.newBuilder().setStringValue("value").build()).build();
        IBaseKVClusterMetadataManager manager = metaService.metadataManager("test");
        manager.setLoadRulesProposalHandler((balancerClassFQN, loadRules) -> REJECTED);

        IBaseKVClusterMetadataManager.ProposalResult result =
            manager.proposeLoadRules("balancer1", loadRulesJSON).join();
        assertEquals(result, IBaseKVClusterMetadataManager.ProposalResult.REJECTED);

        manager.loadRules().test().assertNoValues();
    }

    @Test
    public void proposeLoadRulesDropped() {
        Struct loadRulesJSON =
            Struct.newBuilder().putFields("key", Value.newBuilder().setStringValue("value").build()).build();
        IBaseKVClusterMetadataManager manager = metaService.metadataManager("test");
        manager.setLoadRulesProposalHandler((balancerClassFQN, loadRules) -> NO_BALANCER);

        IBaseKVClusterMetadataManager.ProposalResult result =
            manager.proposeLoadRules("balancer1", loadRulesJSON).join();
        assertEquals(result, IBaseKVClusterMetadataManager.ProposalResult.NO_BALANCER);
        manager.loadRules().test().assertNoValues();
    }

    @Test
    public void proposeLoadRulesOverridden() {
        Struct loadRulesJSON1 =
            Struct.newBuilder().putFields("key", Value.newBuilder().setStringValue("value1").build()).build();

        Struct loadRulesJSON2 =
            Struct.newBuilder().putFields("key", Value.newBuilder().setStringValue("value2").build()).build();
        IBaseKVClusterMetadataManager manager = metaService.metadataManager("test");
        manager.setLoadRulesProposalHandler((balancerClassFQN, loadRules) -> ACCEPTED);

        CompletableFuture<IBaseKVClusterMetadataManager.ProposalResult> resultFuture =
            manager.proposeLoadRules("balancer1", loadRulesJSON1);
        IBaseKVClusterMetadataManager.ProposalResult result1 =
            manager.proposeLoadRules("balancer1", loadRulesJSON2).join();
        assertEquals(resultFuture.join(), IBaseKVClusterMetadataManager.ProposalResult.OVERRIDDEN);
        assertEquals(result1, IBaseKVClusterMetadataManager.ProposalResult.ACCEPTED);
        TestObserver<?> observer = manager.loadRules().test();
        await().until(() ->
            !observer.values().isEmpty() && observer.values().get(0).equals(Map.of("balancer1", loadRulesJSON2)));
    }


    @Test
    public void reportStoreDescriptor() {
        KVRangeStoreDescriptor descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("testStoreId")
            .setHlc(HLC.INST.get())
            .build();
        IBaseKVClusterMetadataManager manager = metaService.metadataManager("test");
        manager.report(descriptor).join();
        assertEquals(Map.of(descriptor.getId(), descriptor), manager.landscape().blockingFirst());
        assertEquals(descriptor, manager.getStoreDescriptor(descriptor.getId()).get());
    }

    @Test
    public void stopReport() {
        IBaseKVClusterMetadataManager manager = metaService.metadataManager("test");
        manager.stopReport("testStoreId").join();

        KVRangeStoreDescriptor descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("testStoreId")
            .setHlc(HLC.INST.get())
            .build();
        manager.report(descriptor).join();
        await().until(() -> manager.getStoreDescriptor(descriptor.getId()).isPresent());

        manager.stopReport("testStoreId").join();
        assertEquals(Collections.emptyMap(), manager.landscape().blockingFirst());
        assertTrue(manager.getStoreDescriptor(descriptor.getId()).isEmpty());
    }
}
