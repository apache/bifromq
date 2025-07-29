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

import static org.testng.Assert.assertTrue;

import io.reactivex.rxjava3.observers.TestObserver;
import java.net.InetSocketAddress;
import java.util.Set;
import org.apache.bifromq.basecluster.AgentHostOptions;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basecrdt.service.CRDTServiceOptions;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BaseKVMetaServiceTest {
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
        metaService2.close();
        crdtService2.close();
        agentHost2.close();
    }

    @Test
    public void clusterIds() {
        assertTrue(metaService1.clusterIds().blockingFirst().isEmpty());
        assertTrue(metaService2.clusterIds().blockingFirst().isEmpty());

        TestObserver<Set<String>> clusterIdObserver1 = metaService1.clusterIds().test();
        TestObserver<Set<String>> clusterIdObserver2 = metaService2.clusterIds().test();

        crdtService1.host(CRDTUtil.toLandscapeURI("testCluster1"));

        clusterIdObserver1.awaitCount(2);
        clusterIdObserver1.assertValueAt(1, Set.of("testCluster1"));

        clusterIdObserver2.awaitCount(2);
        clusterIdObserver2.assertValueAt(1, Set.of("testCluster1"));

        crdtService1.host(CRDTUtil.toLandscapeURI("testCluster2"));

        clusterIdObserver1.awaitCount(3);
        clusterIdObserver1.assertValueAt(2, Set.of("testCluster1", "testCluster2"));

        clusterIdObserver2.awaitCount(3);
        clusterIdObserver2.assertValueAt(2, Set.of("testCluster1", "testCluster2"));

        crdtService1.stopHosting(CRDTUtil.toLandscapeURI("testCluster1")).join();

        clusterIdObserver1.awaitCount(4);
        clusterIdObserver1.assertValueAt(3, Set.of("testCluster2"));

        clusterIdObserver2.awaitCount(4);
        clusterIdObserver2.assertValueAt(3, Set.of("testCluster2"));
    }
}
