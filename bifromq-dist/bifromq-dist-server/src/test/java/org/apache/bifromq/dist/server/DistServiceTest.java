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

package org.apache.bifromq.dist.server;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import org.apache.bifromq.basecluster.AgentHostOptions;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basecrdt.service.CRDTServiceOptions;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.localengine.memory.InMemKVEngineConfigurator;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.basekv.store.option.KVRangeStoreOptions;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.baserpc.client.IRPCClient;
import org.apache.bifromq.baserpc.server.IRPCServer;
import org.apache.bifromq.baserpc.server.RPCServerBuilder;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.dist.worker.IDistWorker;
import org.apache.bifromq.plugin.eventcollector.Event;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.plugin.subbroker.IDeliverer;
import org.apache.bifromq.plugin.subbroker.ISubBroker;
import org.apache.bifromq.plugin.subbroker.ISubBrokerManager;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

@Slf4j
public abstract class DistServiceTest {
    private final ISettingProvider settingProvider = Setting::current;
    private final IResourceThrottler resourceThrottler = (tenantId, type) -> true;
    private final IEventCollector eventCollector = new IEventCollector() {
        @Override
        public void report(Event<?> event) {
            log.debug("event {}", event);
        }
    };
    @Mock
    protected ISubBroker inboxBroker;
    @Mock
    protected IDeliverer inboxDeliverer;
    private IAgentHost agentHost;
    private ICRDTService crdtService;
    private IRPCServiceTrafficService trafficService;
    private IBaseKVMetaService metaService;
    private IRPCServer rpcServer;
    private IDistWorker distWorker;
    private IDistServer distServer;
    private IDistClient distClient;
    private IBaseKVStoreClient workerClient;
    private ScheduledExecutorService bgTaskExecutor;
    @Mock
    private ISubBrokerManager subBrokerMgr;

    private AutoCloseable closeable;

    @BeforeClass(alwaysRun = true)
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(subBrokerMgr.get(anyInt())).thenReturn(inboxBroker);
        when(inboxBroker.open(anyString())).thenReturn(inboxDeliverer);
        bgTaskExecutor = Executors.newSingleThreadScheduledExecutor();
        AgentHostOptions agentHostOpts = AgentHostOptions.builder()
            .addr("127.0.0.1")
            .baseProbeInterval(Duration.ofSeconds(10))
            .joinRetryInSec(5)
            .joinTimeout(Duration.ofMinutes(5))
            .build();
        agentHost = IAgentHost.newInstance(agentHostOpts);

        crdtService = ICRDTService.newInstance(agentHost, CRDTServiceOptions.builder().build());

        trafficService = IRPCServiceTrafficService.newInstance(crdtService);

        metaService = IBaseKVMetaService.newInstance(crdtService);

        distClient = IDistClient.newBuilder().trafficService(trafficService).build();

        KVRangeStoreOptions kvRangeStoreOptions = new KVRangeStoreOptions();
        kvRangeStoreOptions.setDataEngineConfigurator(new InMemKVEngineConfigurator());
        kvRangeStoreOptions.setWalEngineConfigurator(new InMemKVEngineConfigurator());

        workerClient = IBaseKVStoreClient
            .newBuilder()
            .clusterId(IDistWorker.CLUSTER_NAME)
            .trafficService(trafficService)
            .metaService(metaService)
            .build();
        int tickerThreads = 2;
        RPCServerBuilder rpcServerBuilder = IRPCServer.newBuilder()
            .trafficService(trafficService)
            .host("127.0.0.1");
        distWorker = IDistWorker
            .builder()
            .rpcServerBuilder(rpcServerBuilder)
            .agentHost(agentHost)
            .metaService(metaService)
            .eventCollector(eventCollector)
            .resourceThrottler(resourceThrottler)
            .distClient(distClient)
            .distWorkerClient(workerClient)
            .tickerThreads(tickerThreads)
            .bgTaskExecutor(bgTaskExecutor)
            .storeOptions(kvRangeStoreOptions)
            .subBrokerManager(subBrokerMgr)
            .build();
        distServer = IDistServer.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .distWorkerClient(workerClient)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .build();

        rpcServer = rpcServerBuilder.build();
        rpcServer.start();
        await().until(() -> BoundaryUtil.isValidSplitSet(workerClient.latestEffectiveRouter().keySet()));
        distClient.connState().filter(s -> s == IRPCClient.ConnState.READY).blockingFirst();
        log.info("Setup finished, and start testing");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws Exception {
        log.info("Finish testing, and tearing down");
        workerClient.close();
        distClient.close();
        rpcServer.shutdown();
        distWorker.close();
        distServer.close();
        metaService.close();
        trafficService.close();
        crdtService.close();
        agentHost.close();
        bgTaskExecutor.shutdown();
        closeable.close();
    }

    protected final IDistClient distClient() {
        return distClient;
    }
}
