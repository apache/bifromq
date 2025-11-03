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

package org.apache.bifromq.inbox.server;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.protobuf.Struct;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basecluster.AgentHostOptions;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basecrdt.service.CRDTServiceOptions;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.basekv.store.option.KVRangeStoreOptions;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.baserpc.client.IRPCClient;
import org.apache.bifromq.baserpc.server.IRPCServer;
import org.apache.bifromq.baserpc.server.RPCServerBuilder;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.dist.client.MatchResult;
import org.apache.bifromq.dist.client.UnmatchResult;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.inbox.store.IInboxStore;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.retain.client.IRetainClient;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

@Slf4j
public abstract class InboxServiceTest {
    private final int tickerThreads = 2;
    protected IInboxClient inboxClient;
    @Mock
    protected IEventCollector eventCollector;
    @Mock
    protected IResourceThrottler resourceThrottler;
    @Mock
    protected ISettingProvider settingProvider;
    @Mock
    protected IDistClient distClient;
    @Mock
    protected IRetainClient retainClient;
    @Mock
    protected ISessionDictClient sessionDictClient;
    private IAgentHost agentHost;
    private ICRDTService crdtService;
    private IRPCServiceTrafficService trafficService;
    private IBaseKVMetaService metaService;
    private IBaseKVStoreClient inboxStoreClient;
    private IInboxStore inboxStore;
    private IInboxServer inboxServer;
    private IRPCServer rpcServer;
    private ScheduledExecutorService bgTaskExecutor;
    private AutoCloseable closeable;

    @BeforeClass(alwaysRun = true)
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        when(settingProvider.provide(any(), anyString())).thenAnswer(
            invocation -> ((Setting) invocation.getArgument(0)).current(invocation.getArgument(1)));
        when(distClient.addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(),
            anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        when(distClient.removeRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(),
            anyLong()))
            .thenReturn(CompletableFuture.completedFuture(UnmatchResult.OK));
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
        inboxClient = IInboxClient.newBuilder().trafficService(trafficService).build();

        KVRangeStoreOptions kvRangeStoreOptions = new KVRangeStoreOptions();
        Struct memConf = Struct.newBuilder().build();
        kvRangeStoreOptions.setDataEngineType("memory");
        kvRangeStoreOptions.setDataEngineConf(memConf);
        kvRangeStoreOptions.setWalEngineType("memory");
        kvRangeStoreOptions.setWalEngineConf(memConf);
        bgTaskExecutor = Executors.newSingleThreadScheduledExecutor();
        inboxStoreClient = IBaseKVStoreClient.newBuilder()
            .clusterId(IInboxStore.CLUSTER_NAME)
            .trafficService(trafficService)
            .metaService(metaService)
            .build();
        RPCServerBuilder rpcServerBuilder = IRPCServer.newBuilder().host("127.0.0.1").trafficService(trafficService);
        inboxStore = IInboxStore.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .agentHost(agentHost)
            .metaService(metaService)
            .distClient(distClient)
            .inboxClient(inboxClient)
            .retainClient(retainClient)
            .sessionDictClient(sessionDictClient)
            .inboxStoreClient(inboxStoreClient)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .resourceThrottler(resourceThrottler)
            .storeOptions(kvRangeStoreOptions)
            .tickerThreads(tickerThreads)
            .bgTaskExecutor(bgTaskExecutor)
            .detachTimeout(Duration.ofSeconds(2))
            .bootstrapDelay(Duration.ofSeconds(1))
            .build();
        inboxServer = IInboxServer.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .inboxClient(inboxClient)
            .distClient(distClient)
            .inboxStoreClient(inboxStoreClient)
            .build();
        rpcServer = rpcServerBuilder.build();
        rpcServer.start();
        await().forever().until(() -> BoundaryUtil.isValidSplitSet(inboxStoreClient.latestEffectiveRouter().keySet()));
        inboxClient.connState().filter(s -> s == IRPCClient.ConnState.READY).blockingFirst();
        log.info("Setup finished, and start testing");
    }

    @SneakyThrows
    @AfterClass(alwaysRun = true)
    public void tearDown() {
        log.info("Finish testing, and tearing down");
        inboxClient.close();
        inboxStoreClient.close();
        rpcServer.shutdown();
        inboxServer.close();
        inboxStore.close();
        metaService.close();
        trafficService.close();
        crdtService.close();
        agentHost.close();
        bgTaskExecutor.shutdown();
        closeable.close();
    }

    @BeforeMethod(alwaysRun = true)
    public void beforeCaseStart(Method method) {
        log.info("Test case[{}.{}] start", method.getDeclaringClass().getName(), method.getName());
    }

    @SneakyThrows
    @AfterMethod(alwaysRun = true)
    public void afterCaseFinish(Method method) {
        log.info("Test case[{}.{}] finished, doing teardown",
            method.getDeclaringClass().getName(), method.getName());
    }
}
