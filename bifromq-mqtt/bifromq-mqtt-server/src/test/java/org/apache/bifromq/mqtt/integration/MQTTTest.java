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

package org.apache.bifromq.mqtt.integration;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;

import com.google.common.collect.Sets;
import io.reactivex.rxjava3.core.Observable;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.bifromq.dist.server.IDistServer;
import org.apache.bifromq.dist.worker.IDistWorker;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.inbox.server.IInboxServer;
import org.apache.bifromq.inbox.store.IInboxStore;
import org.apache.bifromq.mqtt.IMQTTBroker;
import org.apache.bifromq.mqtt.inbox.IMqttBrokerClient;
import org.apache.bifromq.plugin.authprovider.IAuthProvider;
import org.apache.bifromq.plugin.clientbalancer.IClientBalancer;
import org.apache.bifromq.plugin.eventcollector.Event;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.plugin.subbroker.ISubBrokerManager;
import org.apache.bifromq.plugin.subbroker.SubBrokerManager;
import org.apache.bifromq.retain.client.IRetainClient;
import org.apache.bifromq.retain.server.IRetainServer;
import org.apache.bifromq.retain.store.IRetainStore;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;
import org.apache.bifromq.sessiondict.rpc.proto.KillReply;
import org.apache.bifromq.sessiondict.rpc.proto.ServerRedirection;
import org.apache.bifromq.sessiondict.server.ISessionDictServer;
import org.apache.bifromq.type.ClientInfo;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.pf4j.DefaultPluginManager;
import org.pf4j.PluginManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

@Slf4j
public abstract class MQTTTest {
    protected static final String BROKER_URI = "tcp://127.0.0.1:1883";
    private final int tickerThreads = 2;
    @Mock
    protected IAuthProvider authProvider;
    @Mock
    protected IEventCollector eventCollector;
    @Mock
    protected IResourceThrottler resourceThrottler;
    @Mock
    protected ISettingProvider settingProvider;
    @Mock
    protected IClientBalancer clientBalancer;
    protected String tenantId;
    private IAgentHost agentHost;
    private ICRDTService crdtService;
    private IRPCServiceTrafficService trafficService;
    private IBaseKVMetaService metaService;
    private IRPCServer rpcServer;
    private IMqttBrokerClient mqttServerClient;
    private ISessionDictClient sessionDictClient;
    private ISessionDictServer sessionDictServer;
    private IDistClient distClient;
    private IBaseKVStoreClient distWorkerStoreClient;
    private IDistWorker distWorker;
    private IDistServer distServer;
    private IInboxClient inboxClient;
    private IBaseKVStoreClient inboxStoreKVStoreClient;
    private IInboxStore inboxStore;
    private IInboxServer inboxServer;
    private IRetainClient retainClient;
    private IBaseKVStoreClient retainStoreKVStoreClient;
    private IRetainStore retainStore;
    private IRetainServer retainServer;
    private IMQTTBroker mqttBroker;
    private ISubBrokerManager inboxBrokerMgr;
    private PluginManager pluginMgr;
    private ScheduledExecutorService bgTaskExecutor;
    private AutoCloseable closeable;

    protected MQTTTest() {
    }

    @BeforeClass(alwaysRun = true, groups = "integration")
    public final void setupClass() {
        closeable = MockitoAnnotations.openMocks(this);

        System.setProperty("distservice_topic_match_expiry_seconds", "1");
        pluginMgr = new DefaultPluginManager();
        bgTaskExecutor = Executors.newSingleThreadScheduledExecutor();

        AgentHostOptions agentHostOpts = AgentHostOptions.builder()
            .addr("127.0.0.1")
            .baseProbeInterval(Duration.ofSeconds(10))
            .joinRetryInSec(5)
            .joinTimeout(Duration.ofMinutes(5))
            .build();
        agentHost = IAgentHost.newInstance(agentHostOpts);
        log.info("Agent host started");

        crdtService = ICRDTService.newInstance(agentHost, CRDTServiceOptions.builder().build());
        log.info("CRDT service started");

        trafficService = IRPCServiceTrafficService.newInstance(crdtService);
        metaService = IBaseKVMetaService.newInstance(crdtService);

        mqttServerClient = IMqttBrokerClient.newBuilder()
            .trafficService(trafficService)
            .build();
        inboxClient = IInboxClient.newBuilder()
            .trafficService(trafficService)
            .build();
        sessionDictClient = ISessionDictClient.newBuilder()
            .trafficService(trafficService)
            .build();
        RPCServerBuilder rpcServerBuilder = IRPCServer.newBuilder()
            .host("127.0.0.1")
            .trafficService(trafficService);
        sessionDictServer = ISessionDictServer.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .mqttBrokerClient(mqttServerClient)
            .inboxClient(inboxClient)
            .build();
        inboxStoreKVStoreClient = IBaseKVStoreClient.newBuilder()
            .clusterId(IInboxStore.CLUSTER_NAME)
            .trafficService(trafficService)
            .metaService(metaService)
            .build();
        inboxStore = IInboxStore.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .agentHost(agentHost)
            .metaService(metaService)
            .distClient(distClient)
            .inboxClient(inboxClient)
            .retainClient(retainClient)
            .sessionDictClient(sessionDictClient)
            .inboxStoreClient(inboxStoreKVStoreClient)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .resourceThrottler(resourceThrottler)
            .tickerThreads(tickerThreads)
            .bgTaskExecutor(bgTaskExecutor)
            .bootstrapDelay(Duration.ofSeconds(1))
            .storeOptions(new KVRangeStoreOptions()
                .setDataEngineConfigurator(new InMemKVEngineConfigurator())
                .setWalEngineConfigurator(new InMemKVEngineConfigurator()))
            .build();
        distClient = IDistClient.newBuilder()
            .trafficService(trafficService)
            .build();
        retainClient = IRetainClient
            .newBuilder()
            .trafficService(trafficService)
            .build();
        inboxServer = IInboxServer.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .inboxClient(inboxClient)
            .distClient(distClient)
            .inboxStoreClient(inboxStoreKVStoreClient)
            .build();
        retainStoreKVStoreClient = IBaseKVStoreClient
            .newBuilder()
            .clusterId(IRetainStore.CLUSTER_NAME)
            .trafficService(trafficService)
            .metaService(metaService)
            .build();
        retainStore = IRetainStore.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .agentHost(agentHost)
            .metaService(metaService)
            .retainStoreClient(retainStoreKVStoreClient)
            .tickerThreads(tickerThreads)
            .bgTaskExecutor(bgTaskExecutor)
            .bootstrapDelay(Duration.ofSeconds(1))
            .storeOptions(new KVRangeStoreOptions()
                .setDataEngineConfigurator(new InMemKVEngineConfigurator())
                .setWalEngineConfigurator(new InMemKVEngineConfigurator()))
            .build();

        distWorkerStoreClient = IBaseKVStoreClient.newBuilder()
            .clusterId(IDistWorker.CLUSTER_NAME)
            .trafficService(trafficService)
            .metaService(metaService)
            .build();

        inboxBrokerMgr = new SubBrokerManager(pluginMgr, mqttServerClient, inboxClient);
        retainServer = IRetainServer.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .subBrokerManager(inboxBrokerMgr)
            .distClient(distClient)
            .retainStoreClient(retainStoreKVStoreClient)
            .settingProvider(settingProvider)
            .build();
        distWorker = IDistWorker.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .agentHost(agentHost)
            .metaService(metaService)
            .eventCollector(eventCollector)
            .resourceThrottler(resourceThrottler)
            .distClient(distClient)
            .distWorkerClient(distWorkerStoreClient)
            .tickerThreads(tickerThreads)
            .bgTaskExecutor(bgTaskExecutor)
            .bootstrapDelay(Duration.ofSeconds(1))
            .storeOptions(new KVRangeStoreOptions()
                .setDataEngineConfigurator(new InMemKVEngineConfigurator())
                .setWalEngineConfigurator(new InMemKVEngineConfigurator()))
            .subBrokerManager(inboxBrokerMgr)
            .settingProvider(settingProvider)
            .build();
        distServer = IDistServer.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .distWorkerClient(distWorkerStoreClient)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .build();
        mqttBroker = IMQTTBroker.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .mqttBossELGThreads(1)
            .mqttWorkerELGThreads(4)
            .authProvider(authProvider)
            .clientBalancer(clientBalancer)
            .resourceThrottler(resourceThrottler)
            .eventCollector(eventCollector)
            .settingProvider(settingProvider)
            .distClient(distClient)
            .inboxClient(inboxClient)
            .sessionDictClient(sessionDictClient)
            .retainClient(retainClient)
            .buildTcpConnListener()
            .host("127.0.0.1")
            .buildListener()
            .build();

        rpcServer = rpcServerBuilder.build();
        rpcServer.start();
        log.info("Shared RPC server started");
        mqttBroker.start();
        log.info("Mqtt broker started");

        Observable.combineLatest(
                distWorkerStoreClient.connState(),
                inboxStoreKVStoreClient.connState(),
                retainStoreKVStoreClient.connState(),
                mqttServerClient.connState(),
                inboxClient.connState(),
                sessionDictClient.connState(),
                retainClient.connState(),
                distClient.connState(),
                Sets::newHashSet
            )
            .mapOptional(states -> {
                if (states.size() > 1) {
                    return Optional.empty();
                }
                return states.stream().findFirst();
            })
            .filter(state -> state == IRPCClient.ConnState.READY)
            .firstElement()
            .blockingSubscribe();
        await().forever().until(() -> BoundaryUtil.isValidSplitSet(distWorkerStoreClient.latestEffectiveRouter().keySet()));
        await().forever().until(() -> BoundaryUtil.isValidSplitSet(inboxStoreKVStoreClient.latestEffectiveRouter().keySet()));
        await().forever().until(() -> BoundaryUtil.isValidSplitSet(retainStoreKVStoreClient.latestEffectiveRouter().keySet()));
        lenient().when(settingProvider.provide(any(), anyString()))
            .thenAnswer(invocation -> {
                Setting setting = invocation.getArgument(0);
                return setting.current(invocation.getArgument(1));
            });
    }

    @AfterClass(alwaysRun = true, groups = "integration")
    public final void tearDownClass() throws Exception {
        log.info("Start to tearing down");
        mqttBroker.close();
        log.info("Mqtt broker shut down");

        rpcServer.shutdown();
        log.info("Shared rpc server shutdown");

        distServer.close();
        log.info("Dist server shut down");
        distWorker.close();
        log.info("Dist worker stopped");

        inboxBrokerMgr.close();
        inboxServer.close();
        log.info("Inbox server shut down");
        inboxStore.close();
        log.info("Inbox store closed");

        retainServer.close();
        log.info("Retain server shut down");

        retainStore.close();
        log.info("Retain store closed");

        sessionDictServer.close();
        log.info("Session dict server shut down");

        metaService.close();
        trafficService.close();
        crdtService.close();
        log.info("CRDT service stopped");
        agentHost.close();
        log.info("Agent host stopped");

        log.info("Shutdown bg task executor");
        bgTaskExecutor.shutdownNow();
        closeable.close();
    }

    @BeforeMethod(groups = "integration")
    public final void setupTest(Method method) {
        log.info("Test case[{}.{}] start", method.getDeclaringClass().getName(), method.getName());
        tenantId = System.nanoTime() + "";
        lenient().doAnswer(invocationOnMock -> {
            Event event = invocationOnMock.getArgument(0);
            Event cloned = event.getClass().getConstructor().newInstance();
            cloned.clone(event);
            return cloned;
        }).when(eventCollector).report(any(Event.class));
        lenient().when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        doSetup(method);
    }

    protected CompletableFuture<KillReply.Result> kill(String userId, String clientId) {
        return sessionDictClient.kill(System.nanoTime(), tenantId, userId, clientId, ClientInfo.newBuilder()
                    .setTenantId(tenantId)
                    .setType("Killer")
                    .build(),
                ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build())
            .thenApply(KillReply::getResult);
    }

    protected void doSetup(Method method) {
    }

    @AfterMethod(groups = "integration")
    private void tearDownTest(Method method) {
        log.info("Test case[{}.{}] finished", method.getDeclaringClass().getName(), method.getName());
        doTearDown(method);
        Mockito.reset(authProvider, eventCollector, settingProvider);
    }

    protected void doTearDown(Method method) {
    }
}
