/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.baidu.bifromq.mqtt;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.balance.option.KVRangeBalanceControllerOptions;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.localengine.InMemoryKVEngineConfigurator;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.baserpc.utils.NettyUtil;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.server.IDistServer;
import com.baidu.bifromq.dist.worker.IDistWorker;
import com.baidu.bifromq.inbox.client.IInboxBrokerClient;
import com.baidu.bifromq.inbox.client.IInboxReaderClient;
import com.baidu.bifromq.inbox.server.IInboxServer;
import com.baidu.bifromq.inbox.store.IInboxStore;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.inboxbroker.IInboxBrokerManager;
import com.baidu.bifromq.plugin.inboxbroker.InboxBrokerManager;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.retain.client.IRetainServiceClient;
import com.baidu.bifromq.retain.server.IRetainServer;
import com.baidu.bifromq.retain.store.IRetainStore;
import com.baidu.bifromq.sessiondict.client.ISessionDictionaryClient;
import com.baidu.bifromq.sessiondict.server.ISessionDictionaryServer;
import com.baidu.bifromq.type.ClientInfo;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reactivex.rxjava3.core.Observable;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.mockito.MockitoAnnotations;
import org.testng.ITestListener;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.mockito.Mock;
import org.pf4j.DefaultPluginManager;
import org.pf4j.PluginManager;
import org.testng.annotations.Test;

@Slf4j
abstract class MQTTTest implements ITestListener {
    protected static final String brokerURI = "tcp://127.0.0.1:1883";
//    @Rule
//    public TestRule watcher = new TestWatcher() {
//        protected void starting(Description description) {
//            log.info("Starting test: " + description.getMethodName());
//        }
//    };

    @Mock
    protected IAuthProvider authProvider;

    @Mock
    protected IEventCollector eventCollector;

    @Mock
    protected ISettingProvider settingProvider;

    private IAgentHost agentHost;

    private ICRDTService clientCrdtService;
    private ICRDTService serverCrdtService;
    private IMqttBrokerClient onlineInboxBrokerClient;
    protected ISessionDictionaryClient sessionDictClient;
    private ISessionDictionaryServer sessionDictServer;
    private IDistClient distClient;
    private IBaseKVStoreClient distWorkerStoreClient;
    private IDistWorker distWorker;
    private IDistServer distServer;
    private IInboxReaderClient inboxReaderClient;
    private IInboxBrokerClient inboxWriterClient;
    private IBaseKVStoreClient inboxStoreKVStoreClient;
    private IInboxStore inboxStore;
    private IInboxServer inboxServer;
    private IRetainServiceClient retainClient;
    private IBaseKVStoreClient retainStoreKVStoreClient;
    private IRetainStore retainStore;
    private IRetainServer retainServer;
    private IMQTTBroker mqttBroker;
    private IInboxBrokerManager inboxBrokerMgr;
    private PluginManager pluginMgr;
    private ExecutorService ioExecutor;
    private ExecutorService queryExecutor;
    private ExecutorService mutationExecutor;
    private ScheduledExecutorService tickTaskExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    private AutoCloseable closeable;

    @Override
    public void onTestStart(ITestResult result) {
        ITestNGMethod method = result.getMethod();
        Test testAn = method.getConstructorOrMethod().getMethod().getAnnotation(Test.class);
        if (testAn != null) {
            String[] groups = testAn.groups();
            if (groups.length > 0) {
                System.out.println("yes");
            }
        }
        System.out.println(method.getDescription());
    }

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        pluginMgr = new DefaultPluginManager();
        ioExecutor = newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("MQTTTestExecutor-%d").build());
        bgTaskExecutor = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryBuilder().setNameFormat("bg-task-executor").build());
        tickTaskExecutor = new ScheduledThreadPoolExecutor(2,
            new ThreadFactoryBuilder().setNameFormat("tick-task-executor").build());
        queryExecutor = new ThreadPoolExecutor(2, 2, 10, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(20_000),
            new ThreadFactoryBuilder().setNameFormat("query-executor").build());
        mutationExecutor = new ThreadPoolExecutor(2, 2, 10, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(20_000),
            new ThreadFactoryBuilder().setNameFormat("mutation-executor").build());
        AgentHostOptions agentHostOpts = AgentHostOptions.builder()
            .addr("127.0.0.1")
            .port(freePort())
            .baseProbeInterval(Duration.ofSeconds(10))
            .joinRetryInSec(5)
            .joinTimeout(Duration.ofMinutes(5))
            .build();
        agentHost = IAgentHost.newInstance(agentHostOpts);
        agentHost.start();
        log.info("Agent host started");

        clientCrdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        clientCrdtService.start(agentHost);

        serverCrdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        serverCrdtService.start(agentHost);
        log.info("CRDT service started");

        onlineInboxBrokerClient = IMqttBrokerClient.inProcClientBuilder()
            .executor(MoreExecutors.directExecutor())
            .build();
        sessionDictClient = ISessionDictionaryClient.inProcBuilder()
            .executor(MoreExecutors.directExecutor())
            .build();
        sessionDictServer = ISessionDictionaryServer.inProcServerBuilder()
            .executor(MoreExecutors.directExecutor())
            .build();
        inboxReaderClient = IInboxReaderClient.inProcClientBuilder()
            .executor(MoreExecutors.directExecutor())
            .build();
        inboxWriterClient = IInboxBrokerClient.inProcClientBuilder()
            .executor(MoreExecutors.directExecutor())
            .build();
        inboxStoreKVStoreClient = IBaseKVStoreClient.inProcClientBuilder()
            .clusterId(IInboxStore.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
        inboxStore = IInboxStore.inProcBuilder()
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .storeClient(inboxStoreKVStoreClient)
            .eventCollector(eventCollector)
            .ioExecutor(MoreExecutors.directExecutor())
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .kvRangeStoreOptions(new KVRangeStoreOptions()
                .setDataEngineConfigurator(new InMemoryKVEngineConfigurator())
                .setWalEngineConfigurator(new InMemoryKVEngineConfigurator()))
            .build();
        inboxServer = IInboxServer.inProcBuilder()
            .ioExecutor(MoreExecutors.directExecutor())
            .settingProvider(settingProvider)
            .storeClient(inboxStoreKVStoreClient)
            .build();
        distClient = IDistClient.inProcClientBuilder()
            .executor(MoreExecutors.directExecutor())
            .build();

        retainClient = IRetainServiceClient
            .inProcClientBuilder()
            .executor(MoreExecutors.directExecutor())
            .build();
        retainStoreKVStoreClient = IBaseKVStoreClient
            .inProcClientBuilder()
            .clusterId(IRetainStore.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
        retainStore = IRetainStore
            .inProcBuilder()
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .storeClient(retainStoreKVStoreClient)
            .ioExecutor(MoreExecutors.directExecutor())
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .kvRangeStoreOptions(new KVRangeStoreOptions()
                .setDataEngineConfigurator(new InMemoryKVEngineConfigurator())
                .setWalEngineConfigurator(new InMemoryKVEngineConfigurator()))
            .build();
        retainServer = IRetainServer
            .inProcBuilder()
            .ioExecutor(MoreExecutors.directExecutor())
            .settingProvider(settingProvider)
            .storeClient(retainStoreKVStoreClient)
            .build();

        distWorkerStoreClient = IBaseKVStoreClient.inProcClientBuilder()
            .clusterId(IDistWorker.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .build();

        inboxBrokerMgr = new InboxBrokerManager(pluginMgr, onlineInboxBrokerClient, inboxWriterClient);

        KVRangeStoreOptions distWorkerOptions = new KVRangeStoreOptions();
        KVRangeBalanceControllerOptions balanceControllerOptions = new KVRangeBalanceControllerOptions();
        distWorker = IDistWorker.inProcBuilder()
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .distClient(distClient)
            .storeClient(distWorkerStoreClient)
            .ioExecutor(MoreExecutors.directExecutor())
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .kvRangeStoreOptions(distWorkerOptions)
            .balanceControllerOptions(balanceControllerOptions)
            .inboxBrokerManager(inboxBrokerMgr)
            .build();
        distServer = IDistServer.inProcBuilder()
            .storeClient(distWorkerStoreClient)
            .ioExecutor(MoreExecutors.directExecutor())
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .crdtService(clientCrdtService)
            .build();

        mqttBroker = IMQTTBroker.inProcBrokerBuilder()
            .host("127.0.0.1")
            .bossGroup(NettyUtil.createEventLoopGroup(1))
            .workerGroup(NettyUtil.createEventLoopGroup())
            .ioExecutor(MoreExecutors.directExecutor())
            .authProvider(authProvider)
            .eventCollector(eventCollector)
            .settingProvider(settingProvider)
            .distClient(distClient)
            .inboxReader(inboxReaderClient)
            .sessionDictClient(sessionDictClient)
            .retainClient(retainClient)
            .buildTcpConnListener()
            .buildListener()
            .build();

        sessionDictServer.start();
        log.info("Session dict server started");
        inboxStore.start(true);
        log.info("Inbox store started");
        inboxServer.start();
        inboxStoreKVStoreClient.join();
        log.info("Inbox server started");

        retainStore.start(true);
        log.info("Retain store started");
        retainServer.start();
        retainStoreKVStoreClient.join();
        log.info("Retain server started");

        distWorker.start(true);
        log.info("Dist worker started");
        distServer.start();
        distWorkerStoreClient.join();
        log.info("Dist server started");
        mqttBroker.start();
        log.info("Mqtt broker started");

        Observable.combineLatest(
                sessionDictClient.connState(),
                retainClient.connState(),
                inboxReaderClient.connState(),
                distClient.connState(),
                (s1, s2, s3, s4) -> Sets.newHashSet(s1, s2, s3, s4)
            )
            .mapOptional(states -> {
                if (states.size() > 1) {
                    return Optional.empty();
                }
                return states.stream().findFirst();
            })
            .filter(state -> state == IRPCClient.ConnState.READY)
            .blockingFirst();
        lenient().when(settingProvider.provide(any(), any(ClientInfo.class))).thenAnswer(invocation -> {
            Setting setting = invocation.getArgument(0);
            return setting.current(invocation.getArgument(1));
        });
    }

    @AfterMethod
    public void teardown() throws Exception {
        log.info("Start to tearing down");
        mqttBroker.shutdown();
        log.info("Mqtt broker shut down");

        distClient.stop();
        log.info("Dist client stopped");
        distServer.shutdown();
        log.info("Dist worker stopped");
        distWorkerStoreClient.stop();
        log.info("Dist server shut down");
        distWorker.stop();

        inboxBrokerMgr.stop();
        inboxReaderClient.stop();
        log.info("Inbox reader client stopped");
        inboxServer.shutdown();
        log.info("Inbox server shut down");

        inboxStoreKVStoreClient.stop();
        inboxStore.stop();
        log.info("Inbox store closed");

        retainClient.stop();
        log.info("Retain client stopped");
        retainServer.shutdown();
        log.info("Retain server shut down");

        retainStoreKVStoreClient.stop();
        retainStore.stop();
        log.info("Retain store closed");

        sessionDictClient.stop();
        log.info("Session dict client stopped");
        sessionDictServer.shutdown();
        log.info("Session dict server shut down");

        clientCrdtService.stop();
        serverCrdtService.stop();
        log.info("CRDT service stopped");
        agentHost.shutdown();
        log.info("Agent host stopped");

        log.info("Shutdown work executor");
        queryExecutor.shutdownNow();
        log.info("Shutdown tick task executor");
        tickTaskExecutor.shutdownNow();
        log.info("Shutdown bg task executor");
        bgTaskExecutor.shutdownNow();
        closeable.close();
    }

    private int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
