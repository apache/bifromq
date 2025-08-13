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

package org.apache.bifromq.dist.worker;

import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.tenantBeginKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toGroupRouteKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toNormalRouteKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toReceiverUrl;
import static org.apache.bifromq.plugin.subbroker.TypeUtil.to;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ADDRESS_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_3_1_1_VALUE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.reset;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basecluster.AgentHostOptions;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basecrdt.service.CRDTServiceOptions;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.baseenv.NettyEnv;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.localengine.rocksdb.RocksDBCPableKVEngineConfigurator;
import org.apache.bifromq.basekv.localengine.rocksdb.RocksDBWALableKVEngineConfigurator;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.basekv.store.option.KVRangeStoreOptions;
import org.apache.bifromq.basekv.store.proto.KVRangeROReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.KVRangeRWReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRWRequest;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.baserpc.server.IRPCServer;
import org.apache.bifromq.baserpc.server.RPCServerBuilder;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.dist.rpc.proto.BatchDistReply;
import org.apache.bifromq.dist.rpc.proto.BatchDistRequest;
import org.apache.bifromq.dist.rpc.proto.BatchMatchReply;
import org.apache.bifromq.dist.rpc.proto.BatchMatchRequest;
import org.apache.bifromq.dist.rpc.proto.BatchUnmatchReply;
import org.apache.bifromq.dist.rpc.proto.BatchUnmatchRequest;
import org.apache.bifromq.dist.rpc.proto.DistPack;
import org.apache.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import org.apache.bifromq.dist.rpc.proto.DistServiceROCoProcOutput;
import org.apache.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import org.apache.bifromq.dist.rpc.proto.MatchRoute;
import org.apache.bifromq.dist.rpc.proto.TenantOption;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.plugin.subbroker.DeliveryPack;
import org.apache.bifromq.plugin.subbroker.DeliveryPackage;
import org.apache.bifromq.plugin.subbroker.DeliveryReply;
import org.apache.bifromq.plugin.subbroker.DeliveryRequest;
import org.apache.bifromq.plugin.subbroker.DeliveryResult;
import org.apache.bifromq.plugin.subbroker.IDeliverer;
import org.apache.bifromq.plugin.subbroker.ISubBroker;
import org.apache.bifromq.plugin.subbroker.ISubBrokerManager;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.type.TopicMessagePack;
import org.apache.bifromq.util.TopicUtil;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

@Slf4j
public abstract class DistWorkerTest {
    protected static final int MqttBroker = 0;
    protected static final int InboxService = 1;
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR_NAME = "testDB_cp";
    private static final String DB_WAL_NAME = "testWAL";
    private static final String DB_WAL_CHECKPOINT_DIR = "testWAL_cp";
    private final int tickerThreads = 2;
    @Mock
    protected IEventCollector eventCollector;
    @Mock
    protected IResourceThrottler resourceThrottler;
    @Mock
    protected ISettingProvider settingProvider;
    @Mock
    protected IDistClient distClient;
    @Mock
    protected ISubBrokerManager receiverManager;
    @Mock
    protected ISubBroker mqttBroker;
    @Mock
    protected ISubBroker inboxBroker;
    @Mock
    protected IDeliverer writer1;
    @Mock
    protected IDeliverer writer2;
    @Mock
    protected IDeliverer writer3;
    protected SimpleMeterRegistry meterRegistry;
    protected IRPCServer rpcServer;
    protected IDistWorker testWorker;
    protected IBaseKVStoreClient storeClient;
    protected String tenantA = "tenantA";
    protected String tenantB = "tenantB";
    private IAgentHost agentHost;
    private ICRDTService crdtService;
    private IRPCServiceTrafficService trafficService;
    private IBaseKVMetaService metaService;
    private ScheduledExecutorService bgTaskExecutor;
    private Path dbRootDir;

    private AutoCloseable closeable;

    @BeforeClass(alwaysRun = true)
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        System.setProperty("dist_worker_topic_match_expiry_seconds", "1");
        meterRegistry = new SimpleMeterRegistry();
        try {
            dbRootDir = Files.createTempDirectory("");
        } catch (IOException e) {
        }
        lenient().when(receiverManager.get(MqttBroker)).thenReturn(mqttBroker);
        lenient().when(receiverManager.get(InboxService)).thenReturn(inboxBroker);
        lenient().when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        lenient().when(settingProvider.provide(any(), anyString())).thenAnswer(invocation -> {
            Setting setting = invocation.getArgument(0);
            String tenantId = invocation.getArgument(1);
            return setting.current(tenantId);
        });

        bgTaskExecutor = new ScheduledThreadPoolExecutor(1,
            EnvProvider.INSTANCE.newThreadFactory("bg-task-executor"));

        Metrics.globalRegistry.add(meterRegistry);

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

        String uuid = UUID.randomUUID().toString();
        KVRangeStoreOptions options = new KVRangeStoreOptions();
        ((RocksDBCPableKVEngineConfigurator) options.getDataEngineConfigurator())
            .dbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR_NAME, uuid)
                .toString())
            .dbRootDir(Paths.get(dbRootDir.toString(), DB_NAME, uuid).toString());
        ((RocksDBWALableKVEngineConfigurator) options.getWalEngineConfigurator())
            .dbRootDir(Paths.get(dbRootDir.toString(), DB_WAL_NAME, uuid).toString());

        storeClient = IBaseKVStoreClient
            .newBuilder()
            .eventLoopGroup(NettyEnv.createEventLoopGroup("store-client"))
            .clusterId(IDistWorker.CLUSTER_NAME)
            .trafficService(trafficService)
            .metaService(metaService)
            .build();
        RPCServerBuilder rpcServerBuilder = IRPCServer.newBuilder().host("127.0.0.1").trafficService(trafficService);
        testWorker = IDistWorker.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .agentHost(agentHost)
            .metaService(metaService)
            .eventCollector(eventCollector)
            .resourceThrottler(resourceThrottler)
            .distClient(distClient)
            .distWorkerClient(storeClient)
            .tickerThreads(tickerThreads)
            .bgTaskExecutor(bgTaskExecutor)
            .storeOptions(options)
            .subBrokerManager(receiverManager)
            .settingProvider(settingProvider)
            .inlineFanoutThreshold(1)
            .build();
        rpcServer = rpcServerBuilder.build();
        rpcServer.start();
        await().until(() -> BoundaryUtil.isValidSplitSet(storeClient.latestEffectiveRouter().keySet()));
        log.info("Setup finished, and start testing");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws Exception {
        log.info("Finish testing, and tearing down");
        storeClient.close();
        testWorker.close();
        rpcServer.shutdown();
        metaService.close();
        trafficService.close();
        crdtService.close();
        agentHost.close();
        try {
            Files.walk(dbRootDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        } catch (IOException e) {
            log.error("Failed to delete db root dir", e);
        }
        bgTaskExecutor.shutdown();
        closeable.close();
    }

    @BeforeMethod(alwaysRun = true)
    public void printCaseStart(Method method) {
        log.info("Test case[{}.{}] start", method.getDeclaringClass().getName(), method.getName());
        tenantA = "tenantA" + System.nanoTime();
        tenantB = "tenantB" + System.nanoTime();

    }

    @AfterMethod(alwaysRun = true)
    public void printCaseFinish(Method method) {
        log.info("Test case[{}.{}] finished, doing teardown",
            method.getDeclaringClass().getName(), method.getName());
        reset(eventCollector);
    }

    protected BatchMatchReply.TenantBatch.Code match(String tenantId,
                                                     String topicFilter,
                                                     int subBroker,
                                                     String inboxId,
                                                     String delivererKey) {
        return match(tenantId, topicFilter, subBroker, inboxId, delivererKey, 100);
    }

    protected BatchMatchReply.TenantBatch.Code match(String tenantId,
                                                     String topicFilter,
                                                     int subBroker,
                                                     String inboxId,
                                                     String delivererKey,
                                                     long incarnation) {
        return match(tenantId, topicFilter, subBroker, inboxId, delivererKey, incarnation, 100);
    }

    protected BatchMatchReply.TenantBatch.Code match(String tenantId,
                                                     String topicFilter,
                                                     int subBroker,
                                                     String inboxId,
                                                     String delivererKey,
                                                     int maxMembersPerSharedSubGroup) {
        return match(tenantId, topicFilter, subBroker, inboxId, delivererKey, 0L, maxMembersPerSharedSubGroup);
    }

    protected BatchMatchReply.TenantBatch.Code match(String tenantId,
                                                     String topicFilter,
                                                     int subBroker,
                                                     String inboxId,
                                                     String delivererKey,
                                                     long incarnation,
                                                     int maxMembersPerSharedSubGroup) {
        long reqId = ThreadLocalRandom.current().nextInt();
        MatchRoute route = MatchRoute.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setBrokerId(subBroker)
            .setReceiverId(inboxId)
            .setDelivererKey(delivererKey)
            .setIncarnation(incarnation)
            .build();
        return match(tenantId, maxMembersPerSharedSubGroup, route).get(0);
    }

    protected List<BatchMatchReply.TenantBatch.Code> match(String tenantId,
                                                           int maxMembersPerSharedSubGroup,
                                                           MatchRoute... routes) {
        long reqId = ThreadLocalRandom.current().nextInt();
        KVRangeSetting s = findByKey(tenantBeginKey(tenantId), storeClient.latestEffectiveRouter()).get();
        DistServiceRWCoProcInput input = DistServiceRWCoProcInput.newBuilder()
            .setBatchMatch(BatchMatchRequest.newBuilder()
                .setReqId(reqId)
                .putRequests(tenantId, BatchMatchRequest.TenantBatch.newBuilder()
                    .setOption(TenantOption.newBuilder()
                        .setMaxReceiversPerSharedSubGroup(maxMembersPerSharedSubGroup)
                        .build())
                    .addAllRoute(List.of(routes))
                    .build())
                .build())
            .build();
        KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRwCoProc(RWCoProcInput.newBuilder().setDistService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        BatchMatchReply batchMatchReply = reply.getRwCoProcResult().getDistService().getBatchMatch();
        assertEquals(batchMatchReply.getReqId(), reqId);
        return batchMatchReply.getResultsMap().get(tenantId).getCodeList();
    }

    protected BatchUnmatchReply.TenantBatch.Code unmatch(String tenantId,
                                                         String topicFilter,
                                                         int subBroker,
                                                         String inboxId,
                                                         String delivererKey) {
        return unmatch(tenantId, topicFilter, subBroker, inboxId, delivererKey, 0L);
    }

    protected BatchUnmatchReply.TenantBatch.Code unmatch(String tenantId,
                                                         String topicFilter,
                                                         int subBroker,
                                                         String inboxId,
                                                         String delivererKey,
                                                         long incarnation) {
        long reqId = ThreadLocalRandom.current().nextInt();
        RouteMatcher filter = TopicUtil.from(topicFilter);
        MatchRoute route = MatchRoute.newBuilder()
            .setMatcher(filter)
            .setBrokerId(subBroker)
            .setReceiverId(inboxId)
            .setDelivererKey(delivererKey)
            .setIncarnation(incarnation)
            .build();
        ByteString routeKey = filter.getType() == RouteMatcher.Type.Normal
            ? toNormalRouteKey(tenantId, filter, toReceiverUrl(route)) : toGroupRouteKey(tenantId, filter);
        KVRangeSetting s = findByKey(routeKey, storeClient.latestEffectiveRouter()).get();
        DistServiceRWCoProcInput input = DistServiceRWCoProcInput.newBuilder()
            .setBatchUnmatch(BatchUnmatchRequest.newBuilder()
                .setReqId(reqId)
                .putRequests(tenantId, BatchUnmatchRequest.TenantBatch.newBuilder()
                    .addRoute(route)
                    .build())
                .build())
            .build();
        KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRwCoProc(RWCoProcInput.newBuilder().setDistService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        BatchUnmatchReply batchUnmatchReply = reply.getRwCoProcResult().getDistService().getBatchUnmatch();
        assertEquals(batchUnmatchReply.getReqId(), reqId);
        return batchUnmatchReply.getResultsMap().get(tenantId).getCode(0);
    }

    protected BatchDistReply dist(String tenantId, List<TopicMessagePack> msgs, String orderKey) {
        long reqId = ThreadLocalRandom.current().nextInt();
        KVRangeSetting s = findByKey(tenantBeginKey(tenantId), storeClient.latestEffectiveRouter()).get();
        BatchDistRequest request = BatchDistRequest.newBuilder()
            .setReqId(reqId)
            .addDistPack(DistPack.newBuilder()
                .setTenantId(tenantId)
                .addAllMsgPack(msgs)
                .build())
            .setOrderKey(orderKey)
            .build();
        ROCoProcInput input = ROCoProcInput.newBuilder()
            .setDistService(DistServiceROCoProcInput.newBuilder()
                .setBatchDist(request)
                .build())
            .build();
        KVRangeROReply reply = storeClient.query(s.leader, KVRangeRORequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRoCoProc(input)
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        DistServiceROCoProcOutput output = reply.getRoCoProcResult().getDistService();
        assertTrue(output.hasBatchDist());
        assertEquals(output.getBatchDist().getReqId(), reqId);
        return output.getBatchDist();
    }

    protected BatchDistReply dist(String tenantId, QoS qos, String topic, ByteString payload, String orderKey) {
        return dist(tenantId, List.of(TopicMessagePack.newBuilder()
            .setTopic(topic)
            .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                .setPublisher(ClientInfo.newBuilder()
                    .setTenantId(tenantId)
                    .setType(MQTT_TYPE_VALUE)
                    .putMetadata(MQTT_PROTOCOL_VER_KEY, MQTT_PROTOCOL_VER_3_1_1_VALUE)
                    .putMetadata(MQTT_USER_ID_KEY, "testUser")
                    .putMetadata(MQTT_CLIENT_ID_KEY, "testClientId")
                    .putMetadata(MQTT_CLIENT_ADDRESS_KEY, "127.0.0.1:8080")
                    .build())
                .addMessage(Message.newBuilder()
                    .setMessageId(ThreadLocalRandom.current().nextInt())
                    .setPubQoS(qos)
                    .setPayload(payload)
                    .setTimestamp(HLC.INST.get())
                    .build())
                .build())
            .build()), orderKey);
    }

    protected Answer<CompletableFuture<DeliveryReply>> answer(DeliveryResult.Code code) {
        return invocation -> {
            DeliveryRequest request = invocation.getArgument(0);
            DeliveryReply.Builder replyBuilder = DeliveryReply.newBuilder();
            for (Map.Entry<String, DeliveryPackage> entry : request.getPackageMap().entrySet()) {
                String tenantId = entry.getKey();
                Map<MatchInfo, DeliveryResult.Code> resultMap = new HashMap<>();
                for (DeliveryPack pack : entry.getValue().getPackList()) {
                    for (MatchInfo subInfo : pack.getMatchInfoList()) {
                        resultMap.put(subInfo, code);
                    }
                }
                replyBuilder.setCode(DeliveryReply.Code.OK).putResult(tenantId, to(resultMap));
            }
            return CompletableFuture.completedFuture(replyBuilder.build());
        };
    }

}
