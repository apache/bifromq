/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.dist.worker;

import static com.baidu.bifromq.dist.entity.EntityUtil.toMatchRecordKey;
import static com.baidu.bifromq.dist.entity.EntityUtil.toQInboxId;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ADDRESS_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_3_1_1_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;
import static org.mockito.Mockito.lenient;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.balance.option.KVRangeBalanceControllerOptions;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.localengine.memory.InMemKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBCPableKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBWALableKVEngineConfigurator;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.basekv.store.proto.KVRangeROReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.baserpc.utils.NettyUtil;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.entity.EntityUtil;
import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.dist.rpc.proto.BatchDistRequest;
import com.baidu.bifromq.dist.rpc.proto.BatchMatchReply;
import com.baidu.bifromq.dist.rpc.proto.BatchMatchRequest;
import com.baidu.bifromq.dist.rpc.proto.BatchUnmatchReply;
import com.baidu.bifromq.dist.rpc.proto.BatchUnmatchRequest;
import com.baidu.bifromq.dist.rpc.proto.DistPack;
import com.baidu.bifromq.dist.rpc.proto.DistServiceROCoProcOutput;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import com.baidu.bifromq.dist.util.MessageUtil;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.plugin.subbroker.ISubBroker;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.common.util.concurrent.MoreExecutors;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

@Slf4j
public abstract class DistWorkerTest {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR_NAME = "testDB_cp";
    private static final String DB_WAL_NAME = "testWAL";
    private static final String DB_WAL_CHECKPOINT_DIR = "testWAL_cp";

    protected static final int MqttBroker = 0;
    protected static final int InboxService = 1;

    private IAgentHost agentHost;
    private ICRDTService clientCrdtService;
    private ICRDTService serverCrdtService;
    @Mock
    protected IEventCollector eventCollector;
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
    protected IDistWorker testWorker;
    protected IBaseKVStoreClient storeClient;

    protected String tenantA = "tenantA";
    protected String tenantB = "tenantB";
    private ExecutorService queryExecutor;
    private ScheduledExecutorService tickTaskExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    private Path dbRootDir;

    private static boolean runOnMac() {
        String osName = System.getProperty("os.name");
        return osName != null && osName.startsWith("Mac");
    }

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
        lenient().when(settingProvider.provide(Setting.MaxTopicFiltersPerInbox, tenantA)).thenReturn(200);
        lenient().when(settingProvider.provide(Setting.MaxTopicFiltersPerInbox, tenantB)).thenReturn(200);
        lenient().when(settingProvider.provide(Setting.MaxSharedGroupMembers, tenantA)).thenReturn(200);
        lenient().when(settingProvider.provide(Setting.MaxSharedGroupMembers, tenantB)).thenReturn(200);
        lenient().when(receiverManager.get(MqttBroker)).thenReturn(mqttBroker);
        lenient().when(receiverManager.get(InboxService)).thenReturn(inboxBroker);

        queryExecutor = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
            EnvProvider.INSTANCE.newThreadFactory("query-executor"));
        tickTaskExecutor = new ScheduledThreadPoolExecutor(2,
            EnvProvider.INSTANCE.newThreadFactory("tick-task-executor"));
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
        agentHost.start();
        clientCrdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        clientCrdtService.start(agentHost);

        serverCrdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        serverCrdtService.start(agentHost);

        String uuid = UUID.randomUUID().toString();
        KVRangeStoreOptions options = new KVRangeStoreOptions();
        if (!runOnMac()) {
            options.setDataEngineConfigurator(new InMemKVEngineConfigurator());
            options.setWalEngineConfigurator(new InMemKVEngineConfigurator());
        } else {
            ((RocksDBCPableKVEngineConfigurator) options.getDataEngineConfigurator())
                .dbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR_NAME, uuid)
                    .toString())
                .dbRootDir(Paths.get(dbRootDir.toString(), DB_NAME, uuid).toString());
            ((RocksDBWALableKVEngineConfigurator) options.getWalEngineConfigurator())
                .dbRootDir(Paths.get(dbRootDir.toString(), DB_WAL_NAME, uuid).toString());
        }

        KVRangeBalanceControllerOptions balanceControllerOptions = new KVRangeBalanceControllerOptions();

        storeClient = IBaseKVStoreClient
            .newBuilder()
            .eventLoopGroup(NettyUtil.createEventLoopGroup())
            .clusterId(IDistWorker.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
        testWorker = IDistWorker.standaloneBuilder()
            .host("127.0.0.1")
            .bossEventLoopGroup(NettyUtil.createEventLoopGroup(1))
            .workerEventLoopGroup(NettyUtil.createEventLoopGroup())
            .ioExecutor(MoreExecutors.directExecutor())
            .bootstrap(true)
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .eventCollector(eventCollector)
            .settingProvider(settingProvider)
            .distClient(distClient)
            .storeClient(storeClient)
            .queryExecutor(queryExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .balanceControllerOptions(balanceControllerOptions)
            .statsInterval(Duration.ofSeconds(1))
            .storeOptions(options)
            .subBrokerManager(receiverManager)
            .build();
        testWorker.start();
        storeClient.join();
        log.info("Setup finished, and start testing");
    }

    @AfterClass(alwaysRun = true)
    public void teardown() throws Exception {
        log.info("Finish testing, and tearing down");
        new Thread(() -> {
            storeClient.stop();
            testWorker.stop();
            clientCrdtService.stop();
            serverCrdtService.stop();
            agentHost.shutdown();
            try {
                Files.walk(dbRootDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            } catch (IOException e) {
                log.error("Failed to delete db root dir", e);
            }
            queryExecutor.shutdown();
            tickTaskExecutor.shutdown();
            bgTaskExecutor.shutdown();
        }).start();
        closeable.close();
    }

    @BeforeMethod(alwaysRun = true)
    public void printCaseStart(Method method) {
        log.info("Test case[{}.{}] start", method.getDeclaringClass().getName(), method.getName());
    }

    @AfterMethod(alwaysRun = true)
    public void printCaseFinish(Method method) {
        log.info("Test case[{}.{}] finished, doing teardown",
            method.getDeclaringClass().getName(), method.getName());
    }

    protected BatchMatchReply.Result sub(String tenantId, String topicFilter, QoS subQoS,
                                         int subBroker, String inboxId, String delivererKey) {
        long reqId = ThreadLocalRandom.current().nextInt();
        String qInboxId = toQInboxId(subBroker, inboxId, delivererKey);
        KVRangeSetting s = storeClient.findByKey(toMatchRecordKey(tenantId, topicFilter, qInboxId)).get();
        String scopedTopicFilter = EntityUtil.toScopedTopicFilter(tenantId, qInboxId, topicFilter);
        DistServiceRWCoProcInput input = DistServiceRWCoProcInput.newBuilder()
            .setBatchMatch(BatchMatchRequest.newBuilder()
                .setReqId(reqId)
                .putScopedTopicFilter(EntityUtil.toScopedTopicFilter(tenantId, qInboxId, topicFilter), subQoS)
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
        return batchMatchReply.getResultsMap().get(scopedTopicFilter);
    }

    protected BatchUnmatchReply.Result unsub(String tenantId, String topicFilter, int subBroker, String inboxId,
                                             String delivererKey) {
        long reqId = ThreadLocalRandom.current().nextInt();
        String qInboxId = toQInboxId(subBroker, inboxId, delivererKey);
        KVRangeSetting s = storeClient.findByKey(toMatchRecordKey(tenantId, topicFilter, qInboxId)).get();
        String scopedTopicFilter = EntityUtil.toScopedTopicFilter(tenantId, qInboxId, topicFilter);
        DistServiceRWCoProcInput input = DistServiceRWCoProcInput.newBuilder()
            .setBatchUnmatch(BatchUnmatchRequest.newBuilder()
                .setReqId(reqId)
                .addScopedTopicFilter(scopedTopicFilter)
                .build()).build();
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
        return batchUnmatchReply.getResultsMap().get(scopedTopicFilter);
    }

    protected BatchDistReply dist(String tenantId, List<TopicMessagePack> msgs, String orderKey) {
        long reqId = ThreadLocalRandom.current().nextInt();
        KVRangeSetting s = storeClient.findByKey(EntityUtil.matchRecordKeyPrefix(tenantId)).get();
        BatchDistRequest request = BatchDistRequest.newBuilder()
            .setReqId(reqId)
            .addDistPack(DistPack.newBuilder()
                .setTenantId(tenantId)
                .addAllMsgPack(msgs)
                .build())
            .setOrderKey(orderKey)
            .build();
        ROCoProcInput input = ROCoProcInput.newBuilder()
            .setDistService(MessageUtil.buildBatchDistRequest(request))
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
                    .setTimestamp(System.currentTimeMillis())
                    .setExpireTimestamp(Long.MAX_VALUE)
                    .build())
                .build())
            .build()), orderKey);
    }
}
