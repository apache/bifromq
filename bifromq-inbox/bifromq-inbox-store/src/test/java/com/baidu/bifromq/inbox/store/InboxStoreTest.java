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

package com.baidu.bifromq.inbox.store;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static com.baidu.bifromq.inbox.util.KeyUtil.inboxPrefix;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
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
import com.baidu.bifromq.baserpc.IConnectable;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.storage.proto.BatchAttachReply;
import com.baidu.bifromq.inbox.storage.proto.BatchAttachRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteReply;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchDetachReply;
import com.baidu.bifromq.inbox.storage.proto.BatchDetachRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchGetReply;
import com.baidu.bifromq.inbox.storage.proto.BatchGetRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertReply;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchSubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchSubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchReply;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.CollectMetricsReply;
import com.baidu.bifromq.inbox.storage.proto.CollectMetricsRequest;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.GCReply;
import com.baidu.bifromq.inbox.storage.proto.GCRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.InboxSubMessagePack;
import com.baidu.bifromq.inbox.util.MessageUtil;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessagePack;
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
abstract class InboxStoreTest {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR_NAME = "testDB_cp";
    private static final String DB_WAL_NAME = "testWAL";
    @Mock
    protected IInboxClient inboxClient;
    @Mock
    protected ISettingProvider settingProvider;
    @Mock
    protected IEventCollector eventCollector;
    protected SimpleMeterRegistry meterRegistry;
    private IAgentHost agentHost;
    private ICRDTService clientCrdtService;
    private ICRDTService serverCrdtService;
    private ExecutorService queryExecutor;
    private ScheduledExecutorService tickTaskExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    public Path dbRootDir;

    protected IBaseKVStoreClient storeClient;
    protected StandaloneInboxStore testStore;

    private AutoCloseable closeable;

    @BeforeClass(groups = "integration")
    public void setup() throws IOException {
        closeable = MockitoAnnotations.openMocks(this);
        dbRootDir = Files.createTempDirectory("");
        when(settingProvider.provide(any(Setting.class), anyString())).thenAnswer(
            invocation -> ((Setting) invocation.getArgument(0)).current(invocation.getArgument(1)));
        meterRegistry = new SimpleMeterRegistry();
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
        ((RocksDBCPableKVEngineConfigurator) options.getDataEngineConfigurator())
            .dbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR_NAME, uuid)
                .toString())
            .dbRootDir(Paths.get(dbRootDir.toString(), DB_NAME, uuid).toString());
        ((RocksDBWALableKVEngineConfigurator) options.getWalEngineConfigurator())
            .dbRootDir(Paths.get(dbRootDir.toString(), DB_WAL_NAME, uuid).toString());
        KVRangeBalanceControllerOptions controllerOptions = new KVRangeBalanceControllerOptions();
        queryExecutor = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
            EnvProvider.INSTANCE.newThreadFactory("query-executor"));
        tickTaskExecutor = new ScheduledThreadPoolExecutor(2,
            EnvProvider.INSTANCE.newThreadFactory("tick-task-executor"));
        bgTaskExecutor = new ScheduledThreadPoolExecutor(1,
            EnvProvider.INSTANCE.newThreadFactory("bg-task-executor"));

        storeClient = IBaseKVStoreClient
            .newBuilder()
            .clusterId(IInboxStore.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .build();
        testStore = (StandaloneInboxStore) IInboxStore.standaloneBuilder()
            .bootstrap(true)
            .host("127.0.0.1")
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .inboxClient(inboxClient)
            .storeClient(storeClient)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .purgeDelay(Duration.ZERO)
            .storeOptions(options)
            .balanceControllerOptions(controllerOptions)
            .queryExecutor(queryExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .gcInterval(Duration.ofSeconds(1))
            .statsInterval(Duration.ofSeconds(1))
            .build();
        testStore.start();

        storeClient.connState().filter(connState -> connState == IConnectable.ConnState.READY).blockingFirst();
        storeClient.join();
        log.info("Setup finished, and start testing");
    }

    @AfterClass(groups = "integration")
    public void teardown() throws Exception {
        log.info("Finish testing, and tearing down");
        new Thread(() -> {
            testStore.stop();
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
    public void beforeCastStart(Method method) {
        log.info("Test case[{}.{}] start", method.getDeclaringClass().getName(), method.getName());
    }

    @AfterMethod(alwaysRun = true)
    public void afterCaseFinish(Method method) {
        log.info("Test case[{}.{}] finished, doing teardown",
            method.getDeclaringClass().getName(), method.getName());
    }

    private InboxServiceROCoProcOutput query(ByteString routeKey, InboxServiceROCoProcInput input) {
        KVRangeSetting s = storeClient.findByKey(routeKey).get();
        return query(s, input);
    }

    private InboxServiceROCoProcOutput query(KVRangeSetting s, InboxServiceROCoProcInput input) {
        KVRangeROReply reply = storeClient.query(s.leader, KVRangeRORequest.newBuilder()
            .setReqId(input.getReqId())
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRoCoProc(ROCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), input.getReqId());
        assertEquals(reply.getCode(), ReplyCode.Ok);
        return reply.getRoCoProcResult().getInboxService();
    }


    private InboxServiceRWCoProcOutput mutate(ByteString routeKey, InboxServiceRWCoProcInput input) {
        KVRangeSetting s = storeClient.findByKey(routeKey).get();
        KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
            .setReqId(input.getReqId())
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRwCoProc(RWCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), input.getReqId());
        assertEquals(reply.getCode(), ReplyCode.Ok);
        return reply.getRwCoProcResult().getInboxService();
    }

    protected GCReply requestGCScan(GCRequest request) {
        long reqId = ThreadLocalRandom.current().nextInt();
        InboxServiceROCoProcInput input = InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setGc(request)
            .build();
        List<KVRangeSetting> rangeSettings = storeClient.findByBoundary(FULL_BOUNDARY);
        assert !rangeSettings.isEmpty();
        InboxServiceROCoProcOutput output = query(rangeSettings.get(0), input);
        assertTrue(output.hasGc());
        assertEquals(output.getReqId(), reqId);
        return output.getGc();
    }

    protected CollectMetricsReply requestCollectMetrics(CollectMetricsRequest request) {
        long reqId = ThreadLocalRandom.current().nextInt();
        InboxServiceROCoProcInput input = InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setCollectMetrics(request)
            .build();
        List<KVRangeSetting> rangeSettings = storeClient.findByBoundary(FULL_BOUNDARY);
        assert !rangeSettings.isEmpty();
        InboxServiceROCoProcOutput output = query(rangeSettings.get(0), input);
        assertTrue(output.hasCollectedMetrics());
        assertEquals(output.getReqId(), reqId);
        return output.getCollectedMetrics();
    }

    protected List<BatchGetReply.Result> requestGet(BatchGetRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxPrefix(params[0].getTenantId(), params[0].getInboxId());
        InboxServiceROCoProcInput input = MessageUtil.buildGetRequest(reqId, BatchGetRequest.newBuilder()
            .addAllParams(List.of(params))
            .build());
        InboxServiceROCoProcOutput output = query(routeKey, input);
        assertTrue(output.hasBatchGet());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchGet().getResultList();
    }

    protected List<Fetched> requestFetch(BatchFetchRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxPrefix(params[0].getTenantId(), params[0].getInboxId());
        InboxServiceROCoProcInput input = MessageUtil.buildFetchRequest(reqId, BatchFetchRequest.newBuilder()
            .addAllParams(List.of(params))
            .build());
        InboxServiceROCoProcOutput output = query(routeKey, input);
        assertTrue(output.hasBatchFetch());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchFetch().getResultList();
    }

    protected List<BatchAttachReply.Result> requestAttach(BatchAttachRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxPrefix(params[0].getClient().getTenantId(), params[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil.buildAttachRequest(reqId,
            BatchAttachRequest.newBuilder().addAllParams(List.of(params)).build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchAttach());
        assertEquals(output.getReqId(), reqId);
        assertEquals(params.length, output.getBatchAttach().getResultCount());
        return output.getBatchAttach().getResultList();
    }

    protected List<BatchDetachReply.Result> requestDetach(BatchDetachRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxPrefix(params[0].getTenantId(), params[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil.buildDetachRequest(reqId,
            BatchDetachRequest.newBuilder().addAllParams(List.of(params)).build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchDetach());
        assertEquals(output.getReqId(), reqId);
        assertEquals(params.length, output.getBatchDetach().getResultCount());
        return output.getBatchDetach().getResultList();
    }

    protected List<Boolean> requestCreate(BatchCreateRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxPrefix(params[0].getClient().getTenantId(), params[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil.buildCreateRequest(reqId,
            BatchCreateRequest.newBuilder().addAllParams(List.of(params)).build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchCreate());
        assertEquals(output.getReqId(), reqId);
        assertEquals(params.length, output.getBatchCreate().getSucceedCount());
        return output.getBatchCreate().getSucceedList();
    }

    protected List<BatchDeleteReply.Result> requestDelete(BatchDeleteRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxPrefix(params[0].getTenantId(), params[0].getInboxId());

        InboxServiceRWCoProcInput input = MessageUtil.buildDeleteRequest(reqId, BatchDeleteRequest.newBuilder()
            .addAllParams(List.of(params)).build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchDelete());
        assertEquals(output.getReqId(), reqId);
        assertEquals(params.length, output.getBatchDelete().getResultCount());
        return output.getBatchDelete().getResultList();
    }

    protected List<BatchSubReply.Code> requestSub(BatchSubRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxPrefix(params[0].getTenantId(), params[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil
            .buildSubRequest(reqId, BatchSubRequest.newBuilder().addAllParams(List.of(params)).build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchSub());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchSub().getCodeList();
    }

    protected List<BatchUnsubReply.Code> requestUnsub(BatchUnsubRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxPrefix(params[0].getTenantId(), params[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil.buildUnsubRequest(reqId, BatchUnsubRequest.newBuilder()
            .addAllParams(List.of(params))
            .build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchUnsub());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchUnsub().getCodeList();
    }

    protected List<BatchTouchReply.Code> requestTouch(BatchTouchRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxPrefix(params[0].getTenantId(), params[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil.buildTouchRequest(reqId, BatchTouchRequest.newBuilder()
            .addAllParams(List.of(params))
            .build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchTouch());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchTouch().getCodeList();
    }

    protected List<BatchInsertReply.Result> requestInsert(InboxSubMessagePack... inboxSubMessagePack) {
        assert inboxSubMessagePack.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxPrefix(inboxSubMessagePack[0].getTenantId(), inboxSubMessagePack[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil.buildInsertRequest(reqId, BatchInsertRequest.newBuilder()
            .addAllInboxSubMsgPack(List.of(inboxSubMessagePack))
            .build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchInsert());
        assertEquals(output.getReqId(), reqId);
        assertEquals(inboxSubMessagePack.length, output.getBatchInsert().getResultCount());
        return output.getBatchInsert().getResultList();
    }

    protected List<BatchCommitReply.Code> requestCommit(BatchCommitRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxPrefix(params[0].getTenantId(), params[0].getInboxId());

        InboxServiceRWCoProcInput input = MessageUtil.buildCommitRequest(reqId, BatchCommitRequest.newBuilder()
            .addAllParams(List.of(params))
            .build());

        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchCommit());
        assertEquals(output.getReqId(), reqId);
        assertEquals(params.length, output.getBatchCommit().getCodeCount());
        return output.getBatchCommit().getCodeList();
    }

    protected TopicMessagePack.PublisherPack message(QoS qos, String payload) {
        return TopicMessagePack.PublisherPack.newBuilder()
            .addMessage(Message.newBuilder()
                .setMessageId(System.nanoTime())
                .setPubQoS(qos)
                .setPayload(ByteString.copyFromUtf8(payload))
                .build())
            .build();
    }

    protected TopicMessagePack.PublisherPack message(int messageId, QoS qos, String payload, ClientInfo publisher) {
        return TopicMessagePack.PublisherPack.newBuilder()
            .addMessage(Message.newBuilder()
                .setMessageId(messageId)
                .setPubQoS(qos)
                .setPayload(ByteString.copyFromUtf8(payload))
                .build())
            .setPublisher(publisher)
            .build();
    }
}
