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

package org.apache.bifromq.inbox.store;

import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByBoundary;
import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByKey;
import static org.apache.bifromq.basekv.localengine.StructUtil.toValue;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_CHECKPOINT_ROOT_DIR;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_ROOT_DIR;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.inboxStartKeyPrefix;
import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentSessionNumGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentSessionSpaceGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentSubCountGauge;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basecluster.AgentHostOptions;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basecrdt.service.CRDTServiceOptions;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.KVRangeSetting;
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
import org.apache.bifromq.baserpc.client.IConnectable;
import org.apache.bifromq.baserpc.server.IRPCServer;
import org.apache.bifromq.baserpc.server.RPCServerBuilder;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.inbox.storage.proto.BatchAttachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchCommitReply;
import org.apache.bifromq.inbox.storage.proto.BatchCommitRequest;
import org.apache.bifromq.inbox.storage.proto.BatchDeleteReply;
import org.apache.bifromq.inbox.storage.proto.BatchDeleteRequest;
import org.apache.bifromq.inbox.storage.proto.BatchDetachReply;
import org.apache.bifromq.inbox.storage.proto.BatchDetachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchExistRequest;
import org.apache.bifromq.inbox.storage.proto.BatchFetchRequest;
import org.apache.bifromq.inbox.storage.proto.BatchInsertRequest;
import org.apache.bifromq.inbox.storage.proto.BatchSendLWTReply;
import org.apache.bifromq.inbox.storage.proto.BatchSendLWTRequest;
import org.apache.bifromq.inbox.storage.proto.BatchSubReply;
import org.apache.bifromq.inbox.storage.proto.BatchSubRequest;
import org.apache.bifromq.inbox.storage.proto.BatchUnsubReply;
import org.apache.bifromq.inbox.storage.proto.BatchUnsubRequest;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import org.apache.bifromq.inbox.storage.proto.GCReply;
import org.apache.bifromq.inbox.storage.proto.GCRequest;
import org.apache.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import org.apache.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import org.apache.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import org.apache.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.InsertRequest;
import org.apache.bifromq.inbox.storage.proto.InsertResult;
import org.apache.bifromq.metrics.TenantMetric;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.retain.client.IRetainClient;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicMessagePack;
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
    private final int tickerThreads = 2;
    public Path dbRootDir;
    @Mock
    protected IDistClient distClient;
    @Mock
    protected IInboxClient inboxClient;
    @Mock
    protected IRetainClient retainClient;
    @Mock
    protected ISessionDictClient sessionDictClient;
    @Mock
    protected ISettingProvider settingProvider;
    @Mock
    protected IEventCollector eventCollector;
    @Mock
    protected IResourceThrottler resourceThrottler;
    protected SimpleMeterRegistry meterRegistry;
    protected IBaseKVStoreClient storeClient;
    protected IInboxStore testStore;
    private IAgentHost agentHost;
    private ICRDTService crdtService;
    private IRPCServiceTrafficService trafficService;
    private IRPCServer rpcServer;
    private IBaseKVMetaService metaService;
    private ExecutorService queryExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    private KVRangeStoreOptions options;
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

        crdtService = ICRDTService.newInstance(agentHost, CRDTServiceOptions.builder().build());

        trafficService = IRPCServiceTrafficService.newInstance(crdtService);
        metaService = IBaseKVMetaService.newInstance(crdtService);

        String uuid = UUID.randomUUID().toString();
        options = new KVRangeStoreOptions();
        Struct dataConf = options.getDataEngineConf().toBuilder()
            .putFields(DB_ROOT_DIR, toValue(Paths.get(dbRootDir.toString(), DB_NAME, uuid).toString()))
            .putFields(DB_CHECKPOINT_ROOT_DIR,
                toValue(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR_NAME, uuid).toString()))
            .build();
        options.setDataEngineType(options.getDataEngineType());
        options.setDataEngineConf(dataConf);
        Struct walConf = options.getWalEngineConf().toBuilder()
            .putFields(DB_ROOT_DIR, toValue(Paths.get(dbRootDir.toString(), DB_WAL_NAME, uuid).toString()))
            .build();
        options.setWalEngineType(options.getWalEngineType());
        options.setWalEngineConf(walConf);
        queryExecutor = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
            EnvProvider.INSTANCE.newThreadFactory("query-executor"));
        bgTaskExecutor = new ScheduledThreadPoolExecutor(1,
            EnvProvider.INSTANCE.newThreadFactory("bg-task-executor"));

        storeClient = IBaseKVStoreClient
            .newBuilder()
            .clusterId(IInboxStore.CLUSTER_NAME)
            .trafficService(trafficService)
            .metaService(metaService)
            .build();
        buildStoreServer();
        rpcServer.start();

        storeClient.connState().filter(connState -> connState == IConnectable.ConnState.READY).blockingFirst();
        await().forever().until(() -> BoundaryUtil.isValidSplitSet(storeClient.latestEffectiveRouter().keySet()));

        log.info("Setup finished, and start testing");
    }

    private void buildStoreServer() {
        RPCServerBuilder rpcServerBuilder = IRPCServer.newBuilder().host("127.0.0.1").trafficService(trafficService);
        testStore = IInboxStore.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .agentHost(agentHost)
            .metaService(metaService)
            .distClient(distClient)
            .inboxClient(inboxClient)
            .retainClient(retainClient)
            .sessionDictClient(sessionDictClient)
            .inboxStoreClient(storeClient)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .resourceThrottler(resourceThrottler)
            .storeOptions(options)
            .tickerThreads(tickerThreads)
            .bgTaskExecutor(bgTaskExecutor)
            .detachTimeout(Duration.ofSeconds(1))
            .gcInterval(Duration.ofSeconds(1))
            .bootstrapDelay(Duration.ofSeconds(1))
            .build();
        rpcServer = rpcServerBuilder.build();
    }

    protected void restartStoreServer() {
        rpcServer.shutdown();
        testStore.close();
        buildStoreServer();
        rpcServer.start();
    }

    @AfterClass(groups = "integration")
    public void tearDown() throws Exception {
        log.info("Finish testing, and tearing down");
        inboxClient.close();
        storeClient.close();
        testStore.close();
        rpcServer.shutdown();
        trafficService.close();
        metaService.close();
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
        queryExecutor.shutdown();
        bgTaskExecutor.shutdown();
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
        KVRangeSetting s = findByKey(routeKey, storeClient.latestEffectiveRouter()).get();
        return query(s, input);
    }

    private InboxServiceROCoProcOutput query(KVRangeSetting s, InboxServiceROCoProcInput input) {
        KVRangeROReply reply = storeClient.query(s.leader(), KVRangeRORequest.newBuilder()
            .setReqId(input.getReqId())
            .setVer(s.ver())
            .setKvRangeId(s.id())
            .setRoCoProc(ROCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), input.getReqId());
        assertEquals(reply.getCode(), ReplyCode.Ok);
        return reply.getRoCoProcResult().getInboxService();
    }

    protected InboxServiceRWCoProcOutput mutate(ByteString routeKey, InboxServiceRWCoProcInput input) {
        KVRangeSetting s = findByKey(routeKey, storeClient.latestEffectiveRouter()).get();
        KVRangeRWReply reply = storeClient.execute(s.leader(), KVRangeRWRequest.newBuilder()
            .setReqId(input.getReqId())
            .setVer(s.ver())
            .setKvRangeId(s.id())
            .setRwCoProc(RWCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), input.getReqId());
        assertEquals(reply.getCode(), ReplyCode.Ok);
        return reply.getRwCoProcResult().getInboxService();
    }

    protected GCReply requestGC(GCRequest request) {
        long reqId = ThreadLocalRandom.current().nextInt();
        InboxServiceROCoProcInput input = InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setGc(request)
            .build();
        Collection<KVRangeSetting> rangeSettings = findByBoundary(FULL_BOUNDARY, storeClient.latestEffectiveRouter());
        assert !rangeSettings.isEmpty();
        InboxServiceROCoProcOutput output = query(rangeSettings.stream().findFirst().get(), input);
        assertTrue(output.hasGc());
        assertEquals(output.getReqId(), reqId);
        return output.getGc();
    }

    protected List<BatchSendLWTReply.Code> requestSendLWT(BatchSendLWTRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getTenantId(), params[0].getInboxId());
        InboxServiceROCoProcInput input = MessageUtil.buildSendLWTRequest(reqId, BatchSendLWTRequest.newBuilder()
            .addAllParams(List.of(params))
            .build());
        InboxServiceROCoProcOutput output = query(routeKey, input);
        assertTrue(output.hasBatchSendLWT());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchSendLWT().getCodeList();
    }

    protected List<Boolean> requestExist(BatchExistRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getTenantId(), params[0].getInboxId());
        InboxServiceROCoProcInput input = MessageUtil.buildExistRequest(reqId, BatchExistRequest.newBuilder()
            .addAllParams(List.of(params))
            .build());
        InboxServiceROCoProcOutput output = query(routeKey, input);
        assertTrue(output.hasBatchExist());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchExist().getExistList();
    }

    protected List<Fetched> requestFetch(BatchFetchRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getTenantId(), params[0].getInboxId());
        InboxServiceROCoProcInput input = MessageUtil.buildFetchRequest(reqId, BatchFetchRequest.newBuilder()
            .addAllParams(List.of(params))
            .build());
        InboxServiceROCoProcOutput output = query(routeKey, input);
        assertTrue(output.hasBatchFetch());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchFetch().getResultList();
    }

    protected List<InboxVersion> requestAttach(BatchAttachRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getClient().getTenantId(), params[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil.buildAttachRequest(reqId,
            BatchAttachRequest.newBuilder().addAllParams(List.of(params)).build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchAttach());
        assertEquals(output.getReqId(), reqId);
        assertEquals(params.length, output.getBatchAttach().getVersionCount());
        return output.getBatchAttach().getVersionList();
    }

    protected List<BatchDetachReply.Code> requestDetach(BatchDetachRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getTenantId(), params[0].getInboxId());
        KVRangeSetting s = findByKey(routeKey, storeClient.latestEffectiveRouter()).get();
        InboxServiceRWCoProcInput input = MessageUtil.buildDetachRequest(reqId, BatchDetachRequest.newBuilder()
            .addAllParams(List.of(params))
            .build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchDetach());
        assertEquals(output.getReqId(), reqId);
        assertEquals(params.length, output.getBatchDetach().getCodeCount());
        return output.getBatchDetach().getCodeList();
    }

    protected List<BatchDeleteReply.Result> requestDelete(BatchDeleteRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getTenantId(), params[0].getInboxId());

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
        ByteString routeKey = inboxStartKeyPrefix(params[0].getTenantId(), params[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil
            .buildSubRequest(reqId, BatchSubRequest.newBuilder().addAllParams(List.of(params)).build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchSub());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchSub().getCodeList();
    }

    protected List<BatchUnsubReply.Result> requestUnsub(BatchUnsubRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getTenantId(), params[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil.buildUnsubRequest(reqId, BatchUnsubRequest.newBuilder()
            .addAllParams(List.of(params))
            .build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchUnsub());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchUnsub().getResultList();
    }

    protected List<InsertResult> requestInsert(InsertRequest... insertRequest) {
        assert insertRequest.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey =
            inboxStartKeyPrefix(insertRequest[0].getTenantId(), insertRequest[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil.buildInsertRequest(reqId, BatchInsertRequest.newBuilder()
            .addAllRequest(List.of(insertRequest))
            .build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchInsert());
        assertEquals(output.getReqId(), reqId);
        assertEquals(insertRequest.length, output.getBatchInsert().getResultCount());
        return output.getBatchInsert().getResultList();
    }

    protected List<BatchCommitReply.Code> requestCommit(BatchCommitRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getTenantId(), params[0].getInboxId());

        InboxServiceRWCoProcInput input = MessageUtil.buildCommitRequest(reqId, BatchCommitRequest.newBuilder()
            .addAllParams(List.of(params))
            .build());

        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchCommit());
        assertEquals(output.getReqId(), reqId);
        assertEquals(params.length, output.getBatchCommit().getCodeCount());
        return output.getBatchCommit().getCodeList();
    }

    protected Gauge getPSessionGauge(String tenantId) {
        return getGauge(tenantId, MqttPersistentSessionNumGauge);
    }

    protected Gauge getPSessionSpaceGauge(String tenantId) {
        return getGauge(tenantId, MqttPersistentSessionSpaceGauge);
    }

    protected Gauge getSubCountGauge(String tenantId) {
        return getGauge(tenantId, MqttPersistentSubCountGauge);
    }

    protected void assertNoGauge(String tenantId, TenantMetric gaugeMetric) {
        await().until(() -> {
            boolean found = false;
            for (Meter meter : meterRegistry.getMeters()) {
                if (meter.getId().getType() == Meter.Type.GAUGE
                    && meter.getId().getName().equals(gaugeMetric.metricName)
                    && Objects.equals(meter.getId().getTag("tenantId"), tenantId)) {
                    found = true;
                }
            }
            return !found;
        });
    }

    protected Gauge getGauge(String tenantId, TenantMetric gaugeMetric) {
        AtomicReference<Gauge> holder = new AtomicReference<>();
        await().until(() -> {
            for (Meter meter : meterRegistry.getMeters()) {
                if (meter.getId().getType() == Meter.Type.GAUGE
                    && meter.getId().getName().equals(gaugeMetric.metricName)
                    && Objects.equals(meter.getId().getTag("tenantId"), tenantId)) {
                    holder.set((Gauge) meter);
                    break;
                }
            }
            return holder.get() != null;
        });
        return holder.get();
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
