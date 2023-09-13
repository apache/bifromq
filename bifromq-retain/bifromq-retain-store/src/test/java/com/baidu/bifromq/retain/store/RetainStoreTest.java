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

package com.baidu.bifromq.retain.store;

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
import com.baidu.bifromq.basekv.localengine.InMemoryKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.RocksDBKVEngineConfigurator;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.basekv.store.proto.KVRangeROReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.retain.rpc.proto.GCReply;
import com.baidu.bifromq.retain.rpc.proto.MatchCoProcReply;
import com.baidu.bifromq.retain.rpc.proto.MatchCoProcRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainCoProcReply;
import com.baidu.bifromq.retain.rpc.proto.RetainCoProcRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceROCoProcInput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceROCoProcOutput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceRWCoProcInput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceRWCoProcOutput;
import com.baidu.bifromq.retain.utils.KeyUtil;
import com.baidu.bifromq.retain.utils.MessageUtil;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.TopicMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

@Slf4j
public class RetainStoreTest {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR_NAME = "testDB_cp";

    private static final String DB_WAL_NAME = "testWAL";
    private static final String DB_WAL_CHECKPOINT_DIR = "testWAL_cp";
    private IAgentHost agentHost;

    private ICRDTService clientCrdtService;
    private ICRDTService serverCrdtService;

    protected IRetainStore testStore;

    protected IBaseKVStoreClient storeClient;
    private ExecutorService queryExecutor;
    private ExecutorService mutationExecutor;
    private ScheduledExecutorService tickTaskExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    private Path dbRootDir;
    private AutoCloseable closeable;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws IOException {
        closeable = MockitoAnnotations.openMocks(this);
        dbRootDir = Files.createTempDirectory("");
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
            options.setDataEngineConfigurator(new InMemoryKVEngineConfigurator());
            options.setWalEngineConfigurator(new InMemoryKVEngineConfigurator());
        } else {
            ((RocksDBKVEngineConfigurator) options.getDataEngineConfigurator())
                .setDbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR_NAME, uuid)
                    .toString())
                .setDbRootDir(Paths.get(dbRootDir.toString(), DB_NAME, uuid).toString());
            ((RocksDBKVEngineConfigurator) options.getWalEngineConfigurator())
                .setDbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_WAL_CHECKPOINT_DIR, uuid)
                    .toString())
                .setDbRootDir(Paths.get(dbRootDir.toString(), DB_WAL_NAME, uuid).toString());
        }
        queryExecutor = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
            EnvProvider.INSTANCE.newThreadFactory("query-executor"));
        mutationExecutor = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
            EnvProvider.INSTANCE.newThreadFactory("mutation-executor"));
        tickTaskExecutor = new ScheduledThreadPoolExecutor(2,
            EnvProvider.INSTANCE.newThreadFactory("tick-task-executor"));
        bgTaskExecutor = new ScheduledThreadPoolExecutor(1,
            EnvProvider.INSTANCE.newThreadFactory("bg-task-executor"));

        storeClient = IBaseKVStoreClient
            .newBuilder()
            .clusterId(IRetainStore.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .build();
        testStore = IRetainStore.standaloneBuilder()
            .bootstrap(true)
            .host("127.0.0.1")
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .storeClient(storeClient)
            .clock(getClock())
            .storeOptions(options)
            .balanceControllerOptions(new KVRangeBalanceControllerOptions())
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .build();
        testStore.start();

        storeClient.join();
        log.info("Setup finished, and start testing");
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        log.info("Finish testing, and tearing down");
        storeClient.stop();
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
        mutationExecutor.shutdown();
        tickTaskExecutor.shutdown();
        bgTaskExecutor.shutdown();
        closeable.close();
    }

    protected Clock getClock() {
        return Clock.systemUTC();
    }


    private static boolean runOnMac() {
        String osName = System.getProperty("os.name");
        return osName != null && osName.startsWith("Mac");
    }

    protected RetainCoProcReply requestRetain(String tenantId, int maxRetainedTopics, TopicMessage topicMsg) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString tenantNS = KeyUtil.tenantNS(tenantId);
            KVRangeSetting s = storeClient.findByKey(tenantNS).get();
            String topic = topicMsg.getTopic();
            Message message = topicMsg.getMessage();
            RetainCoProcRequest request = RetainCoProcRequest.newBuilder()
                .setTenantId(tenantId)
                .setReqId(message.getMessageId())
                .setQos(message.getPubQoS())
                .setTopic(topic)
                .setTimestamp(message.getTimestamp())
                .setExpireTimestamp(message.getExpireTimestamp())
                .setPublisher(topicMsg.getPublisher())
                .setMessage(message.getPayload())
                .setMaxRetainedTopics(maxRetainedTopics)
                .build();
            RetainServiceRWCoProcInput input = MessageUtil.buildRetainRequest(request);
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reply.getReqId(), reqId);
            assertEquals(reply.getCode(), ReplyCode.Ok);
            RetainServiceRWCoProcOutput output = RetainServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult());
            assertTrue(output.hasRetainReply());
            assertEquals(output.getRetainReply().getReqId(), message.getMessageId());
            return output.getRetainReply();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected MatchCoProcReply requestMatch(String tenantId, String topicFilter, int limit) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString tenantNS = KeyUtil.tenantNS(tenantId);
            KVRangeSetting s = storeClient.findByKey(tenantNS).get();
            MatchCoProcRequest request = MatchCoProcRequest.newBuilder().setReqId(reqId)
                .setTenantNS(tenantNS)
                .setTopicFilter(topicFilter)
                .setLimit(limit)
                .build();
            RetainServiceROCoProcInput input = MessageUtil.buildMatchRequest(request);
            KVRangeROReply reply = storeClient.query(s.leader, KVRangeRORequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRoCoProcInput(input.toByteString())
                .build()).join();
            assertEquals(reply.getReqId(), reqId);
            assertEquals(reply.getCode(), ReplyCode.Ok);
            RetainServiceROCoProcOutput output = RetainServiceROCoProcOutput.parseFrom(reply.getRoCoProcResult());
            assertTrue(output.hasMatchReply());
            assertEquals(output.getMatchReply().getReqId(), reqId);
            return output.getMatchReply();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected GCReply requestGC(String tenantId) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString tenantNS = KeyUtil.tenantNS(tenantId);
            KVRangeSetting s = storeClient.findByKey(tenantNS).get();
            RetainServiceRWCoProcInput input = MessageUtil.buildGCRequest(reqId);
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reply.getReqId(), reqId);
            assertEquals(reply.getCode(), ReplyCode.Ok);
            RetainServiceRWCoProcOutput output = RetainServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult());
            assertTrue(output.hasGcReply());
            assertEquals(output.getGcReply().getReqId(), reqId);
            return output.getGcReply();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected TopicMessage message(String topic, String payload) {
        return message(topic, payload, System.currentTimeMillis(), Integer.MAX_VALUE);
    }

    protected TopicMessage message(String topic, String payload, long timestamp, int expirySeconds) {
        return TopicMessage.newBuilder()
            .setTopic(topic)
            .setMessage(Message.newBuilder()
                .setMessageId(System.nanoTime())
                .setPayload(ByteString.copyFromUtf8(payload))
                .setTimestamp(timestamp)
                .setExpireTimestamp(expirySeconds == Integer.MAX_VALUE ? Long.MAX_VALUE :
                    timestamp + Duration.ofSeconds(expirySeconds).toMillis())
                .build())
            .setPublisher(ClientInfo.getDefaultInstance())
            .build();
    }
}
