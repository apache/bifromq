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

package org.apache.bifromq.inbox.store;

import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static org.apache.bifromq.plugin.settingprovider.Setting.RetainEnabled;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainMessageSpaceBytes;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainTopics;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.bufferedMsgKey;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.inboxInstanceStartKey;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.isInboxInstanceKey;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.isInboxInstanceStartKey;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.parseInboxInstanceStartKeyPrefix;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.parseSeq;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.parseTenantId;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.qos0MsgKey;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.qos0QueuePrefix;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.sendBufferPrefix;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;

import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.OutOfTenantResource;
import org.apache.bifromq.plugin.eventcollector.inboxservice.Overflowed;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.disthandling.WillDistError;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.disthandling.WillDisted;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.retainhandling.MsgRetained;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.retainhandling.MsgRetainedError;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.retainhandling.RetainMsgCleared;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.retain.client.IRetainClient;
import org.apache.bifromq.retain.rpc.proto.RetainReply;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.baseenv.ZeroCopyParser;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVRangeCoProc;
import org.apache.bifromq.basekv.store.api.IKVReader;
import org.apache.bifromq.basekv.store.api.IKVWriter;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.dist.client.PubResult;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.inbox.record.InboxInstance;
import org.apache.bifromq.inbox.record.TenantInboxInstance;
import org.apache.bifromq.inbox.storage.proto.BatchAttachReply;
import org.apache.bifromq.inbox.storage.proto.BatchAttachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchCheckSubReply;
import org.apache.bifromq.inbox.storage.proto.BatchCheckSubRequest;
import org.apache.bifromq.inbox.storage.proto.BatchCommitReply;
import org.apache.bifromq.inbox.storage.proto.BatchCommitRequest;
import org.apache.bifromq.inbox.storage.proto.BatchDeleteReply;
import org.apache.bifromq.inbox.storage.proto.BatchDeleteRequest;
import org.apache.bifromq.inbox.storage.proto.BatchDetachReply;
import org.apache.bifromq.inbox.storage.proto.BatchDetachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchExistReply;
import org.apache.bifromq.inbox.storage.proto.BatchExistRequest;
import org.apache.bifromq.inbox.storage.proto.BatchFetchReply;
import org.apache.bifromq.inbox.storage.proto.BatchFetchRequest;
import org.apache.bifromq.inbox.storage.proto.BatchInsertReply;
import org.apache.bifromq.inbox.storage.proto.BatchInsertRequest;
import org.apache.bifromq.inbox.storage.proto.BatchSendLWTReply;
import org.apache.bifromq.inbox.storage.proto.BatchSendLWTRequest;
import org.apache.bifromq.inbox.storage.proto.BatchSubReply;
import org.apache.bifromq.inbox.storage.proto.BatchSubRequest;
import org.apache.bifromq.inbox.storage.proto.BatchUnsubReply;
import org.apache.bifromq.inbox.storage.proto.BatchUnsubRequest;
import org.apache.bifromq.inbox.storage.proto.ExpireTenantReply;
import org.apache.bifromq.inbox.storage.proto.ExpireTenantRequest;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import org.apache.bifromq.inbox.storage.proto.GCReply;
import org.apache.bifromq.inbox.storage.proto.GCRequest;
import org.apache.bifromq.inbox.storage.proto.InboxMessage;
import org.apache.bifromq.inbox.storage.proto.InboxMessageList;
import org.apache.bifromq.inbox.storage.proto.InboxMetadata;
import org.apache.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import org.apache.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import org.apache.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import org.apache.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.InsertRequest;
import org.apache.bifromq.inbox.storage.proto.InsertResult;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.inbox.storage.proto.SubMessagePack;
import org.apache.bifromq.inbox.storage.proto.TopicFilterOption;
import org.apache.bifromq.inbox.store.delay.DelayTaskRunner;
import org.apache.bifromq.inbox.store.delay.ExpireInboxTask;
import org.apache.bifromq.inbox.store.delay.IDelayTaskRunner;
import org.apache.bifromq.inbox.store.delay.SendLWTTask;
import org.apache.bifromq.inbox.store.schema.KVSchemaUtil;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;
import org.apache.bifromq.sessiondict.client.type.OnlineCheckRequest;
import org.apache.bifromq.sessiondict.client.type.OnlineCheckResult;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicMessage;
import org.apache.bifromq.type.TopicMessagePack;

@Slf4j
final class InboxStoreCoProc implements IKVRangeCoProc {
    private static final int UINT_MAX = 0xFFFFFFFF;
    private final KVRangeId id;
    private final String storeId;
    private final IDistClient distClient;
    private final IRetainClient retainClient;
    private final IInboxClient inboxClient;
    private final ISessionDictClient sessionDictClient;
    private final ISettingProvider settingProvider;
    private final IEventCollector eventCollector;
    private final IResourceThrottler resourceThrottler;
    private final TenantsState tenantStates;
    private final Supplier<IKVCloseableReader> rangeReaderProvider;
    private final IDelayTaskRunner<TenantInboxInstance> delayTaskRunner;
    private final Duration detachTimeout;

    InboxStoreCoProc(String clusterId,
                     String storeId,
                     KVRangeId id,
                     IDistClient distClient,
                     IInboxClient inboxClient,
                     IRetainClient retainClient,
                     ISessionDictClient sessionDictClient,
                     ISettingProvider settingProvider,
                     IEventCollector eventCollector,
                     IResourceThrottler resourceThrottler,
                     Supplier<IKVCloseableReader> rangeReaderProvider,
                     Duration detachTimeout,
                     int expireRateLimit) {
        this.id = id;
        this.storeId = storeId;
        this.distClient = distClient;
        this.retainClient = retainClient;
        this.inboxClient = inboxClient;
        this.sessionDictClient = sessionDictClient;
        this.settingProvider = settingProvider;
        this.eventCollector = eventCollector;
        this.resourceThrottler = resourceThrottler;
        this.rangeReaderProvider = rangeReaderProvider;
        this.tenantStates = new TenantsState(eventCollector, rangeReaderProvider.get(), "clusterId", clusterId,
            "storeId", storeId, "rangeId", KVRangeIdUtil.toString(id));
        this.delayTaskRunner = new DelayTaskRunner<>(TenantInboxInstance::compareTo, HLC.INST::getPhysical,
            expireRateLimit);
        this.detachTimeout = detachTimeout;
    }

    private static int getExpireSeconds(Duration expireTime, InboxMetadata latestInboxMetadata) {
        int expireSeconds;
        int newExpireSeconds = (int) expireTime.toSeconds();
        if (Integer.compareUnsigned(latestInboxMetadata.getExpirySeconds(), UINT_MAX) == 0) {
            if (newExpireSeconds > 0) {
                expireSeconds = newExpireSeconds;
            } else {
                expireSeconds = UINT_MAX;
            }
        } else {
            if (newExpireSeconds > 0) {
                expireSeconds = Integer.compareUnsigned(latestInboxMetadata.getExpirySeconds(),
                    newExpireSeconds) < 0 ? latestInboxMetadata.getExpirySeconds() : newExpireSeconds;
            } else {
                expireSeconds = latestInboxMetadata.getExpirySeconds();
            }
        }
        return expireSeconds;
    }

    @Override
    public CompletableFuture<ROCoProcOutput> query(ROCoProcInput input, IKVReader reader) {
        try {
            InboxServiceROCoProcInput coProcInput = input.getInboxService();
            InboxServiceROCoProcOutput.Builder outputBuilder = InboxServiceROCoProcOutput.newBuilder()
                .setReqId(coProcInput.getReqId());
            CompletableFuture<InboxServiceROCoProcOutput.Builder> outputFuture;
            switch (coProcInput.getInputCase()) {
                case BATCHEXIST -> outputFuture = batchExist(coProcInput.getBatchExist())
                    .thenApply(outputBuilder::setBatchExist);
                case BATCHFETCH -> outputFuture = batchFetch(coProcInput.getBatchFetch(), reader)
                    .thenApply(outputBuilder::setBatchFetch);
                case BATCHCHECKSUB -> outputFuture = batchCheckSub(coProcInput.getBatchCheckSub())
                    .thenApply(outputBuilder::setBatchCheckSub);
                case GC -> outputFuture = gc(coProcInput.getGc())
                    .thenApply(outputBuilder::setGc);
                case EXPIRETENANT -> outputFuture = expireTenant(coProcInput.getExpireTenant())
                    .thenApply(outputBuilder::setExpireTenant);
                default -> outputFuture = batchSendLWT(coProcInput.getBatchSendLWT()).thenApply(
                    outputBuilder::setBatchSendLWT);
            }
            return outputFuture.thenApply(o -> ROCoProcOutput.newBuilder().setInboxService(o.build()).build());
        } catch (Throwable e) {
            log.error("Query co-proc failed", e);
            return CompletableFuture.failedFuture(new IllegalStateException("Query co-proc failed", e));
        }
    }

    @Override
    public Supplier<MutationResult> mutate(RWCoProcInput input, IKVReader reader, IKVWriter writer) {
        InboxServiceRWCoProcInput coProcInput = input.getInboxService();
        InboxServiceRWCoProcOutput.Builder outputBuilder = InboxServiceRWCoProcOutput.newBuilder()
            .setReqId(coProcInput.getReqId());
        AtomicReference<Runnable> afterMutate = new AtomicReference<>();
        switch (coProcInput.getTypeCase()) {
            case BATCHATTACH -> {
                BatchAttachReply.Builder replyBuilder = BatchAttachReply.newBuilder();
                afterMutate.set(batchAttach(coProcInput.getBatchAttach(), replyBuilder, reader, writer));
                outputBuilder.setBatchAttach(replyBuilder);
            }
            case BATCHDETACH -> {
                BatchDetachReply.Builder replyBuilder = BatchDetachReply.newBuilder();
                afterMutate.set(batchDetach(coProcInput.getBatchDetach(), replyBuilder, writer));
                outputBuilder.setBatchDetach(replyBuilder);
            }
            case BATCHDELETE -> {
                BatchDeleteReply.Builder replyBuilder = BatchDeleteReply.newBuilder();
                afterMutate.set(batchDelete(coProcInput.getBatchDelete(), replyBuilder, reader, writer));
                outputBuilder.setBatchDelete(replyBuilder.build());
            }
            case BATCHSUB -> {
                BatchSubReply.Builder replyBuilder = BatchSubReply.newBuilder();
                afterMutate.set(batchSub(coProcInput.getBatchSub(), replyBuilder, writer));
                outputBuilder.setBatchSub(replyBuilder);
            }
            case BATCHUNSUB -> {
                BatchUnsubReply.Builder replyBuilder = BatchUnsubReply.newBuilder();
                afterMutate.set(batchUnsub(coProcInput.getBatchUnsub(), replyBuilder, writer));
                outputBuilder.setBatchUnsub(replyBuilder);
            }
            case BATCHINSERT -> {
                BatchInsertReply.Builder replyBuilder = BatchInsertReply.newBuilder();
                afterMutate.set(batchInsert(coProcInput.getBatchInsert(), replyBuilder, reader, writer));
                outputBuilder.setBatchInsert(replyBuilder);
            }
            case BATCHCOMMIT -> {
                BatchCommitReply.Builder replyBuilder = BatchCommitReply.newBuilder();
                afterMutate.set(batchCommit(coProcInput.getBatchCommit(), replyBuilder, reader, writer));
                outputBuilder.setBatchCommit(replyBuilder);
            }
            default -> {
                // never happens
            }
        }
        RWCoProcOutput output = RWCoProcOutput.newBuilder().setInboxService(outputBuilder.build()).build();
        return () -> {
            afterMutate.get().run();
            return new MutationResult(output, Optional.empty());
        };
    }

    @Override
    public Any reset(Boundary boundary) {
        tenantStates.reset();
        log.debug("Loading tenant states: rangeId={}", KVRangeIdUtil.toString(id));
        load();
        return Any.getDefaultInstance();
    }

    @Override
    public void close() {
        tenantStates.close();
        delayTaskRunner.shutdown();
    }

    private CompletableFuture<BatchExistReply> batchExist(BatchExistRequest request) {
        BatchExistReply.Builder replyBuilder = BatchExistReply.newBuilder();
        for (BatchExistRequest.Params params : request.getParamsList()) {
            SortedMap<Long, InboxMetadata> inboxInstances = tenantStates.getAll(params.getTenantId(),
                params.getInboxId());
            replyBuilder.addExist(!inboxInstances.isEmpty()
                && !hasExpired(inboxInstances.get(inboxInstances.lastKey()), params.getNow()));
        }
        return CompletableFuture.completedFuture(replyBuilder.build());
    }

    private CompletableFuture<BatchCheckSubReply> batchCheckSub(BatchCheckSubRequest request) {
        BatchCheckSubReply.Builder replyBuilder = BatchCheckSubReply.newBuilder();
        for (BatchCheckSubRequest.Params params : request.getParamsList()) {
            Optional<InboxMetadata> metadataOpt = tenantStates.get(params.getTenantId(), params.getInboxId(),
                params.getIncarnation());
            if (metadataOpt.isEmpty()) {
                replyBuilder.addCode(BatchCheckSubReply.Code.NO_INBOX);
                continue;
            }
            if (hasExpired(metadataOpt.get(), request.getNow())) {
                replyBuilder.addCode(BatchCheckSubReply.Code.NO_INBOX);
                continue;
            }
            InboxMetadata metadata = metadataOpt.get();
            if (metadata.containsTopicFilters(params.getTopicFilter())) {
                replyBuilder.addCode(BatchCheckSubReply.Code.OK);
            } else {
                replyBuilder.addCode(BatchCheckSubReply.Code.NO_MATCH);
            }
        }
        return CompletableFuture.completedFuture(replyBuilder.build());
    }

    private CompletableFuture<BatchFetchReply> batchFetch(BatchFetchRequest request, IKVReader reader) {
        BatchFetchReply.Builder replyBuilder = BatchFetchReply.newBuilder();
        for (BatchFetchRequest.Params params : request.getParamsList()) {
            replyBuilder.addResult(fetch(params, reader));
        }
        return CompletableFuture.completedFuture(replyBuilder.build());
    }

    private Fetched fetch(BatchFetchRequest.Params params, IKVReader reader) {
        Fetched.Builder replyBuilder = Fetched.newBuilder();
        Optional<InboxMetadata> inboxMetadataOpt = tenantStates.get(params.getTenantId(), params.getInboxId(),
            params.getIncarnation());
        if (inboxMetadataOpt.isEmpty()) {
            replyBuilder.setResult(Fetched.Result.NO_INBOX);
            return replyBuilder.build();
        }
        InboxMetadata metadata = inboxMetadataOpt.get();
        ByteString inboxInstStartKey = inboxInstanceStartKey(params.getTenantId(), params.getInboxId(),
            params.getIncarnation());
        // deal with qos0 queue
        long startFetchFromSeq = !params.hasQos0StartAfter() ? metadata.getQos0StartSeq() :
            Math.max(params.getQos0StartAfter() + 1, metadata.getQos0StartSeq());
        fetchFromInbox(inboxInstStartKey, Integer.MAX_VALUE, metadata.getQos0StartSeq(), startFetchFromSeq,
            metadata.getQos0NextSeq(), KVSchemaUtil::qos0MsgKey, Fetched.Builder::addQos0Msg, reader, replyBuilder);
        // deal with qos12 queue
        startFetchFromSeq = !params.hasSendBufferStartAfter() ? metadata.getSendBufferStartSeq() :
            Math.max(params.getSendBufferStartAfter() + 1, metadata.getSendBufferStartSeq());
        fetchFromInbox(inboxInstStartKey, params.getMaxFetch(), metadata.getSendBufferStartSeq(), startFetchFromSeq,
            metadata.getSendBufferNextSeq(), KVSchemaUtil::bufferedMsgKey, Fetched.Builder::addSendBufferMsg, reader,
            replyBuilder);
        return replyBuilder.setResult(Fetched.Result.OK).build();
    }

    @SneakyThrows
    private void fetchFromInbox(ByteString inboxInstStartKey,
                                int fetchCount,
                                long startSeq,
                                long startFetchFromSeq,
                                long nextSeq,
                                BiFunction<ByteString, Long, ByteString> keyGenerator,
                                BiConsumer<Fetched.Builder, InboxMessage> messageConsumer,
                                IKVReader reader,
                                Fetched.Builder replyBuilder) {
        if (startFetchFromSeq < nextSeq) {
            while (startSeq < nextSeq && fetchCount > 0) {
                ByteString startKey = keyGenerator.apply(inboxInstStartKey, startSeq);
                Optional<ByteString> msgListData = reader.get(startKey);
                // the startSeq may not reflect the latest seq of the first message when query is non-linearized
                // it may point to the message was committed.
                if (msgListData.isEmpty()) {
                    startSeq++;
                    continue;
                }
                List<InboxMessage> messageList = ZeroCopyParser.parse(msgListData.get(), InboxMessageList.parser())
                    .getMessageList();
                long lastSeq = messageList.get(messageList.size() - 1).getSeq();
                if (lastSeq >= startFetchFromSeq) {
                    for (InboxMessage inboxMsg : messageList) {
                        if (inboxMsg.getSeq() >= startFetchFromSeq) {
                            messageConsumer.accept(replyBuilder, inboxMsg);
                            fetchCount--;
                            if (fetchCount == 0) {
                                break;
                            }
                        }
                    }
                }
                startSeq = lastSeq + 1;
            }
        }
    }

    private CompletableFuture<BatchSendLWTReply> batchSendLWT(BatchSendLWTRequest request) {
        List<CompletableFuture<BatchSendLWTReply.Code>> sendLWTFutures = new ArrayList<>(request.getParamsCount());
        for (BatchSendLWTRequest.Params params : request.getParamsList()) {
            Optional<InboxMetadata> metadataOpt = tenantStates.get(params.getTenantId(), params.getInboxId(),
                params.getVersion().getIncarnation());
            if (metadataOpt.isEmpty()) {
                sendLWTFutures.add(CompletableFuture.completedFuture(BatchSendLWTReply.Code.NO_INBOX));
                continue;
            }
            if (metadataOpt.get().getMod() != params.getVersion().getMod()) {
                sendLWTFutures.add(CompletableFuture.completedFuture(BatchSendLWTReply.Code.CONFLICT));
                continue;
            }
            if (!metadataOpt.get().hasDetachedAt()) {
                log.error("Illegal state: inbox has not detached");
                sendLWTFutures.add(CompletableFuture.completedFuture(BatchSendLWTReply.Code.ERROR));
                continue;
            }
            if (!metadataOpt.get().hasLwt()) {
                log.error("Illegal state: inbox has no lwt");
                sendLWTFutures.add(CompletableFuture.completedFuture(BatchSendLWTReply.Code.ERROR));
                continue;
            }
            sendLWTFutures.add(sendLWTAndExpireInbox(params.getTenantId(), metadataOpt.get(), params.getNow()));
        }
        return CompletableFuture.allOf(sendLWTFutures.toArray(CompletableFuture[]::new))
            .thenApply(v -> {
                BatchSendLWTReply.Builder replyBuilder = BatchSendLWTReply.newBuilder();
                for (CompletableFuture<BatchSendLWTReply.Code> future : sendLWTFutures) {
                    replyBuilder.addCode(future.join());
                }
                return replyBuilder.build();
            });
    }

    private CompletableFuture<BatchSendLWTReply.Code> sendLWTAndExpireInbox(String tenantId,
                                                                            InboxMetadata metadata,
                                                                            long now) {
        return sendLWT(tenantId, metadata, now)
            .thenApply(v -> {
                if (v == BatchSendLWTReply.Code.OK) {
                    if (Integer.compareUnsigned(metadata.getExpirySeconds(), UINT_MAX) == 0) {
                        // UINT_MAX never expire according to MQTT5 spec
                        return v;
                    }
                    TenantInboxInstance inboxInstance = new TenantInboxInstance(tenantId,
                        new InboxInstance(metadata.getInboxId(), metadata.getIncarnation()));
                    long detachAtMillis = metadata.getDetachedAt();
                    long expireAtMillis = detachAtMillis + Duration.ofSeconds(metadata.getExpirySeconds()).toMillis();
                    Duration delay = Duration.ofMillis(Math.max(0, expireAtMillis - now))
                        .plusMillis(ThreadLocalRandom.current().nextLong(0, 1000));
                    delayTaskRunner.schedule(inboxInstance,
                        new ExpireInboxTask(delay, metadata.getMod(), inboxClient));
                }
                return v;
            });
    }

    private CompletableFuture<BatchSendLWTReply.Code> sendLWT(String tenantId, InboxMetadata metadata, long now) {
        long reqId = System.nanoTime();
        LWT lwt = metadata.getLwt();
        ClientInfo clientInfo = metadata.getClient();
        CompletableFuture<PubResult> distLWTFuture = distClient.pub(reqId, lwt.getTopic(), lwt.getMessage()
            .toBuilder().setTimestamp(now).build(), metadata.getClient());
        CompletableFuture<RetainReply.Result> retainLWTFuture;
        boolean willRetain = lwt.getMessage().getIsRetain();
        boolean retainEnabled = settingProvider.provide(RetainEnabled, tenantId);
        if (willRetain) {
            if (!retainEnabled) {
                eventCollector.report(getLocal(MsgRetainedError.class)
                    .reqId(reqId)
                    .topic(lwt.getTopic())
                    .qos(lwt.getMessage().getPubQoS())
                    .payload(lwt.getMessage().getPayload().asReadOnlyByteBuffer())
                    .size(lwt.getMessage().getPayload().size())
                    .reason("Retain Disabled")
                    .clientInfo(clientInfo));
                retainLWTFuture = CompletableFuture.completedFuture(RetainReply.Result.ERROR);
            } else {
                retainLWTFuture = retain(reqId, lwt, clientInfo)
                    .thenApply(v -> {
                        switch (v) {
                            case RETAINED -> eventCollector.report(getLocal(MsgRetained.class)
                                .topic(lwt.getTopic())
                                .qos(lwt.getMessage().getPubQoS())
                                .isLastWill(true)
                                .size(lwt.getMessage().getPayload().size())
                                .clientInfo(clientInfo));
                            case CLEARED -> eventCollector.report(
                                getLocal(RetainMsgCleared.class)
                                    .topic(lwt.getTopic())
                                    .isLastWill(true)
                                    .clientInfo(clientInfo));
                            case BACK_PRESSURE_REJECTED -> eventCollector.report(
                                getLocal(MsgRetainedError.class)
                                    .topic(lwt.getTopic())
                                    .qos(lwt.getMessage().getPubQoS())
                                    .isLastWill(true)
                                    .payload(lwt.getMessage().getPayload().asReadOnlyByteBuffer())
                                    .size(lwt.getMessage().getPayload().size())
                                    .reason("Server Busy")
                                    .clientInfo(clientInfo));
                            case EXCEED_LIMIT -> eventCollector.report(
                                getLocal(MsgRetainedError.class)
                                    .topic(lwt.getTopic())
                                    .qos(lwt.getMessage().getPubQoS())
                                    .isLastWill(true)
                                    .payload(lwt.getMessage().getPayload().asReadOnlyByteBuffer())
                                    .size(lwt.getMessage().getPayload().size())
                                    .reason("Exceed Limit")
                                    .clientInfo(clientInfo));
                            case ERROR -> eventCollector.report(getLocal(MsgRetainedError.class)
                                .topic(lwt.getTopic())
                                .qos(lwt.getMessage().getPubQoS())
                                .isLastWill(true)
                                .payload(lwt.getMessage().getPayload().asReadOnlyByteBuffer())
                                .size(lwt.getMessage().getPayload().size())
                                .reason("Internal Error")
                                .clientInfo(clientInfo));
                            default -> {
                                // never happen
                            }
                        }
                        return v;
                    });
            }
        } else {
            retainLWTFuture = CompletableFuture.completedFuture(RetainReply.Result.RETAINED);
        }
        return CompletableFuture.allOf(distLWTFuture, retainLWTFuture)
            .thenApply(v -> {
                PubResult distResult = distLWTFuture.join();
                boolean retry = distResult == PubResult.TRY_LATER;
                if (!retry) {
                    if (willRetain && retainEnabled) {
                        retry = retainLWTFuture.join() == RetainReply.Result.TRY_LATER;
                    }
                }
                if (retry) {
                    return BatchSendLWTReply.Code.TRY_LATER;
                } else {
                    switch (distResult) {
                        case OK, NO_MATCH -> {
                            eventCollector.report(getLocal(WillDisted.class)
                                .reqId(reqId)
                                .topic(lwt.getTopic())
                                .qos(lwt.getMessage().getPubQoS())
                                .size(lwt.getMessage().getPayload().size())
                                .clientInfo(clientInfo));
                            return BatchSendLWTReply.Code.OK;
                        }
                        case BACK_PRESSURE_REJECTED -> {
                            eventCollector.report(getLocal(WillDistError.class)
                                .reqId(reqId)
                                .topic(lwt.getTopic())
                                .qos(lwt.getMessage().getPubQoS())
                                .size(lwt.getMessage().getPayload().size())
                                .reason("Server Busy")
                                .clientInfo(clientInfo));
                            return BatchSendLWTReply.Code.OK;
                        }
                        default -> {
                            eventCollector.report(getLocal(WillDistError.class)
                                .reqId(reqId)
                                .topic(lwt.getTopic())
                                .qos(lwt.getMessage().getPubQoS())
                                .size(lwt.getMessage().getPayload().size())
                                .reason("Internal Error")
                                .clientInfo(clientInfo));
                            return BatchSendLWTReply.Code.ERROR;
                        }
                    }
                }
            });
    }

    private CompletableFuture<RetainReply.Result> retain(long reqId, LWT lwt, ClientInfo publisher) {
        if (!resourceThrottler.hasResource(publisher.getTenantId(), TotalRetainTopics)) {
            eventCollector.report(getLocal(OutOfTenantResource.class)
                .reason(TotalRetainTopics.name())
                .clientInfo(publisher));
            return CompletableFuture.completedFuture(RetainReply.Result.EXCEED_LIMIT);
        }
        if (!resourceThrottler.hasResource(publisher.getTenantId(), TotalRetainMessageSpaceBytes)) {
            eventCollector.report(getLocal(OutOfTenantResource.class)
                .reason(TotalRetainMessageSpaceBytes.name())
                .clientInfo(publisher));
            return CompletableFuture.completedFuture(RetainReply.Result.EXCEED_LIMIT);
        }

        return retainClient.retain(reqId,
                lwt.getTopic(),
                lwt.getMessage().getPubQoS(),
                lwt.getMessage().getPayload(),
                lwt.getMessage().getExpiryInterval(),
                publisher)
            .thenApply(RetainReply::getResult);
    }

    private Runnable batchAttach(BatchAttachRequest request,
                                 BatchAttachReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        Map<String, Set<InboxMetadata>> toBeTracked = new HashMap<>();
        Set<TenantInboxInstance> toBeCanceled = new HashSet<>();
        Map<String, Set<InboxMetadata>> toBeEnsured = new HashMap<>();
        for (BatchAttachRequest.Params params : request.getParamsList()) {
            String tenantId = params.getClient().getTenantId();
            String inboxId = params.getInboxId();
            long now = params.getNow();
            SortedMap<Long, InboxMetadata> inboxInstances = tenantStates.getAll(tenantId, inboxId);
            if (inboxInstances.isEmpty() || hasExpired(inboxInstances.get(inboxInstances.lastKey()), now)) {
                long incarnation = params.getIncarnation();
                ByteString metadataKey = inboxInstanceStartKey(tenantId, inboxId, incarnation);
                if (params.getExpirySeconds() == 0) {
                    // no need to create a new inbox instance with expirySeconds=0,
                    // mqtt broker will create a transient session in this case
                    replyBuilder.addVersion(InboxVersion.newBuilder()
                        .setMod(0)
                        .setIncarnation(0)
                        .build());
                } else {
                    InboxMetadata.Builder metadataBuilder = InboxMetadata.newBuilder()
                        .setInboxId(params.getInboxId())
                        .setIncarnation(incarnation)
                        .setMod(0) // mod always starts from 0
                        .setExpirySeconds(params.getExpirySeconds())
                        .setLimit(params.getLimit())
                        .setDropOldest(params.getDropOldest())
                        .setClient(params.getClient())
                        .setLastActiveTime(params.getNow());
                    if (params.hasLwt()) {
                        metadataBuilder.setLwt(params.getLwt());
                    }
                    InboxMetadata metadata = metadataBuilder.build();
                    writer.put(metadataKey, metadata.toByteString());
                    toBeTracked.computeIfAbsent(tenantId, k -> new HashSet<>()).add(metadata);
                    replyBuilder.addVersion(InboxVersion.newBuilder()
                        .setMod(0)
                        .setIncarnation(incarnation)
                        .build());
                }
                if (!inboxInstances.isEmpty()) {
                    // this is an optimization for ensure expire task exists for all expired incarnations
                    if (request.getLeader().getRangeId().equals(id)
                        && request.getLeader().getStoreId().equals(storeId)) {
                        for (InboxMetadata oldInboxMetadata : inboxInstances.values()) {
                            toBeEnsured.computeIfAbsent(tenantId, k -> new HashSet<>()).add(oldInboxMetadata);
                        }
                    }
                }
            } else {
                InboxMetadata existingMetadata = inboxInstances.get(inboxInstances.lastKey());
                long incarnation = existingMetadata.getIncarnation();
                InboxMetadata.Builder metadataBuilder = existingMetadata.toBuilder()
                    // bump mod whenever attach to an existing non-expired inbox incarnation
                    // this happens when client's connection is flipping
                    .setMod(existingMetadata.getMod() + 1)
                    .setExpirySeconds(params.getExpirySeconds())
                    .setLastActiveTime(params.getNow())
                    .setClient(params.getClient())
                    .clearDetachedAt();
                if (params.hasLwt()) {
                    metadataBuilder.setLwt(params.getLwt());
                } else {
                    metadataBuilder.clearLwt();
                }
                InboxMetadata metadata = metadataBuilder.build();
                ByteString metadataKey = inboxInstanceStartKey(tenantId, inboxId, incarnation);
                writer.put(metadataKey, metadata.toByteString());
                replyBuilder.addVersion(InboxVersion.newBuilder()
                    .setMod(metadata.getMod())
                    .setIncarnation(incarnation)
                    .build());
                toBeTracked.computeIfAbsent(tenantId, k -> new HashSet<>()).add(metadata);
                if (request.getLeader().getRangeId().equals(id) && request.getLeader().getStoreId().equals(storeId)) {
                    TenantInboxInstance inboxInstance = new TenantInboxInstance(tenantId,
                        new InboxInstance(inboxId, incarnation));
                    toBeCanceled.add(inboxInstance);
                }
            }
        }
        return () -> {
            updateTenantStates(toBeTracked);
            delayTaskRunner.cancelAll(toBeCanceled);
            toBeEnsured.forEach((tenantId, inboxSet) -> inboxSet.forEach(metadata -> {
                TenantInboxInstance inboxInstance = new TenantInboxInstance(tenantId,
                    new InboxInstance(metadata.getInboxId(), metadata.getIncarnation()));
                // schedule a task for sending LWT or expiry session
                if (metadata.hasLwt()) {
                    Duration delay = Duration.ofSeconds(
                            Integer.compareUnsigned(metadata.getLwt().getDelaySeconds(), metadata.getExpirySeconds())
                                < 0 ? metadata.getLwt().getDelaySeconds() : metadata.getExpirySeconds())
                        .plusMillis(ThreadLocalRandom.current().nextLong(0, 1000));
                    delayTaskRunner.scheduleIfAbsent(inboxInstance,
                        new SendLWTTask(delay, metadata.getMod(), inboxClient));
                } else {
                    if (Integer.compareUnsigned(metadata.getExpirySeconds(), UINT_MAX) < 0) {
                        // UINT_MAX never expire according to MQTT5 spec
                        Duration delay = Duration.ofSeconds(metadata.getExpirySeconds());
                        delayTaskRunner.scheduleIfAbsent(inboxInstance,
                            new ExpireInboxTask(delay, metadata.getMod(), inboxClient));
                    }
                }
            }));
        };
    }

    private Runnable batchDetach(BatchDetachRequest request, BatchDetachReply.Builder replyBuilder, IKVWriter writer) {
        Map<String, Set<InboxMetadata>> toBeUpdated = new HashMap<>();
        Map<String, Set<InboxMetadata>> toBeEnsured = new HashMap<>();
        Map<String, Set<InboxMetadata>> toBeScheduled = new HashMap<>();
        for (BatchDetachRequest.Params params : request.getParamsList()) {
            SortedMap<Long, InboxMetadata> inboxInstances = tenantStates.getAll(params.getTenantId(),
                params.getInboxId());
            if (inboxInstances.isEmpty()) {
                replyBuilder.addCode(BatchDetachReply.Code.NO_INBOX);
                continue;
            }
            InboxMetadata metadata;
            if (params.hasVersion()) {
                metadata = inboxInstances.get(params.getVersion().getIncarnation());
                if (metadata == null) {
                    replyBuilder.addCode(BatchDetachReply.Code.NO_INBOX);
                    continue;
                }
                if (metadata.getMod() != params.getVersion().getMod()) {
                    replyBuilder.addCode(BatchDetachReply.Code.CONFLICT);
                    continue;
                }
            } else {
                metadata = inboxInstances.get(inboxInstances.lastKey());
            }
            InboxMetadata.Builder metadataBuilder = metadata.toBuilder()
                .setMod(params.getVersion().getMod() + 1)
                .setExpirySeconds(params.getExpirySeconds())
                .setDetachedAt(params.getNow());
            if (params.getDiscardLWT()) {
                metadataBuilder.clearLwt();
            }
            metadata = metadataBuilder.build();
            ByteString metadataKey = inboxInstanceStartKey(params.getTenantId(), params.getInboxId(),
                metadata.getIncarnation());
            writer.put(metadataKey, metadata.toByteString());
            toBeUpdated.computeIfAbsent(params.getTenantId(), k -> new HashSet<>()).add(metadata);

            if (request.getLeader().getRangeId().equals(id) && request.getLeader().getStoreId().equals(storeId)) {
                // setup expire task only on the leader
                toBeScheduled.computeIfAbsent(params.getTenantId(), k -> new HashSet<>()).add(metadata);
                for (InboxMetadata oldInboxMetadata : inboxInstances.headMap(inboxInstances.lastKey()).values()) {
                    toBeEnsured.computeIfAbsent(params.getTenantId(), k -> new HashSet<>()).add(oldInboxMetadata);
                }
            }
            replyBuilder.addCode(BatchDetachReply.Code.OK);
        }
        return () -> {
            toBeUpdated.forEach((tenantId, putSet) ->
                putSet.forEach(metadata -> tenantStates.upsert(tenantId, metadata)));
            toBeScheduled.forEach((tenantId, inboxSet) -> inboxSet.forEach(metadata -> {
                TenantInboxInstance inboxInstance = new TenantInboxInstance(tenantId,
                    new InboxInstance(metadata.getInboxId(), metadata.getIncarnation()));
                // schedule a task for sending LWT or expiry session
                if (metadata.hasLwt()) {
                    Duration delay = Duration.ofSeconds(
                            Integer.compareUnsigned(metadata.getLwt().getDelaySeconds(), metadata.getExpirySeconds())
                                < 0 ? metadata.getLwt().getDelaySeconds() : metadata.getExpirySeconds())
                        .plusMillis(ThreadLocalRandom.current().nextLong(0, 1000));
                    delayTaskRunner.schedule(inboxInstance, new SendLWTTask(delay, metadata.getMod(), inboxClient));
                } else {
                    if (Integer.compareUnsigned(metadata.getExpirySeconds(), UINT_MAX) < 0) {
                        // UINT_MAX never expire according to MQTT5 spec
                        Duration delay = Duration.ofSeconds(metadata.getExpirySeconds());
                        delayTaskRunner.schedule(inboxInstance,
                            new ExpireInboxTask(delay, metadata.getMod(), inboxClient));
                    }
                }
            }));
            toBeEnsured.forEach((tenantId, inboxSet) -> inboxSet.forEach(metadata -> {
                TenantInboxInstance inboxInstance = new TenantInboxInstance(tenantId,
                    new InboxInstance(metadata.getInboxId(), metadata.getIncarnation()));
                // schedule a task for sending LWT or expiry session
                if (metadata.hasLwt()) {
                    Duration delay = Duration.ofSeconds(
                            Integer.compareUnsigned(metadata.getLwt().getDelaySeconds(), metadata.getExpirySeconds())
                                < 0 ? metadata.getLwt().getDelaySeconds() : metadata.getExpirySeconds())
                        .plusMillis(ThreadLocalRandom.current().nextLong(0, 1000));
                    delayTaskRunner.scheduleIfAbsent(inboxInstance,
                        new SendLWTTask(delay, metadata.getMod(), inboxClient));
                } else {
                    if (Integer.compareUnsigned(metadata.getExpirySeconds(), UINT_MAX) < 0) {
                        // UINT_MAX never expire according to MQTT5 spec
                        Duration delay = Duration.ofSeconds(metadata.getExpirySeconds());
                        delayTaskRunner.scheduleIfAbsent(inboxInstance,
                            new ExpireInboxTask(delay, metadata.getMod(), inboxClient));
                    }
                }
            }));
        };
    }

    @SneakyThrows
    private Runnable batchDelete(BatchDeleteRequest request,
                                 BatchDeleteReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        Map<String, Set<InboxMetadata>> toBeRemoved = new HashMap<>();
        reader.refresh();
        IKVIterator itr = reader.iterator();
        for (BatchDeleteRequest.Params params : request.getParamsList()) {
            Optional<InboxMetadata> metadataOpt = tenantStates.get(params.getTenantId(), params.getInboxId(),
                params.getVersion().getIncarnation());
            if (metadataOpt.isEmpty()) {
                replyBuilder.addResult(
                    BatchDeleteReply.Result.newBuilder().setCode(BatchDeleteReply.Code.NO_INBOX).build());
                continue;
            }
            if (metadataOpt.get().getMod() != params.getVersion().getMod()) {
                replyBuilder.addResult(
                    BatchDeleteReply.Result.newBuilder().setCode(BatchDeleteReply.Code.CONFLICT).build());
                continue;
            }
            InboxMetadata metadata = metadataOpt.get();
            clearInboxInstance(metadata, itr, writer);
            toBeRemoved.computeIfAbsent(params.getTenantId(), k -> new HashSet<>()).add(metadata);
            replyBuilder.addResult(BatchDeleteReply.Result.newBuilder().setCode(BatchDeleteReply.Code.OK)
                .putAllTopicFilters(metadata.getTopicFiltersMap()).build());
        }
        return () -> toBeRemoved.forEach((tenantId, removeSet) ->
            removeSet.forEach(inboxMetadata ->
                tenantStates.remove(tenantId, inboxMetadata.getInboxId(), inboxMetadata.getIncarnation())));
    }

    private Runnable batchSub(BatchSubRequest request, BatchSubReply.Builder replyBuilder, IKVWriter writer) {
        Map<String, Set<InboxMetadata>> toBeCached = new HashMap<>();
        for (BatchSubRequest.Params params : request.getParamsList()) {
            Optional<InboxMetadata> metadataOpt = tenantStates.get(params.getTenantId(), params.getInboxId(),
                params.getVersion().getIncarnation());
            if (metadataOpt.isEmpty()) {
                replyBuilder.addCode(BatchSubReply.Code.NO_INBOX);
                continue;
            }
            if (metadataOpt.get().getMod() != params.getVersion().getMod()) {
                replyBuilder.addCode(BatchSubReply.Code.CONFLICT);
                continue;
            }
            int maxTopicFilters = params.getMaxTopicFilters();
            InboxMetadata metadata = metadataOpt.get();
            InboxMetadata.Builder metadataBuilder = metadataOpt.get().toBuilder();
            if (metadata.getTopicFiltersCount() < maxTopicFilters) {
                TopicFilterOption option = metadataBuilder.getTopicFiltersMap().get(params.getTopicFilter());
                if (option != null && option.equals(params.getOption())) {
                    replyBuilder.addCode(BatchSubReply.Code.EXISTS);
                } else {
                    metadataBuilder.putTopicFilters(params.getTopicFilter(), params.getOption());
                    replyBuilder.addCode(BatchSubReply.Code.OK);
                }
            } else {
                replyBuilder.addCode(BatchSubReply.Code.EXCEED_LIMIT);
            }
            metadata = metadataBuilder.setLastActiveTime(params.getNow()).build();
            ByteString inboxInstStartKey = inboxInstanceStartKey(params.getTenantId(), params.getInboxId(),
                params.getVersion().getIncarnation());
            writer.put(inboxInstStartKey, metadata.toByteString());
            toBeCached.computeIfAbsent(params.getTenantId(), k -> new HashSet<>()).add(metadata);
        }
        return () -> updateTenantStates(toBeCached);
    }

    private Runnable batchUnsub(BatchUnsubRequest request, BatchUnsubReply.Builder replyBuilder, IKVWriter write) {
        Map<String, Set<InboxMetadata>> toBeCached = new HashMap<>();
        for (BatchUnsubRequest.Params params : request.getParamsList()) {
            Optional<InboxMetadata> metadataOpt = tenantStates.get(params.getTenantId(), params.getInboxId(),
                params.getVersion().getIncarnation());
            if (metadataOpt.isEmpty()) {
                replyBuilder.addResult(
                    BatchUnsubReply.Result.newBuilder().setCode(BatchUnsubReply.Code.NO_INBOX).build());
                continue;
            }
            if (metadataOpt.get().getMod() != params.getVersion().getMod()) {
                replyBuilder.addResult(
                    BatchUnsubReply.Result.newBuilder().setCode(BatchUnsubReply.Code.CONFLICT).build());
                continue;
            }
            InboxMetadata metadata = metadataOpt.get();
            InboxMetadata.Builder metadataBuilder = metadata.toBuilder();
            if (metadataBuilder.containsTopicFilters(params.getTopicFilter())) {
                metadataBuilder.removeTopicFilters(params.getTopicFilter());
                replyBuilder.addResult(BatchUnsubReply.Result.newBuilder().setCode(BatchUnsubReply.Code.OK)
                    .setOption(metadata.getTopicFiltersMap().get(params.getTopicFilter())).build());
            } else {
                replyBuilder.addResult(
                    BatchUnsubReply.Result.newBuilder().setCode(BatchUnsubReply.Code.NO_SUB).build());
            }
            metadata = metadataBuilder.setLastActiveTime(params.getNow()).build();
            ByteString inboxInstStartKey = inboxInstanceStartKey(params.getTenantId(), params.getInboxId(),
                params.getVersion().getIncarnation());
            write.put(inboxInstStartKey, metadata.toByteString());
            toBeCached.computeIfAbsent(params.getTenantId(), k -> new HashSet<>()).add(metadata);
        }
        return () -> updateTenantStates(toBeCached);
    }

    private void clearInboxInstance(InboxMetadata metadata, IKVIterator itr, IKVWriter writer) {
        ByteString startKey = inboxInstanceStartKey(metadata.getClient().getTenantId(), metadata.getInboxId(),
            metadata.getIncarnation());
        if (metadata.getQos0NextSeq() > 0) {
            // find lowest seq of qos0 message
            itr.seek(qos0QueuePrefix(startKey));
            if (itr.isValid() && itr.key().startsWith(startKey)) {
                for (long s = parseSeq(startKey, itr.key()); s < metadata.getQos0NextSeq(); s++) {
                    writer.delete(qos0MsgKey(startKey, s));
                }
            }
        }
        if (metadata.getSendBufferNextSeq() > 0) {
            itr.seek(sendBufferPrefix(startKey));
            if (itr.isValid() && itr.key().startsWith(startKey)) {
                for (long s = parseSeq(startKey, itr.key()); s < metadata.getSendBufferNextSeq(); s++) {
                    writer.delete(bufferedMsgKey(startKey, s));
                }
            }
        }
        writer.delete(startKey);
    }

    @SneakyThrows
    private CompletableFuture<GCReply> gc(GCRequest request) {
        List<CompletableFuture<Void>> expireFutures = new LinkedList<>();
        for (String tenantId : tenantStates.getAllTenantIds()) {
            expireFutures.add(expireTenant(tenantId, Duration.ZERO, request.getNow()));
        }
        return CompletableFuture.allOf(expireFutures.toArray(CompletableFuture[]::new))
            .thenApply(v -> GCReply.newBuilder().build());
    }

    private Runnable batchInsert(BatchInsertRequest request,
                                 BatchInsertReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        Map<String, Set<InboxMetadata>> toBeCached = new HashMap<>();
        Map<ClientInfo, Map<QoS, Integer>> dropCountMap = new HashMap<>();
        Map<ClientInfo, Boolean> dropOldestMap = new HashMap<>();
        for (InsertRequest params : request.getRequestList()) {
            Optional<InboxMetadata> metadataOpt = tenantStates.get(params.getTenantId(), params.getInboxId(),
                params.getIncarnation());
            if (metadataOpt.isEmpty()) {
                replyBuilder.addResult(InsertResult.newBuilder().setCode(InsertResult.Code.NO_INBOX).build());
                continue;
            }
            InboxMetadata metadata = metadataOpt.get();
            List<SubMessage> qos0MsgList = new ArrayList<>();
            List<SubMessage> bufferMsgList = new ArrayList<>();
            Set<InsertResult.SubStatus> insertResults = new HashSet<>();
            for (SubMessagePack messagePack : params.getMessagePackList()) {
                Map<String, Long> matchedTopicFilters = messagePack.getMatchedTopicFiltersMap();
                Map<String, TopicFilterOption> qos0TopicFilters = new HashMap<>();
                Map<String, TopicFilterOption> qos1TopicFilters = new HashMap<>();
                Map<String, TopicFilterOption> qos2TopicFilters = new HashMap<>();
                TopicMessagePack topicMsgPack = messagePack.getMessages();
                for (String matchedTopicFilter : matchedTopicFilters.keySet()) {
                    long matchedIncarnation = matchedTopicFilters.get(matchedTopicFilter);
                    TopicFilterOption tfOption = metadata.getTopicFiltersMap().get(matchedTopicFilter);
                    if (tfOption == null) {
                        insertResults.add(
                            InsertResult.SubStatus.newBuilder().setTopicFilter(matchedTopicFilter)
                                .setIncarnation(matchedIncarnation).setRejected(true).build());
                    } else {
                        if (tfOption.getIncarnation() > matchedIncarnation) {
                            // messages from old sub incarnation
                            log.debug("Receive message from previous subscription: topicFilter={}, inc={}, prevInc={}",
                                matchedTopicFilter, tfOption.getIncarnation(), matchedIncarnation);
                        }
                        switch (tfOption.getQos()) {
                            case AT_MOST_ONCE -> qos0TopicFilters.put(matchedTopicFilter, tfOption);
                            case AT_LEAST_ONCE -> qos1TopicFilters.put(matchedTopicFilter, tfOption);
                            case EXACTLY_ONCE -> qos2TopicFilters.put(matchedTopicFilter, tfOption);
                            default -> {
                                // never happens
                            }
                        }
                        insertResults.add(InsertResult.SubStatus.newBuilder()
                            .setTopicFilter(matchedTopicFilter)
                            .setIncarnation(matchedIncarnation)
                            .setRejected(false)
                            .build());
                    }
                }
                String topic = topicMsgPack.getTopic();
                for (TopicMessagePack.PublisherPack publisherPack : topicMsgPack.getMessageList()) {
                    for (Message message : publisherPack.getMessageList()) {
                        ClientInfo publisher = publisherPack.getPublisher();
                        switch (message.getPubQoS()) {
                            case AT_MOST_ONCE -> {
                                // add to qos0 inbox queue
                                Map<String, TopicFilterOption> topicFilters = new HashMap<>();
                                topicFilters.putAll(qos0TopicFilters);
                                topicFilters.putAll(qos1TopicFilters);
                                topicFilters.putAll(qos2TopicFilters);
                                qos0MsgList.add(new SubMessage(topic, publisher, message, topicFilters));
                            }
                            case AT_LEAST_ONCE, EXACTLY_ONCE -> {
                                if (!qos0TopicFilters.isEmpty()) {
                                    // add to qos0 inbox queue
                                    qos0MsgList.add(new SubMessage(topic, publisher, message, qos0TopicFilters));
                                }
                                if (!qos1TopicFilters.isEmpty() || !qos2TopicFilters.isEmpty()) {
                                    // add to buffer queue for qos1 and qos2 messages
                                    Map<String, TopicFilterOption> topicFilters = new HashMap<>();
                                    topicFilters.putAll(qos1TopicFilters);
                                    topicFilters.putAll(qos2TopicFilters);
                                    bufferMsgList.add(new SubMessage(topic, publisher, message, topicFilters));
                                }
                            }
                            default -> {
                                // never happens
                            }
                        }
                    }
                }
            }
            InboxMetadata.Builder metadataBuilder = metadata.toBuilder();
            dropOldestMap.put(metadata.getClient(), metadata.getDropOldest());
            ByteString inboxInstStartKey = inboxInstanceStartKey(params.getTenantId(), params.getInboxId(),
                params.getIncarnation());
            Map<QoS, Integer> dropCounts = insertInbox(inboxInstStartKey, qos0MsgList, bufferMsgList, metadataBuilder,
                reader, writer);
            metadata = metadataBuilder.build();

            Map<QoS, Integer> aggregated = dropCountMap.computeIfAbsent(metadata.getClient(), k -> new HashMap<>());
            dropCounts.forEach((qos, count) -> aggregated.compute(qos, (k, v) -> {
                if (v == null) {
                    return count;
                }
                return v + count;
            }));

            replyBuilder.addResult(InsertResult.newBuilder()
                .setCode(InsertResult.Code.OK)
                .addAllResult(insertResults)
                .build());

            writer.put(inboxInstStartKey, metadata.toByteString());

            toBeCached.computeIfAbsent(params.getTenantId(), k -> new HashSet<>()).add(metadata);
        }
        return () -> {
            toBeCached.forEach((tenantId, putSet) -> putSet.forEach(
                inboxMetadata -> tenantStates.upsert(tenantId, inboxMetadata)));
            dropCountMap.forEach((client, dropCounts) -> dropCounts.forEach((qos, count) -> {
                if (count > 0) {
                    eventCollector.report(getLocal(Overflowed.class)
                        .oldest(dropOldestMap.get(client))
                        .isQoS0(qos == QoS.AT_MOST_ONCE)
                        .clientInfo(client)
                        .dropCount(count));
                }
            }));
        };
    }

    private Map<QoS, Integer> insertInbox(ByteString inboxKeyPrefix,
                                          List<SubMessage> qos0MsgList,
                                          List<SubMessage> bufferedMsgList,
                                          InboxMetadata.Builder metaBuilder,
                                          IKVReader reader,
                                          IKVWriter writer) {
        Map<QoS, Integer> dropCounts = new HashMap<>();
        if (!qos0MsgList.isEmpty()) {
            long startSeq = metaBuilder.getQos0StartSeq();
            long nextSeq = metaBuilder.getQos0NextSeq();
            int dropCount = insertToInbox(inboxKeyPrefix, startSeq, nextSeq, metaBuilder.getLimit(),
                metaBuilder.getDropOldest(), KVSchemaUtil::qos0MsgKey, metaBuilder::setQos0StartSeq,
                metaBuilder::setQos0NextSeq, qos0MsgList, reader, writer);
            if (dropCount > 0) {
                dropCounts.put(QoS.AT_MOST_ONCE, dropCount);
            }
        }
        if (!bufferedMsgList.isEmpty()) {
            long startSeq = metaBuilder.getSendBufferStartSeq();
            long nextSeq = metaBuilder.getSendBufferNextSeq();
            int dropCount = insertToInbox(inboxKeyPrefix, startSeq, nextSeq, metaBuilder.getLimit(), false,
                KVSchemaUtil::bufferedMsgKey, metaBuilder::setSendBufferStartSeq, metaBuilder::setSendBufferNextSeq,
                bufferedMsgList, reader, writer);
            if (dropCount > 0) {
                dropCounts.put(QoS.AT_LEAST_ONCE, dropCount);
            }
        }
        return dropCounts;
    }

    @SneakyThrows
    private int insertToInbox(ByteString inboxKeyPrefix,
                              long startSeq,
                              long nextSeq,
                              int limit,
                              boolean dropOldest,
                              BiFunction<ByteString, Long, ByteString> keyGenerator,
                              Function<Long, InboxMetadata.Builder> startSeqSetter,
                              Function<Long, InboxMetadata.Builder> nextSeqSetter,
                              List<SubMessage> messages,
                              IKVReader reader,
                              IKVWriter writer) {
        int newMsgCount = messages.size();
        int currCount = (int) (nextSeq - startSeq);
        int dropCount = currCount + newMsgCount - limit;
        if (dropOldest) {
            if (dropCount > 0) {
                if (dropCount >= currCount) {
                    // drop all
                    writer.clear(Boundary.newBuilder().setStartKey(keyGenerator.apply(inboxKeyPrefix, startSeq))
                        .setEndKey(keyGenerator.apply(inboxKeyPrefix, nextSeq)).build());
                    // and trim if needed
                    if (dropCount > currCount) {
                        messages = messages.subList(dropCount - currCount, newMsgCount);
                    }
                    writer.insert(keyGenerator.apply(inboxKeyPrefix, startSeq + dropCount),
                        buildInboxMessageList(startSeq + dropCount, messages).toByteString());
                } else {
                    reader.refresh();
                    IKVIterator itr = reader.iterator();
                    // drop partially
                    itr.seekForPrev(keyGenerator.apply(inboxKeyPrefix, startSeq + dropCount));
                    long beginSeq = parseSeq(inboxKeyPrefix, itr.key());
                    List<InboxMessage> msgList = ZeroCopyParser.parse(itr.value(), InboxMessageList.parser())
                        .getMessageList();
                    InboxMessageList.Builder msgListBuilder = InboxMessageList.newBuilder();
                    List<InboxMessage> subMsgList = msgList.subList((int) (startSeq + dropCount - beginSeq),
                        msgList.size());
                    if (!subMsgList.isEmpty()) {
                        msgListBuilder.addAllMessage(subMsgList).addAllMessage(
                            buildInboxMessageList(subMsgList.get(subMsgList.size() - 1).getSeq() + 1,
                                messages).getMessageList());
                    } else {
                        msgListBuilder.addAllMessage(
                            buildInboxMessageList(startSeq + dropCount, messages).getMessageList());
                    }
                    writer.clear(Boundary.newBuilder().setStartKey(keyGenerator.apply(inboxKeyPrefix, startSeq))
                        .setEndKey(keyGenerator.apply(inboxKeyPrefix, startSeq + dropCount)).build());
                    if (beginSeq == startSeq + dropCount) {
                        // override existing key
                        writer.put(keyGenerator.apply(inboxKeyPrefix, startSeq + dropCount),
                            msgListBuilder.build().toByteString());
                    } else {
                        // insert new key
                        writer.insert(keyGenerator.apply(inboxKeyPrefix, startSeq + dropCount),
                            msgListBuilder.build().toByteString());
                    }
                }
                startSeq += dropCount;
            } else {
                writer.insert(keyGenerator.apply(inboxKeyPrefix, nextSeq),
                    buildInboxMessageList(nextSeq, messages).toByteString());
            }
            nextSeq += newMsgCount;
            startSeqSetter.apply(startSeq);
            nextSeqSetter.apply(nextSeq);
        } else {
            if (dropCount < newMsgCount) {
                List<SubMessage> subMessages = dropCount > 0 ? messages.subList(0, newMsgCount - dropCount) : messages;
                writer.insert(keyGenerator.apply(inboxKeyPrefix, nextSeq),
                    buildInboxMessageList(nextSeq, subMessages).toByteString());
                nextSeq += subMessages.size();
            }
            // else drop all new messages;
        }
        startSeqSetter.apply(startSeq);
        nextSeqSetter.apply(nextSeq);
        return Math.max(dropCount, 0);
    }

    private InboxMessageList buildInboxMessageList(long beginSeq, List<SubMessage> subMessages) {
        InboxMessageList.Builder listBuilder = InboxMessageList.newBuilder();
        for (SubMessage subMessage : subMessages) {
            listBuilder.addMessage(
                InboxMessage.newBuilder().setSeq(beginSeq).putAllMatchedTopicFilter(subMessage.matchedTopicFilters)
                    .setMsg(TopicMessage.newBuilder().setTopic(subMessage.topic).setPublisher(subMessage.publisher)
                        .setMessage(subMessage.message).build()).build());
            beginSeq++;
        }
        return listBuilder.build();
    }

    @SneakyThrows
    private Runnable batchCommit(BatchCommitRequest request,
                                 BatchCommitReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        Map<String, Set<InboxMetadata>> toBeCached = new HashMap<>();
        for (BatchCommitRequest.Params params : request.getParamsList()) {
            Optional<InboxMetadata> metadataOpt = tenantStates.get(params.getTenantId(), params.getInboxId(),
                params.getVersion().getIncarnation());
            if (metadataOpt.isEmpty()) {
                replyBuilder.addCode(BatchCommitReply.Code.NO_INBOX);
                continue;
            }
            if (metadataOpt.get().getMod() != params.getVersion().getMod()) {
                replyBuilder.addCode(BatchCommitReply.Code.CONFLICT);
                continue;
            }
            ByteString inboxInstStartKey = inboxInstanceStartKey(params.getTenantId(), params.getInboxId(),
                params.getVersion().getIncarnation());
            InboxMetadata metadata = metadataOpt.get();
            InboxMetadata.Builder metaBuilder = metadata.toBuilder();
            commitInbox(inboxInstStartKey, params, metaBuilder, reader, writer);
            metadata = metaBuilder.setLastActiveTime(params.getNow()).build();
            writer.put(inboxInstStartKey, metadata.toByteString());
            replyBuilder.addCode(BatchCommitReply.Code.OK);
            toBeCached.computeIfAbsent(params.getTenantId(), k -> new HashSet<>()).add(metadata);
        }
        return () -> updateTenantStates(toBeCached);
    }

    private void commitInbox(ByteString scopedInboxId,
                             BatchCommitRequest.Params params,
                             InboxMetadata.Builder metaBuilder,
                             IKVReader reader,
                             IKVWriter writer) {
        if (params.hasQos0UpToSeq()) {
            long startSeq = metaBuilder.getQos0StartSeq();
            long nextSeq = metaBuilder.getQos0NextSeq();
            long commitSeq = params.getQos0UpToSeq();
            commitToInbox(scopedInboxId, startSeq, nextSeq, commitSeq, KVSchemaUtil::qos0MsgKey,
                metaBuilder::setQos0StartSeq, reader, writer);
        }
        if (params.hasSendBufferUpToSeq()) {
            long startSeq = metaBuilder.getSendBufferStartSeq();
            long nextSeq = metaBuilder.getSendBufferNextSeq();
            long commitSeq = params.getSendBufferUpToSeq();
            commitToInbox(scopedInboxId, startSeq, nextSeq, commitSeq, KVSchemaUtil::bufferedMsgKey,
                metaBuilder::setSendBufferStartSeq, reader, writer);
        }
    }

    @SneakyThrows
    private void commitToInbox(ByteString scopedInboxId,
                               long startSeq,
                               long nextSeq,
                               long commitSeq,
                               BiFunction<ByteString, Long, ByteString> keyGenerator,
                               Function<Long, InboxMetadata.Builder> metadataSetter,
                               IKVReader reader,
                               IKVWriter writer) {
        if (startSeq <= commitSeq && commitSeq < nextSeq) {
            while (startSeq <= commitSeq) {
                ByteString msgKey = keyGenerator.apply(scopedInboxId, startSeq);
                Optional<ByteString> msgListData = reader.get(msgKey);
                if (msgListData.isEmpty()) {
                    break;
                }
                List<InboxMessage> msgList = ZeroCopyParser.parse(msgListData.get(), InboxMessageList.parser())
                    .getMessageList();
                long lastSeq = msgList.get(msgList.size() - 1).getSeq();
                if (lastSeq <= commitSeq) {
                    writer.delete(msgKey);
                    startSeq = lastSeq + 1;
                } else {
                    writer.delete(msgKey);
                    msgList = msgList.subList((int) (commitSeq - startSeq + 1), msgList.size());
                    writer.insert(keyGenerator.apply(scopedInboxId, commitSeq + 1),
                        InboxMessageList.newBuilder().addAllMessage(msgList).build().toByteString());
                    startSeq = commitSeq + 1;
                    break;
                }
            }
            metadataSetter.apply(startSeq);
        }
    }

    private CompletableFuture<ExpireTenantReply> expireTenant(ExpireTenantRequest request) {
        return expireTenant(request.getTenantId(), Duration.ofSeconds(request.getExpirySeconds()), request.getNow())
            .thenApply(v -> ExpireTenantReply.newBuilder().build());
    }

    private CompletableFuture<Void> expireTenant(String tenantId, Duration expireTime, long now) {
        Map<String, SortedMap<Long, InboxMetadata>> inboxSet = tenantStates.getAll(tenantId);
        List<CompletableFuture<ExpireCheckResult>> onlineCheckFutures = new LinkedList<>();
        for (SortedMap<Long, InboxMetadata> inboxVersions : inboxSet.values()) {
            InboxMetadata latestInboxMetadata;
            if (inboxVersions.size() > 1) {
                long latestVersion = inboxVersions.lastKey();
                for (long olderVersion : inboxVersions.headMap(latestVersion).keySet()) {
                    InboxMetadata metadata = inboxVersions.get(olderVersion);
                    // expire all older version
                    onlineCheckFutures.add(
                        CompletableFuture.completedFuture(new ExpireCheckResult(metadata, true)));
                }
                // check latest version
                latestInboxMetadata = inboxVersions.get(latestVersion);
            } else {
                latestInboxMetadata = inboxVersions.get(inboxVersions.firstKey());
            }
            if (latestInboxMetadata.hasDetachedAt()) {
                long detachedAtMillis = latestInboxMetadata.getDetachedAt();
                int expireSeconds = getExpireSeconds(expireTime, latestInboxMetadata);
                if (Integer.compareUnsigned(expireSeconds, UINT_MAX) == 0) {
                    // never expire according to MQTT5 spec
                    onlineCheckFutures.add(
                        CompletableFuture.completedFuture(new ExpireCheckResult(latestInboxMetadata, false)));
                    continue;
                }
                long expireMillis = Duration.ofSeconds(expireSeconds).toMillis();
                if (detachedAtMillis + expireMillis + 5000 > now) {
                    onlineCheckFutures.add(
                        CompletableFuture.completedFuture(new ExpireCheckResult(latestInboxMetadata, false)));
                } else {
                    // expired
                    onlineCheckFutures.add(
                        CompletableFuture.completedFuture(new ExpireCheckResult(latestInboxMetadata, true)));
                }
            } else {
                // not detached
                int expireSeconds = latestInboxMetadata.getExpirySeconds();
                if (Integer.compareUnsigned(expireSeconds, UINT_MAX) == 0) {
                    // never expire according to mqtt5 spec
                    onlineCheckFutures.add(
                        CompletableFuture.completedFuture(new ExpireCheckResult(latestInboxMetadata, false)));
                    continue;
                }
                long lastActiveTime = latestInboxMetadata.getLastActiveTime();
                long detachTimeoutMillis = detachTimeout.toMillis();
                if (lastActiveTime + detachTimeoutMillis > now) {
                    onlineCheckFutures.add(
                        CompletableFuture.completedFuture(new ExpireCheckResult(latestInboxMetadata, false)));
                } else {
                    // check online status
                    OnlineCheckRequest clientId = new OnlineCheckRequest(tenantId,
                        latestInboxMetadata.getClient().getMetadataMap().get(MQTT_USER_ID_KEY),
                        latestInboxMetadata.getClient().getMetadataMap().get(MQTT_CLIENT_ID_KEY));
                    onlineCheckFutures.add(sessionDictClient.exist(clientId)
                        .thenApply((v) -> {
                            if (v == OnlineCheckResult.NOT_EXISTS) {
                                return new ExpireCheckResult(latestInboxMetadata, true);
                            } else {
                                return new ExpireCheckResult(latestInboxMetadata, false);
                            }
                        }));
                }
            }
        }
        return CompletableFuture.allOf(onlineCheckFutures.toArray(CompletableFuture[]::new))
            .thenAccept(v -> {
                for (CompletableFuture<ExpireCheckResult> future : onlineCheckFutures) {
                    ExpireCheckResult result = future.join();
                    if (result.expired) {
                        TenantInboxInstance inboxInstance = new TenantInboxInstance(tenantId,
                            new InboxInstance(result.metadata.getInboxId(), result.metadata.getIncarnation()));
                        delayTaskRunner.schedule(inboxInstance,
                            new ExpireInboxTask(Duration.ofMillis(0), result.metadata.getMod(), inboxClient));
                    }
                }
            });
    }

    private void updateTenantStates(Map<String, Set<InboxMetadata>> toBeCached) {
        toBeCached.forEach((tenantId, inboxSet) ->
            inboxSet.forEach(inboxMetadata -> tenantStates.upsert(tenantId, inboxMetadata)));
    }

    private void load() {
        try (IKVCloseableReader reader = rangeReaderProvider.get()) {
            IKVIterator itr = reader.iterator();
            int probe = 0;
            for (itr.seekToFirst(); itr.isValid(); ) {
                if (isInboxInstanceStartKey(itr.key())) {
                    probe = 0;
                    try {
                        tenantStates.upsert(parseTenantId(itr.key()), InboxMetadata.parseFrom(itr.value()));
                    } catch (InvalidProtocolBufferException e) {
                        log.error("Unexpected error", e);
                    } finally {
                        itr.next();
                        probe++;
                    }
                } else {
                    if (probe < 20) {
                        itr.next();
                        probe++;
                    } else {
                        if (isInboxInstanceKey(itr.key())) {
                            itr.seek(upperBound(parseInboxInstanceStartKeyPrefix(itr.key())));
                        } else {
                            itr.next();
                            probe++;
                        }
                    }
                }
            }
            log.debug("Tenant states loaded: rangeId={}", KVRangeIdUtil.toString(id));
        }
    }

    private boolean hasExpired(InboxMetadata metadata, long nowTS) {
        if (!metadata.hasDetachedAt()) {
            return false;
        }
        return Duration.ofMillis(metadata.getDetachedAt()).plusSeconds(metadata.getExpirySeconds()).toMillis() < nowTS;
    }

    private record ExpireCheckResult(InboxMetadata metadata, boolean expired) {

    }

    private record SubMessage(String topic, ClientInfo publisher, Message message,
                              Map<String, TopicFilterOption> matchedTopicFilters) {
    }
}
