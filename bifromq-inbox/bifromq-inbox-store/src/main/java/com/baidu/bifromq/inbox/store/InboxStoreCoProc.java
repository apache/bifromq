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

package com.baidu.bifromq.inbox.store;

import static com.baidu.bifromq.basekv.localengine.RangeUtil.upperBound;
import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.intersect;
import static com.baidu.bifromq.inbox.util.KeyUtil.buildMsgKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.isInboxMetadataKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.isQoS0MessageKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.isQoS1MessageKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.isQoS2MessageIndexKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseQoS2Index;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseScopedInboxId;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseSeq;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseTenantId;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos0InboxMsgKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos0InboxPrefix;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos1InboxMsgKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos1InboxPrefix;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos2InboxIndex;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos2InboxMsgKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos2InboxPrefix;
import static com.baidu.bifromq.inbox.util.KeyUtil.tenantPrefix;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProc;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.baidu.bifromq.basekv.store.range.ILoadTracker;
import com.baidu.bifromq.inbox.storage.proto.CollectMetricsReply;
import com.baidu.bifromq.inbox.storage.proto.CollectMetricsRequest;
import com.baidu.bifromq.inbox.storage.proto.CreateParams;
import com.baidu.bifromq.inbox.storage.proto.CreateReply;
import com.baidu.bifromq.inbox.storage.proto.CreateRequest;
import com.baidu.bifromq.inbox.storage.proto.FetchParams;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.GCReply;
import com.baidu.bifromq.inbox.storage.proto.GCRequest;
import com.baidu.bifromq.inbox.storage.proto.HasReply;
import com.baidu.bifromq.inbox.storage.proto.HasRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxComSertReply;
import com.baidu.bifromq.inbox.storage.proto.InboxComSertRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxCommit;
import com.baidu.bifromq.inbox.storage.proto.InboxCommitReply;
import com.baidu.bifromq.inbox.storage.proto.InboxCommitRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxFetchReply;
import com.baidu.bifromq.inbox.storage.proto.InboxFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxInsertReply;
import com.baidu.bifromq.inbox.storage.proto.InboxInsertRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxInsertResult;
import com.baidu.bifromq.inbox.storage.proto.InboxMessage;
import com.baidu.bifromq.inbox.storage.proto.InboxMessageList;
import com.baidu.bifromq.inbox.storage.proto.InboxMetadata;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.MessagePack;
import com.baidu.bifromq.inbox.storage.proto.TouchReply;
import com.baidu.bifromq.inbox.storage.proto.TouchRequest;
import com.baidu.bifromq.inbox.util.KeyUtil;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.inboxservice.Overflowed;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessage;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class InboxStoreCoProc implements IKVRangeCoProc {
    private final IEventCollector eventCollector;
    private final Clock clock;
    private final Duration purgeDelay;

    InboxStoreCoProc(KVRangeId id,
                     Supplier<IKVRangeReader> rangeReaderProvider,
                     IEventCollector eventCollector,
                     Clock clock,
                     Duration purgeDelay,
                     ILoadTracker loadTracker) {
        this.eventCollector = eventCollector;
        this.clock = clock;
        this.purgeDelay = purgeDelay;
    }

    @Override
    public CompletableFuture<ByteString> query(ByteString input, IKVReader reader) {
        try {
            InboxServiceROCoProcInput coProcInput = InboxServiceROCoProcInput.parseFrom(input);
            InboxServiceROCoProcOutput.Builder outputBuilder = InboxServiceROCoProcOutput.newBuilder()
                .setReqId(coProcInput.getReqId());
            switch (coProcInput.getInputCase()) {
                case HAS -> outputBuilder.setHas(hasInbox(coProcInput.getHas(), reader));
                case FETCH -> outputBuilder.setFetch(fetch(coProcInput.getFetch(), reader));
                case COLLECTMETRICS ->
                    outputBuilder.setCollectedMetrics(collect(coProcInput.getCollectMetrics(), reader));
            }
            return CompletableFuture.completedFuture(outputBuilder.build().toByteString());
        } catch (Throwable e) {
            log.error("Query co-proc failed", e);
            return CompletableFuture.failedFuture(new IllegalStateException("Query co-proc failed", e));
        }
    }

    @SneakyThrows
    @Override
    public Supplier<ByteString> mutate(ByteString input, IKVReader reader, IKVWriter writer) {
        InboxServiceRWCoProcInput coProcInput = InboxServiceRWCoProcInput.parseFrom(input);
        InboxServiceRWCoProcOutput.Builder replyBuilder =
            InboxServiceRWCoProcOutput.newBuilder().setReqId(coProcInput.getReqId());
        switch (coProcInput.getTypeCase()) {
            case CREATEINBOX -> replyBuilder.setCreateInbox(createInbox(coProcInput.getCreateInbox(), reader, writer));
            case INSERT -> replyBuilder.setInsert(batchInsert(coProcInput.getInsert(), reader, writer));
            case COMMIT -> replyBuilder.setCommit(batchCommit(coProcInput.getCommit(), reader, writer));
            case INSERTANDCOMMIT ->
                replyBuilder.setInsertAndCommit(batchInsertAndCommit(coProcInput.getInsertAndCommit(), reader, writer));
            case TOUCH -> replyBuilder.setTouch(touch(coProcInput.getTouch(), reader, writer));
            case GC -> replyBuilder.setGc(gc(coProcInput.getGc(), reader, writer));
        }
        ByteString output = replyBuilder.build().toByteString();
        return () -> output;
    }

    @Override
    public void close() {
    }

    @SneakyThrows
    private HasReply hasInbox(HasRequest request, IKVReader reader) {
        HasReply.Builder replyBuilder = HasReply.newBuilder();
        for (ByteString scopedInboxId : request.getScopedInboxIdList()) {
            Optional<ByteString> value = reader.get(scopedInboxId);
            if (value.isPresent()) {
                replyBuilder.putExists(scopedInboxId.toStringUtf8(), !hasExpired(InboxMetadata.parseFrom(value.get())));
            } else {
                replyBuilder.putExists(scopedInboxId.toStringUtf8(), false);
            }
        }
        return replyBuilder.build();
    }

    private InboxFetchReply fetch(InboxFetchRequest request, IKVReader reader) {
        InboxFetchReply.Builder replyBuilder = InboxFetchReply.newBuilder();
        IKVIterator itr = reader.iterator();
        for (String scopedInboxIdUtf8 : request.getInboxFetchMap().keySet()) {
            ByteString scopedInboxId = ByteString.copyFromUtf8(scopedInboxIdUtf8);
            FetchParams inboxFetch = request.getInboxFetchMap().get(scopedInboxIdUtf8);
            replyBuilder.putResult(scopedInboxIdUtf8, fetch(scopedInboxId, inboxFetch, itr, reader));
        }
        return replyBuilder.build();
    }

    private Fetched fetch(ByteString scopedInboxId, FetchParams request, IKVIterator itr, IKVReader reader) {
        Fetched.Builder replyBuilder = Fetched.newBuilder();
        int fetchCount = request.getMaxFetch();
        try {
            itr.seek(scopedInboxId);
            if (!itr.isValid() || !itr.key().equals(scopedInboxId)) {
                replyBuilder.setResult(Fetched.Result.NO_INBOX);
                return replyBuilder.build();
            }
            InboxMetadata metadata = InboxMetadata.parseFrom(itr.value());
            if (hasExpired(metadata)) {
                replyBuilder.setResult(Fetched.Result.NO_INBOX);
                return replyBuilder.build();
            }
            // deal with qos0 queue
            long startFetchFromSeq = !request.hasQos0StartAfter()
                ? metadata.getQos0LastFetchBeforeSeq()
                : Math.max(request.getQos0StartAfter() + 1, metadata.getQos0LastFetchBeforeSeq());
            if (startFetchFromSeq < metadata.getQos0NextSeq()) {
                itr.seekForPrev(qos0InboxMsgKey(scopedInboxId, startFetchFromSeq));
                if (itr.isValid() && isQoS0MessageKey(itr.key(), scopedInboxId)) {
                    long startSeq = parseSeq(scopedInboxId, itr.key());
                    InboxMessageList messageList = InboxMessageList.parseFrom(itr.value());
                    for (int i = (int) (startFetchFromSeq - startSeq); i < messageList.getMessageCount(); i++) {
                        if (fetchCount > 0) {
                            replyBuilder.addQos0Seq(startSeq + i);
                            replyBuilder.addQos0Msg(messageList.getMessage(i));
                            fetchCount--;
                        } else {
                            break;
                        }
                    }
                }
                itr.next();
                outer:
                while (fetchCount > 0 && itr.isValid() && isQoS0MessageKey(itr.key(), scopedInboxId)) {
                    long startSeq = parseSeq(scopedInboxId, itr.key());
                    InboxMessageList messageList = InboxMessageList.parseFrom(itr.value());
                    for (int i = 0; i < messageList.getMessageCount(); i++) {
                        if (fetchCount > 0) {
                            replyBuilder.addQos0Seq(startSeq + i);
                            replyBuilder.addQos0Msg(messageList.getMessage(i));
                            fetchCount--;
                        } else {
                            break outer;
                        }
                    }
                    itr.next();
                }
            }
            // deal with qos1 queue
            startFetchFromSeq =
                !request.hasQos1StartAfter()
                    ? metadata.getQos1LastCommitBeforeSeq()
                    : Math.max(request.getQos1StartAfter() + 1, metadata.getQos1LastCommitBeforeSeq());
            if (startFetchFromSeq < metadata.getQos1NextSeq()) {
                itr.seekForPrev(qos1InboxMsgKey(scopedInboxId, startFetchFromSeq));
                if (itr.isValid() && isQoS1MessageKey(itr.key(), scopedInboxId)) {
                    long startSeq = parseSeq(scopedInboxId, itr.key());
                    InboxMessageList messageList = InboxMessageList.parseFrom(itr.value());
                    for (int i = (int) (startFetchFromSeq - startSeq); i < messageList.getMessageCount(); i++) {
                        if (fetchCount > 0) {
                            replyBuilder.addQos1Seq(startSeq + i);
                            replyBuilder.addQos1Msg(messageList.getMessage(i));
                            fetchCount--;
                        } else {
                            break;
                        }
                    }
                }
                itr.next();
                outer:
                while (fetchCount > 0 && itr.isValid() && isQoS1MessageKey(itr.key(), scopedInboxId)) {
                    long startSeq = parseSeq(scopedInboxId, itr.key());
                    InboxMessageList messageList = InboxMessageList.parseFrom(itr.value());
                    for (int i = 0; i < messageList.getMessageCount(); i++) {
                        if (fetchCount > 0) {
                            replyBuilder.addQos1Seq(startSeq + i);
                            replyBuilder.addQos1Msg(messageList.getMessage(i));
                            fetchCount--;
                        } else {
                            break outer;
                        }
                    }
                    itr.next();
                }
            }
            // deal with qos2 queue
            startFetchFromSeq = !request.hasQos2StartAfter()
                ? metadata.getQos2LastCommitBeforeSeq()
                : Math.max(request.getQos2StartAfter() + 1, metadata.getQos2LastCommitBeforeSeq());
            if (startFetchFromSeq < metadata.getQos2NextSeq()) {
                itr.seek(qos2InboxIndex(scopedInboxId, startFetchFromSeq));
                while (fetchCount > 0 && itr.isValid() && isQoS2MessageIndexKey(itr.key(), scopedInboxId)) {
                    replyBuilder.addQos2Seq(parseQoS2Index(scopedInboxId, itr.key()));
                    Optional<ByteString> msgBytes = reader.get(qos2InboxMsgKey(scopedInboxId, itr.value()));
                    assert msgBytes.isPresent();
                    replyBuilder.addQos2Msg(InboxMessage.parseFrom(msgBytes.get()));
                    fetchCount--;
                    itr.next();
                }
            }
            return replyBuilder.setResult(Fetched.Result.OK).build();
        } catch (InvalidProtocolBufferException e) {
            return replyBuilder.setResult(Fetched.Result.ERROR).build();
        }
    }

    @SneakyThrows
    private CreateReply createInbox(CreateRequest request, IKVReader reader, IKVWriter writeClient) {
        for (String scopedInboxIdUtf8 : request.getInboxesMap().keySet()) {
            ByteString scopedInboxId = ByteString.copyFromUtf8(scopedInboxIdUtf8);
            CreateParams inboxParams = request.getInboxesMap().get(scopedInboxIdUtf8);
            Optional<ByteString> value = reader.get(scopedInboxId);
            if (value.isPresent()) {
                InboxMetadata metadata = InboxMetadata.parseFrom(value.get());
                if (hasExpired(metadata)) {
                    // clear all message belong to previous inbox
                    clearInbox(scopedInboxId, metadata, reader.iterator(), writeClient);
                    writeClient.put(
                        scopedInboxId,
                        InboxMetadata.newBuilder()
                            .setExpireSeconds(inboxParams.getExpireSeconds())
                            .setLimit(inboxParams.getLimit())
                            .setLastFetchTime(clock.millis())
                            .setDropOldest(inboxParams.getDropOldest())
                            .setQos0NextSeq(0)
                            .setQos1NextSeq(0)
                            .setQos2NextSeq(0)
                            .setClient(inboxParams.getClient())
                            .build()
                            .toByteString());
                }
            } else {
                writeClient.put(
                    scopedInboxId,
                    InboxMetadata.newBuilder()
                        .setExpireSeconds(inboxParams.getExpireSeconds())
                        .setLimit(inboxParams.getLimit())
                        .setLastFetchTime(clock.millis())
                        .setDropOldest(inboxParams.getDropOldest())
                        .setQos0NextSeq(0)
                        .setQos1NextSeq(0)
                        .setQos2NextSeq(0)
                        .setClient(inboxParams.getClient())
                        .build()
                        .toByteString());
            }
        }
        return CreateReply.getDefaultInstance();
    }


    private void clearInbox(ByteString scopedInboxId, InboxMetadata metadata, IKVIterator itr, IKVWriter writeClient) {
        if (metadata.getQos0NextSeq() > 0) {
            // find lowest seq of qos0 message
            itr.seek(qos0InboxPrefix(scopedInboxId));
            if (itr.isValid() && isQoS0MessageKey(itr.key(), scopedInboxId)) {
                for (long s = parseSeq(scopedInboxId, itr.key()); s < metadata.getQos0NextSeq(); s++) {
                    writeClient.delete(qos0InboxMsgKey(scopedInboxId, s));
                }
            }
        }
        if (metadata.getQos1NextSeq() > 0) {
            itr.seek(qos1InboxPrefix(scopedInboxId));
            if (itr.isValid() && isQoS1MessageKey(itr.key(), scopedInboxId)) {
                for (long s = parseSeq(scopedInboxId, itr.key()); s < metadata.getQos1NextSeq(); s++) {
                    writeClient.delete(qos1InboxMsgKey(scopedInboxId, s));
                }
            }
        }
        if (metadata.getQos2NextSeq() > 0) {
            itr.seek(qos2InboxPrefix(scopedInboxId));
            if (itr.isValid() && isQoS2MessageIndexKey(itr.key(), scopedInboxId)) {
                for (long seq = parseQoS2Index(scopedInboxId, itr.key()); seq < metadata.getQos2NextSeq(); seq++) {
                    writeClient.delete(qos2InboxIndex(scopedInboxId, seq));
                    writeClient.delete(qos2InboxMsgKey(scopedInboxId, itr.value()));
                }
            }
        }
        writeClient.delete(scopedInboxId);
    }

    @SneakyThrows
    private GCReply gc(GCRequest request, IKVReader reader, IKVWriter writeClient) {
        long start = System.nanoTime();
        long yieldThreshold = TimeUnit.NANOSECONDS.convert(100, TimeUnit.MILLISECONDS);
        IKVIterator itr = reader.iterator();
        for (itr.seekToFirst(); itr.isValid() && System.nanoTime() - start < yieldThreshold; ) {
            ByteString scopedInboxId = parseScopedInboxId(itr.key());
            if (isInboxMetadataKey(itr.key())) {
                InboxMetadata metadata = InboxMetadata.parseFrom(itr.value());
                if (isGCable(metadata)) {
                    clearInbox(scopedInboxId, metadata, itr, writeClient);
                }
            }
            itr.seek(upperBound(scopedInboxId));
        }
        return GCReply.newBuilder().setReqId(request.getReqId()).build();
    }

    private CollectMetricsReply collect(CollectMetricsRequest request, IKVReader reader) {
        CollectMetricsReply.Builder builder = CollectMetricsReply.newBuilder().setReqId(request.getReqId());
        try (IKVIterator itr = reader.iterator()) {
            for (itr.seekToFirst(); itr.isValid(); ) {
                String tenantId = parseTenantId(itr.key());
                ByteString startKey = tenantPrefix(tenantId);
                ByteString endKey = upperBound(tenantPrefix(tenantId));
                builder.putUsedSpaces(tenantId, reader.size(intersect(reader.range(), Range.newBuilder()
                    .setStartKey(startKey)
                    .setEndKey(endKey)
                    .build())));
                itr.seek(endKey);
            }
        } catch (Exception e) {
            // never happens
        }
        return builder.build();
    }

    private InboxComSertReply batchInsertAndCommit(InboxComSertRequest request, IKVReader reader, IKVWriter writer) {
        IKVIterator itr = reader.iterator();
        Map<SubInfo, InboxInsertResult.Result> results = new LinkedHashMap<>();
        Map<ByteString, List<MessagePack>> subMsgPacksByInbox = new HashMap<>();
        List<MessagePack> subMsgPackList = request.getInsertList();
        Map<ByteString, InboxCommit> commitInboxs = new HashMap<>();
        request.getCommitMap().forEach((k, v) -> commitInboxs.put(ByteString.copyFromUtf8(k), v));


        InboxComSertReply.Builder replyBuilder = InboxComSertReply.newBuilder();
        for (MessagePack subMsgPack : subMsgPackList) {
            SubInfo subInfo = subMsgPack.getSubInfo();
            ByteString scopedInboxId = KeyUtil.scopedInboxId(subInfo.getTenantId(), subInfo.getInboxId());
            results.put(subInfo, null);
            subMsgPacksByInbox.computeIfAbsent(scopedInboxId, k -> new LinkedList<>()).add(subMsgPack);
        }
        for (ByteString scopedInboxId : subMsgPacksByInbox.keySet()) {
            List<MessagePack> subMsgPacks = subMsgPacksByInbox.get(scopedInboxId);
            InboxCommit inboxCommit = commitInboxs.remove(scopedInboxId);
            Optional<ByteString> metadataBytes = reader.get(scopedInboxId);
            if (metadataBytes.isEmpty()) {
                subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InboxInsertResult.Result.NO_INBOX));
                if (inboxCommit != null) {
                    replyBuilder.putCommitResults(scopedInboxId.toStringUtf8(), false);
                }
                continue;
            }
            try {
                InboxMetadata metadata = InboxMetadata.parseFrom(metadataBytes.get());
                if (hasExpired(metadata)) {
                    subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InboxInsertResult.Result.NO_INBOX));
                    if (inboxCommit != null) {
                        replyBuilder.putCommitResults(scopedInboxId.toStringUtf8(), false);
                    }
                    continue;
                }
                metadata = insertInbox(scopedInboxId, subMsgPacks, metadata, itr, reader, writer);
                if (inboxCommit != null) {
                    // handle insert and commit inbox
                    metadata = commitInbox(scopedInboxId, inboxCommit, metadata, itr, writer);
                    replyBuilder.putCommitResults(scopedInboxId.toStringUtf8(), true);
                }
                writer.put(scopedInboxId, metadata.toByteString());
                subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InboxInsertResult.Result.OK));
            } catch (Throwable e) {
                log.error("Error inserting messages to inbox {}", scopedInboxId, e);
                subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InboxInsertResult.Result.NO_INBOX));
            }
        }
        for (Map.Entry<SubInfo, InboxInsertResult.Result> entry : results.entrySet()) {
            replyBuilder.addInsertResults(InboxInsertResult.newBuilder()
                .setSubInfo(entry.getKey())
                .setResult(entry.getValue())
                .build());
        }
        // handle commit only inbox
        for (ByteString scopedInboxId : commitInboxs.keySet()) {
            InboxCommit inboxCommit = commitInboxs.get(scopedInboxId);
            assert inboxCommit.hasQos0UpToSeq() || inboxCommit.hasQos1UpToSeq() || inboxCommit.hasQos2UpToSeq();
            Optional<ByteString> metadataBytes = reader.get(scopedInboxId);
            if (metadataBytes.isEmpty()) {
                replyBuilder.putCommitResults(scopedInboxId.toStringUtf8(), false);
                continue;
            }
            try {
                InboxMetadata metadata = InboxMetadata.parseFrom(metadataBytes.get());
                if (hasExpired(metadata)) {
                    replyBuilder.putCommitResults(scopedInboxId.toStringUtf8(), false);
                    continue;
                }
                metadata = commitInbox(scopedInboxId, inboxCommit, metadata, itr, writer);
                writer.put(scopedInboxId, metadata.toByteString());
                replyBuilder.putCommitResults(scopedInboxId.toStringUtf8(), true);
            } catch (InvalidProtocolBufferException e) {
                replyBuilder.putCommitResults(scopedInboxId.toStringUtf8(), false);
            }
        }
        return replyBuilder.build();
    }

    private InboxInsertReply batchInsert(InboxInsertRequest request, IKVReader reader, IKVWriter writer) {
        IKVIterator itr = reader.iterator();
        Map<SubInfo, InboxInsertResult.Result> results = new LinkedHashMap<>();
        Map<ByteString, List<MessagePack>> subMsgPacksByInbox = new HashMap<>();
        for (MessagePack subMsgPack : request.getSubMsgPackList()) {
            SubInfo subInfo = subMsgPack.getSubInfo();
            ByteString scopedInboxId = KeyUtil.scopedInboxId(subInfo.getTenantId(), subInfo.getInboxId());
            results.put(subInfo, null);
            subMsgPacksByInbox.computeIfAbsent(scopedInboxId, k -> new LinkedList<>()).add(subMsgPack);
        }
        for (ByteString scopedInboxId : subMsgPacksByInbox.keySet()) {
            List<MessagePack> subMsgPacks = subMsgPacksByInbox.get(scopedInboxId);
            Optional<ByteString> metadataBytes = reader.get(scopedInboxId);
            if (metadataBytes.isEmpty()) {
                subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InboxInsertResult.Result.NO_INBOX));
                continue;
            }
            try {
                InboxMetadata metadata = InboxMetadata.parseFrom(metadataBytes.get());
                if (hasExpired(metadata)) {
                    subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InboxInsertResult.Result.NO_INBOX));
                    continue;
                }
                metadata = insertInbox(scopedInboxId, subMsgPacks, metadata, itr, reader, writer);
                writer.put(scopedInboxId, metadata.toByteString());
                subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InboxInsertResult.Result.OK));
            } catch (Throwable e) {
                log.error("Error inserting messages to inbox {}", scopedInboxId, e);
                subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InboxInsertResult.Result.NO_INBOX));
            }
        }

        InboxInsertReply.Builder replyBuilder = InboxInsertReply.newBuilder();
        for (Map.Entry<SubInfo, InboxInsertResult.Result> entry : results.entrySet()) {
            replyBuilder.addResults(InboxInsertResult.newBuilder()
                .setSubInfo(entry.getKey())
                .setResult(entry.getValue())
                .build());
        }
        return replyBuilder.build();
    }

    private InboxMetadata insertInbox(ByteString scopedInboxId,
                                      List<MessagePack> msgPacks,
                                      InboxMetadata metadata,
                                      IKVIterator itr,
                                      IKVReader reader,
                                      IKVWriter writer) {
        List<InboxMessage> qos0MsgList = new ArrayList<>();
        List<InboxMessage> qos1MsgList = new ArrayList<>();
        List<InboxMessage> qos2MsgList = new ArrayList<>();
        for (MessagePack inboxMsgPack : msgPacks) {
            SubInfo subInfo = inboxMsgPack.getSubInfo();
            for (TopicMessagePack topicMsgPack : inboxMsgPack.getMessagesList()) {
                String topic = topicMsgPack.getTopic();
                for (TopicMessagePack.PublisherPack publisherPack : topicMsgPack.getMessageList()) {
                    for (Message message : publisherPack.getMessageList()) {
                        InboxMessage inboxMsg =
                            InboxMessage.newBuilder()
                                .setTopicFilter(subInfo.getTopicFilter())
                                .setMsg(TopicMessage.newBuilder()
                                    .setTopic(topic)
                                    .setMessage(message)
                                    .setPublisher(publisherPack.getPublisher())
                                    .build())
                                .build();
                        QoS finalQoS =
                            QoS.forNumber(Math.min(message.getPubQoS().getNumber(), subInfo.getSubQoS().getNumber()));
                        assert finalQoS != null;
                        switch (finalQoS) {
                            case AT_MOST_ONCE -> qos0MsgList.add(inboxMsg);
                            case AT_LEAST_ONCE -> qos1MsgList.add(inboxMsg);
                            case EXACTLY_ONCE -> qos2MsgList.add(inboxMsg);
                        }
                    }
                }
            }
        }
        if (!qos0MsgList.isEmpty()) {
            metadata = insertQoS0Inbox(scopedInboxId, metadata, qos0MsgList, itr, writer);
        }
        if (!qos1MsgList.isEmpty()) {
            metadata = insertQoS1Inbox(scopedInboxId, metadata, qos1MsgList, itr, writer);
        }
        if (!qos2MsgList.isEmpty()) {
            metadata = insertQoS2Inbox(scopedInboxId, metadata, qos2MsgList, itr, reader, writer);
        }
        return metadata;
    }

    private InboxMetadata insertQoS0Inbox(ByteString scopedInboxId,
                                          InboxMetadata metadata,
                                          List<InboxMessage> messages,
                                          IKVIterator itr, IKVWriter writeClient) {
        itr.seek(qos0InboxMsgKey(scopedInboxId, 0));
        long nextSeq = metadata.getQos0NextSeq();
        long oldestSeq =
            itr.isValid() && isQoS0MessageKey(itr.key(), scopedInboxId) ? parseSeq(scopedInboxId, itr.key()) : nextSeq;
        int current = (int) (nextSeq - oldestSeq);
        int dropCount = current + messages.size() - metadata.getLimit();
        int actualDropped = 0;
        if (metadata.getDropOldest()) {
            if (messages.size() >= metadata.getLimit()) {
                if (current > 0) {
                    // drop all messages in the queue
                    writeClient.deleteRange(
                        Range.newBuilder()
                            .setStartKey(qos0InboxMsgKey(scopedInboxId, oldestSeq))
                            .setEndKey(qos0InboxMsgKey(scopedInboxId, nextSeq))
                            .build());
                    actualDropped += current;
                }
                // TODO: put a limit on value size?
                InboxMessageList messageList =
                    InboxMessageList.newBuilder()
                        .addAllMessage(
                            messages.size() > metadata.getLimit()
                                ? messages.subList(messages.size() - metadata.getLimit(), messages.size())
                                : messages)
                        .build();
                actualDropped += messages.size() - metadata.getLimit();
                writeClient.insert(qos0InboxMsgKey(scopedInboxId, nextSeq), messageList.toByteString());
                nextSeq += metadata.getLimit();
            } else {
                if (dropCount > 0) {
                    long delBeforeSeq = dropCount + oldestSeq;
                    // keep current key and move to next one
                    ByteString delKey = itr.key();
                    for (itr.next(); itr.isValid() && isQoS0MessageKey(itr.key(), scopedInboxId); itr.next()) {
                        long seq = parseSeq(scopedInboxId, itr.key());
                        if (seq >= delBeforeSeq) {
                            // all messages in delKey's value should be dropped
                            writeClient.delete(delKey);
                            actualDropped += seq - parseSeq(scopedInboxId, delKey);
                            delKey = itr.key();
                        } else {
                            break;
                        }
                    }
                }
                // TODO: put a limit on value size?
                writeClient.insert(
                    qos0InboxMsgKey(scopedInboxId, nextSeq),
                    InboxMessageList.newBuilder().addAllMessage(messages).build().toByteString());
                nextSeq += messages.size();
            }
        } else {
            if (dropCount < messages.size()) {
                // TODO: put a limit on value size?
                InboxMessageList messageList =
                    InboxMessageList.newBuilder()
                        .addAllMessage(dropCount > 0 ? messages.subList(0, messages.size() - dropCount) : messages)
                        .build();
                writeClient.insert(qos0InboxMsgKey(scopedInboxId, nextSeq), messageList.toByteString());
                if (dropCount > 0) {
                    actualDropped += dropCount;
                    nextSeq += messages.size() - dropCount;
                } else {
                    nextSeq += messages.size();
                }
            } else {
                actualDropped += dropCount;
            }
        }
        if (actualDropped > 0) {
            eventCollector.report(getLocal(Overflowed.class)
                .oldest(metadata.getDropOldest())
                .qos(QoS.AT_MOST_ONCE)
                .clientInfo(metadata.getClient())
                .dropCount(actualDropped));
        }
        return metadata.toBuilder().setQos0NextSeq(nextSeq).build();
    }

    private InboxMetadata insertQoS1Inbox(ByteString scopedInboxId,
                                          InboxMetadata metadata,
                                          List<InboxMessage> messages, IKVIterator itr,
                                          IKVWriter writeClient) {
        itr.seek(qos1InboxMsgKey(scopedInboxId, 0));
        long nextSeq = metadata.getQos1NextSeq();
        long oldestSeq =
            itr.isValid() && isQoS1MessageKey(itr.key(), scopedInboxId) ? parseSeq(scopedInboxId, itr.key()) : nextSeq;
        int current = (int) (nextSeq - oldestSeq);
        int dropCount = current + messages.size() - metadata.getLimit();
        int actualDropped = 0;
        if (metadata.getDropOldest()) {
            if (messages.size() >= metadata.getLimit()) {
                if (current > 0) {
                    // drop all messages in the queue
                    writeClient.deleteRange(
                        Range.newBuilder()
                            .setStartKey(qos1InboxMsgKey(scopedInboxId, oldestSeq))
                            .setEndKey(qos1InboxMsgKey(scopedInboxId, nextSeq))
                            .build());
                    actualDropped += current;
                }
                // TODO: put a limit on value size?
                InboxMessageList messageList =
                    InboxMessageList.newBuilder()
                        .addAllMessage(
                            messages.size() > metadata.getLimit()
                                ? messages.subList(messages.size() - metadata.getLimit(), messages.size())
                                : messages)
                        .build();
                actualDropped += messages.size() - metadata.getLimit();
                writeClient.insert(qos1InboxMsgKey(scopedInboxId, nextSeq), messageList.toByteString());
                nextSeq += metadata.getLimit();
            } else {
                if (dropCount > 0) {
                    long delBeforeSeq = dropCount + oldestSeq;
                    // keep current key and move to next one
                    ByteString delKey = itr.key();
                    for (itr.next(); itr.isValid() && isQoS1MessageKey(itr.key(), scopedInboxId); itr.next()) {
                        long seq = parseSeq(scopedInboxId, itr.key());
                        if (seq >= delBeforeSeq) {
                            // all messages in delKey's value should be dropped
                            writeClient.delete(delKey);
                            actualDropped += seq - parseSeq(scopedInboxId, delKey);
                            delKey = itr.key();
                        } else {
                            break;
                        }
                    }
                }
                // TODO: put a limit on value size?
                writeClient.insert(
                    qos1InboxMsgKey(scopedInboxId, nextSeq),
                    InboxMessageList.newBuilder().addAllMessage(messages).build().toByteString());
                nextSeq += messages.size();
            }
        } else {
            if (dropCount < messages.size()) {
                // TODO: put a limit on value size?
                InboxMessageList messageList =
                    InboxMessageList.newBuilder()
                        .addAllMessage(dropCount > 0 ? messages.subList(0, messages.size() - dropCount) : messages)
                        .build();
                writeClient.insert(qos1InboxMsgKey(scopedInboxId, nextSeq), messageList.toByteString());
                if (dropCount > 0) {
                    actualDropped += dropCount;
                    nextSeq += messages.size() - dropCount;
                } else {
                    nextSeq += messages.size();
                }
            } else {
                actualDropped += dropCount;
            }
        }
        if (actualDropped > 0) {
            eventCollector.report(getLocal(Overflowed.class)
                .oldest(metadata.getDropOldest())
                .qos(QoS.AT_LEAST_ONCE)
                .clientInfo(metadata.getClient())
                .dropCount(actualDropped));
        }
        return metadata.toBuilder().setQos1NextSeq(nextSeq).build();
    }

    private InboxMetadata insertQoS2Inbox(ByteString scopedInboxId,
                                          InboxMetadata metadata,
                                          List<InboxMessage> messages,
                                          IKVIterator itr,
                                          IKVReader reader,
                                          IKVWriter writeClient) {
        // filter out those already existed
        List<InboxMessage> uniqueInboxMsgList = new ArrayList<>(messages.size());
        Set<ByteString> msgKeySet = new HashSet<>();
        for (InboxMessage msg : messages) {
            ByteString msgKey = buildMsgKey(msg);
            if (!reader.exist(qos2InboxMsgKey(scopedInboxId, msgKey)) && !msgKeySet.contains(msgKey)) {
                uniqueInboxMsgList.add(msg);
                msgKeySet.add(msgKey);
            }
        }
        messages = uniqueInboxMsgList;
        itr.seek(qos2InboxIndex(scopedInboxId, 0));
        long nextSeq = metadata.getQos2NextSeq();
        long oldestSeq =
            itr.isValid() && isQoS2MessageIndexKey(itr.key(), scopedInboxId)
                ? parseQoS2Index(scopedInboxId, itr.key())
                : nextSeq;
        assert oldestSeq <= nextSeq;
        int messageCount = messages.size();
        int limit = metadata.getLimit();
        int current = (int) (nextSeq - oldestSeq);
        int total = current + messageCount;
        int dropCount = total - limit;
        if (total > limit) {
            if (metadata.getDropOldest()) {
                //             limit
                // [...........................]
                // [-------------------][++++++++++++]
                //  nextSeq - oldestSeq  messageCount   > limit
                if (total - limit < limit) {
                    int delCount = dropCount;
                    while (itr.isValid() && delCount > 0) {
                        writeClient.delete(itr.key());
                        writeClient.delete(qos2InboxMsgKey(scopedInboxId, itr.value()));
                        itr.next();
                        delCount--;
                    }
                    for (InboxMessage message : messages) {
                        ByteString msgKey = buildMsgKey(message);
                        writeClient.insert(qos2InboxIndex(scopedInboxId, nextSeq), msgKey);
                        writeClient.insert(qos2InboxMsgKey(scopedInboxId, msgKey), message.toByteString());
                        nextSeq++;
                    }
                } else {
                    //             limit
                    // [...........................]
                    // [-------------------][++++++++++++++++++++++++++++++++++++++++++]
                    //  nextSeq - oldestSeq  messageCount   > limit
                    int delCount = (int) (nextSeq - oldestSeq);
                    while (itr.isValid() && delCount > 0) {
                        writeClient.delete(itr.key());
                        writeClient.delete(qos2InboxMsgKey(scopedInboxId, itr.value()));
                        itr.next();
                        delCount--;
                    }
                    for (int i = messageCount - limit; i < messageCount; i++) {
                        InboxMessage message = messages.get(i);
                        ByteString msgKey = buildMsgKey(message);
                        writeClient.insert(qos2InboxIndex(scopedInboxId, nextSeq), msgKey);
                        writeClient.insert(qos2InboxMsgKey(scopedInboxId, msgKey), message.toByteString());
                        nextSeq++;
                    }
                }
            } else {
                //             limit
                // [...........................]
                // [-------------------][++++++++++++]
                //  nextSeq - oldestSeq  messageCount   > limit
                for (int i = 0; i < limit - current && i < messageCount; i++) {
                    InboxMessage message = messages.get(i);
                    ByteString msgKey = buildMsgKey(message);
                    writeClient.insert(qos2InboxIndex(scopedInboxId, nextSeq), msgKey);
                    writeClient.insert(qos2InboxMsgKey(scopedInboxId, msgKey), message.toByteString());
                    nextSeq++;
                    if (nextSeq - oldestSeq >= limit) {
                        break;
                    }
                }
            }
        } else {
            for (InboxMessage message : messages) {
                ByteString msgKey = buildMsgKey(message);
                writeClient.insert(qos2InboxIndex(scopedInboxId, nextSeq), msgKey);
                writeClient.insert(qos2InboxMsgKey(scopedInboxId, msgKey), message.toByteString());
                nextSeq++;
            }
        }
        if (dropCount > 0) {
            eventCollector.report(getLocal(Overflowed.class)
                .oldest(metadata.getDropOldest())
                .qos(QoS.EXACTLY_ONCE)
                .clientInfo(metadata.getClient())
                .dropCount(dropCount));
        }
        return metadata.toBuilder().setQos2NextSeq(nextSeq).build();
    }

    @SneakyThrows
    private TouchReply touch(TouchRequest request, IKVReader reader, IKVWriter writer) {
        for (String scopedInboxIdUtf8 : request.getScopedInboxIdMap().keySet()) {
            ByteString scopedInboxId = ByteString.copyFromUtf8(scopedInboxIdUtf8);
            Optional<ByteString> metadataBytes = reader.get(scopedInboxId);
            if (metadataBytes.isPresent()) {
                InboxMetadata metadata = InboxMetadata.parseFrom(metadataBytes.get());
                if (hasExpired(metadata) || !request.getScopedInboxIdMap().get(scopedInboxIdUtf8)) {
                    clearInbox(scopedInboxId, metadata, reader.iterator(), writer);
                    break;
                }
                metadata = metadata.toBuilder().setLastFetchTime(clock.millis()).build();
                writer.put(scopedInboxId, metadata.toByteString());
            }
        }
        return TouchReply.getDefaultInstance();
    }

    private InboxCommitReply batchCommit(InboxCommitRequest request, IKVReader reader, IKVWriter writer) {
        InboxCommitReply.Builder replyBuilder = InboxCommitReply.newBuilder();
        IKVIterator itr = reader.iterator();
        for (String scopedInboxIdUtf8 : request.getInboxCommitMap().keySet()) {
            ByteString scopedInboxId = ByteString.copyFromUtf8(scopedInboxIdUtf8);
            InboxCommit inboxCommit = request.getInboxCommitMap().get(scopedInboxIdUtf8);
            assert inboxCommit.hasQos0UpToSeq() || inboxCommit.hasQos1UpToSeq() || inboxCommit.hasQos2UpToSeq();
            Optional<ByteString> metadataBytes = reader.get(scopedInboxId);
            if (metadataBytes.isEmpty()) {
                replyBuilder.putResult(scopedInboxIdUtf8, false);
                continue;
            }
            try {
                InboxMetadata metadata = InboxMetadata.parseFrom(metadataBytes.get());
                if (hasExpired(metadata)) {
                    replyBuilder.putResult(scopedInboxIdUtf8, false);
                    continue;
                }
                metadata = commitInbox(scopedInboxId, inboxCommit, metadata, itr, writer);
                writer.put(scopedInboxId, metadata.toByteString());
                replyBuilder.putResult(scopedInboxIdUtf8, true);
            } catch (InvalidProtocolBufferException e) {
                replyBuilder.putResult(scopedInboxIdUtf8, false);
            }
        }
        return replyBuilder.build();
    }

    private InboxMetadata commitInbox(ByteString scopedInboxId,
                                      InboxCommit inboxCommit,
                                      InboxMetadata metadata,
                                      IKVIterator itr,
                                      IKVWriter writer) {
        if (inboxCommit.hasQos0UpToSeq()) {
            itr.seek(qos0InboxPrefix(scopedInboxId));
            if (itr.isValid() && isQoS0MessageKey(itr.key(), scopedInboxId)) {
                long oldestSeq = parseSeq(scopedInboxId, itr.key());
                if (oldestSeq <= inboxCommit.getQos0UpToSeq()) {
                    ByteString delKey = itr.key();
                    do {
                        itr.next();
                        if (!itr.isValid() || !isQoS0MessageKey(itr.key(), scopedInboxId)) {
                            if (inboxCommit.getQos0UpToSeq() == metadata.getQos0NextSeq() - 1) {
                                writer.delete(delKey);
                            }
                            break;
                        } else {
                            if (parseSeq(scopedInboxId, itr.key()) <= inboxCommit.getQos0UpToSeq() + 1) {
                                writer.delete(delKey);
                                delKey = itr.key();
                            }
                        }
                    } while (itr.isValid() && isQoS0MessageKey(itr.key(), scopedInboxId));
                }
            }
            metadata = metadata.toBuilder().setQos0LastFetchBeforeSeq(inboxCommit.getQos0UpToSeq() + 1).build();
        }
        if (inboxCommit.hasQos1UpToSeq()) {
            itr.seek(qos1InboxPrefix(scopedInboxId));
            if (itr.isValid() && isQoS1MessageKey(itr.key(), scopedInboxId)) {
                long oldestSeq = parseSeq(scopedInboxId, itr.key());
                if (oldestSeq <= inboxCommit.getQos1UpToSeq()) {
                    ByteString delKey = itr.key();
                    do {
                        itr.next();
                        if (!itr.isValid() || !isQoS1MessageKey(itr.key(), scopedInboxId)) {
                            if (inboxCommit.getQos1UpToSeq() == metadata.getQos1NextSeq() - 1) {
                                writer.delete(delKey);
                            }
                            break;
                        } else {
                            if (parseSeq(scopedInboxId, itr.key()) <= inboxCommit.getQos1UpToSeq() + 1) {
                                writer.delete(delKey);
                                delKey = itr.key();
                            }
                        }
                    } while (itr.isValid() && isQoS1MessageKey(itr.key(), scopedInboxId));
                }
            }
            metadata = metadata.toBuilder().setQos1LastCommitBeforeSeq(inboxCommit.getQos1UpToSeq() + 1).build();
        }
        if (inboxCommit.hasQos2UpToSeq()) {
            itr.seek(qos2InboxPrefix(scopedInboxId));
            while (itr.isValid() && isQoS2MessageIndexKey(itr.key(), scopedInboxId)
                && parseQoS2Index(scopedInboxId, itr.key()) <= inboxCommit.getQos2UpToSeq()) {
                writer.delete(itr.key());
                writer.delete(qos2InboxMsgKey(scopedInboxId, itr.value()));
                itr.next();
            }
            metadata = metadata.toBuilder().setQos2LastCommitBeforeSeq(inboxCommit.getQos2UpToSeq() + 1).build();
        }

        metadata = metadata.toBuilder().setLastFetchTime(clock.millis()).build();
        return metadata;
    }

    private boolean hasExpired(InboxMetadata metadata) {
        Duration now = Duration.ofMillis(clock.millis());
        Duration expireAt =
            Duration.ofMillis(metadata.getLastFetchTime()).plus(Duration.ofSeconds(metadata.getExpireSeconds()));
        return now.compareTo(expireAt) > 0;
    }

    private boolean isGCable(InboxMetadata metadata) {
        Duration now = Duration.ofMillis(clock.millis());
        Duration expireAt = Duration.ofMillis(metadata.getLastFetchTime())
            .plus(Duration.ofSeconds(metadata.getExpireSeconds()))
            .plus(purgeDelay);
        return now.compareTo(expireAt) > 0;
    }
}
