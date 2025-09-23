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

package org.apache.bifromq.dist.server.scheduler;

import static java.util.Collections.emptyMap;
import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;

import com.google.common.collect.Iterables;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.proto.KVRangeROReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.baserpc.client.exception.ServerNotFoundException;
import org.apache.bifromq.basescheduler.IBatchCall;
import org.apache.bifromq.basescheduler.ICallTask;
import org.apache.bifromq.dist.rpc.proto.BatchDistReply;
import org.apache.bifromq.dist.rpc.proto.BatchDistRequest;
import org.apache.bifromq.dist.rpc.proto.DistPack;
import org.apache.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.PublisherMessagePack;
import org.apache.bifromq.type.TopicMessagePack;

@Slf4j
class BatchDistServerCall implements IBatchCall<TenantPubRequest, DistServerCallResult, DistServerCallBatcherKey> {
    private final IBaseKVStoreClient distWorkerClient;
    private final DistServerCallBatcherKey batcherKey;
    private final String orderKey;
    private final Queue<ICallTask<TenantPubRequest, DistServerCallResult, DistServerCallBatcherKey>> tasks =
        new ArrayDeque<>();
    private final Map<String, Map<ClientInfo, Iterable<Message>>> batch = new HashMap<>(128);
    private final TenantRangeLookupCache lookupCache;

    BatchDistServerCall(IBaseKVStoreClient distWorkerClient,
                        DistServerCallBatcherKey batcherKey,
                        TenantRangeLookupCache lookupCache) {
        this.distWorkerClient = distWorkerClient;
        this.batcherKey = batcherKey;
        this.orderKey = batcherKey.tenantId() + batcherKey.batcherId();
        this.lookupCache = lookupCache;
    }

    @Override
    public void add(ICallTask<TenantPubRequest, DistServerCallResult, DistServerCallBatcherKey> callTask) {
        tasks.add(callTask);
        callTask.call().publisherMessagePacks().forEach(publisherMsgPack -> publisherMsgPack.getMessagePackList()
            .forEach(topicMsgs -> batch.computeIfAbsent(topicMsgs.getTopic(), k -> new HashMap<>())
                .compute(publisherMsgPack.getPublisher(), (k, v) -> {
                    if (v == null) {
                        v = topicMsgs.getMessageList();
                    } else {
                        v = Iterables.concat(v, topicMsgs.getMessageList());
                    }
                    return v;
                })));
    }

    @Override
    public void reset() {
        batch.clear();
    }

    @Override
    public CompletableFuture<Void> execute() {
        Map<KVRangeSetting, Set<String>> topicsByRange = rangeLookup();
        if (topicsByRange.isEmpty()) {
            // no candidate range
            ICallTask<TenantPubRequest, DistServerCallResult, DistServerCallBatcherKey> task;
            while ((task = tasks.poll()) != null) {
                Map<String, Integer> fanOutResult = new HashMap<>();
                task.call().publisherMessagePacks().forEach(clientMessagePack -> clientMessagePack.getMessagePackList()
                    .forEach(topicMessagePack -> fanOutResult.put(topicMessagePack.getTopic(), 0)));
                task.resultPromise().complete(new DistServerCallResult(DistServerCallResult.Code.OK, fanOutResult));
            }
            return CompletableFuture.completedFuture(null);
        } else {
            return parallelDist(topicsByRange);
        }
    }

    private CompletableFuture<Void> parallelDist(Map<KVRangeSetting, Set<String>> topicsByRange) {
        long reqId = System.nanoTime();
        CompletableFuture<?>[] rangeQueryReplies = replicaSelect(topicsByRange).entrySet().stream().map(entry -> {
            KVRangeReplica rangeReplica = entry.getKey();
            Map<String, Map<ClientInfo, Iterable<Message>>> replicaBatch = entry.getValue();
            BatchDistRequest.Builder batchDistBuilder =
                BatchDistRequest.newBuilder().setReqId(reqId).setOrderKey(orderKey);
            replicaBatch.forEach((topic, publisherMsgs) -> {
                String tenantId = batcherKey.tenantId();
                DistPack.Builder distPackBuilder = DistPack.newBuilder().setTenantId(tenantId);
                TopicMessagePack.Builder topicMsgPackBuilder = TopicMessagePack.newBuilder().setTopic(topic);
                publisherMsgs.forEach((publisher, msgs) -> {
                    TopicMessagePack.PublisherPack.Builder packBuilder = TopicMessagePack.PublisherPack.newBuilder()
                        .setPublisher(publisher);
                    msgs.forEach(packBuilder::addMessage);
                    topicMsgPackBuilder.addMessage(packBuilder.build());
                });
                distPackBuilder.addMsgPack(topicMsgPackBuilder.build());
                batchDistBuilder.addDistPack(distPackBuilder.build());
            });
            return distWorkerClient.query(rangeReplica.storeId,
                    KVRangeRORequest.newBuilder().setReqId(reqId).setVer(rangeReplica.ver).setKvRangeId(rangeReplica.id)
                        .setRoCoProc(ROCoProcInput.newBuilder()
                            .setDistService(DistServiceROCoProcInput.newBuilder()
                                .setBatchDist(batchDistBuilder.build())
                                .build())
                            .build())
                        .build(), orderKey)
                .exceptionally(unwrap(e -> {
                    if (e instanceof ServerNotFoundException) {
                        // map server not found to try later
                        return KVRangeROReply.newBuilder().setReqId(reqId).setCode(ReplyCode.TryLater).build();
                    } else {
                        log.debug("Failed to query range: {}", rangeReplica, e);
                        // map rpc exception to internal error
                        return KVRangeROReply.newBuilder().setReqId(reqId).setCode(ReplyCode.InternalError).build();
                    }
                }));
        }).toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(rangeQueryReplies).thenAccept(replies -> {
            boolean needRetry = false;
            boolean hasError = false;
            // aggregate fan-out count from each reply
            Map<String, Integer> allFanOutByTopic = new HashMap<>();
            for (CompletableFuture<?> replyFuture : rangeQueryReplies) {
                KVRangeROReply rangeROReply = ((KVRangeROReply) replyFuture.join());
                switch (rangeROReply.getCode()) {
                    case Ok -> {
                        BatchDistReply reply = rangeROReply.getRoCoProcResult().getDistService().getBatchDist();
                        for (String tenantId : reply.getResultMap().keySet()) {
                            assert tenantId.equals(batcherKey.tenantId());
                            Map<String, Integer> topicFanOut = reply.getResultMap().get(tenantId).getFanoutMap();
                            topicFanOut.forEach((topic, fanOut) -> allFanOutByTopic.compute(topic, (k, val) -> {
                                if (val == null) {
                                    val = 0;
                                }
                                val += fanOut;
                                return val;
                            }));
                        }
                    }
                    case BadVersion, TryLater -> needRetry = true;
                    default -> hasError = true;
                }
            }
            ICallTask<TenantPubRequest, DistServerCallResult, DistServerCallBatcherKey> task;
            if (needRetry && !hasError) {
                while ((task = tasks.poll()) != null) {
                    task.resultPromise()
                        .complete(new DistServerCallResult(DistServerCallResult.Code.TryLater, emptyMap()));
                }
                return;
            }
            if (hasError) {
                while ((task = tasks.poll()) != null) {
                    task.resultPromise()
                        .complete(new DistServerCallResult(DistServerCallResult.Code.Error, emptyMap()));
                }
                return;
            }
            while ((task = tasks.poll()) != null) {
                Map<String, Integer> fanOutResult = new HashMap<>();
                for (PublisherMessagePack clientMsgPack : task.call().publisherMessagePacks()) {
                    for (PublisherMessagePack.TopicPack topicMsgPack : clientMsgPack.getMessagePackList()) {
                        int fanOut = allFanOutByTopic.getOrDefault(topicMsgPack.getTopic(), 0);
                        fanOutResult.put(topicMsgPack.getTopic(), fanOut);
                    }
                }
                task.resultPromise().complete(new DistServerCallResult(DistServerCallResult.Code.OK, fanOutResult));
            }
        });
    }

    private Map<KVRangeSetting, Set<String>> rangeLookup() {
        NavigableMap<Boundary, KVRangeSetting> effectiveRouter = distWorkerClient.latestEffectiveRouter();
        Map<KVRangeSetting, Set<String>> topicsByRange = new HashMap<>();
        for (String topic : batch.keySet()) {
            Collection<KVRangeSetting> candidates = lookupCache.lookup(topic, effectiveRouter);
            for (KVRangeSetting candidate : candidates) {
                topicsByRange.computeIfAbsent(candidate, k -> new HashSet<>()).add(topic);
            }
        }
        return topicsByRange;
    }

    private Map<KVRangeReplica, Map<String, Map<ClientInfo, Iterable<Message>>>> replicaSelect(
        Map<KVRangeSetting, Set<String>> topicsByRange) {
        Map<KVRangeReplica, Map<String, Map<ClientInfo, Iterable<Message>>>> batchByReplica = new HashMap<>();
        for (KVRangeSetting rangeSetting : topicsByRange.keySet()) {
            Map<String, Map<ClientInfo, Iterable<Message>>> rangeBatch = new HashMap<>();
            for (String topic : topicsByRange.get(rangeSetting)) {
                rangeBatch.put(topic, batch.get(topic));
            }
            if (rangeSetting.hasInProcReplica() || rangeSetting.allReplicas().size() == 1) {
                // build-in or single replica
                KVRangeReplica replica =
                    new KVRangeReplica(rangeSetting.id(), rangeSetting.ver(), rangeSetting.randomReplica());
                batchByReplica.put(replica, rangeBatch);
            } else {
                for (String topic : rangeBatch.keySet()) {
                    // bind replica based on tenantId, topic
                    int hash = Objects.hash(batcherKey.tenantId(), topic);
                    int replicaIdx = Math.abs(hash) % rangeSetting.allReplicas().size();
                    // replica bind
                    KVRangeReplica replica = new KVRangeReplica(rangeSetting.id(), rangeSetting.ver(),
                        rangeSetting.allReplicas().get(replicaIdx));
                    batchByReplica.computeIfAbsent(replica, k -> new HashMap<>()).put(topic, rangeBatch.get(topic));
                }
            }
        }
        return batchByReplica;
    }

    private record KVRangeReplica(KVRangeId id, long ver, String storeId) {
    }
}
