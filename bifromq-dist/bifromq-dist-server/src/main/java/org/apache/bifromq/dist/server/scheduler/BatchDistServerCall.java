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

import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;

import com.google.common.collect.Iterables;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
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
import org.apache.bifromq.dist.rpc.proto.BatchDistRequest;
import org.apache.bifromq.dist.rpc.proto.DistPack;
import org.apache.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import org.apache.bifromq.dist.rpc.proto.TopicFanout;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.PublisherMessagePack;
import org.apache.bifromq.type.TopicMessagePack;

@Slf4j
class BatchDistServerCall implements IBatchCall<TenantPubRequest, Map<String, Integer>, DistServerCallBatcherKey> {
    private final IBaseKVStoreClient distWorkerClient;
    private final DistServerCallBatcherKey batcherKey;
    private final String orderKey;
    private final TenantRangeLookupCache lookupCache;
    private Queue<ICallTask<TenantPubRequest, Map<String, Integer>, DistServerCallBatcherKey>> tasks =
        new ArrayDeque<>();
    private Map<String, Map<ClientInfo, Iterable<Message>>> batch = new HashMap<>(128);

    BatchDistServerCall(IBaseKVStoreClient distWorkerClient,
                        DistServerCallBatcherKey batcherKey,
                        TenantRangeLookupCache lookupCache) {
        this.distWorkerClient = distWorkerClient;
        this.batcherKey = batcherKey;
        this.orderKey = batcherKey.tenantId() + batcherKey.batcherId();
        this.lookupCache = lookupCache;
    }

    @Override
    public void add(ICallTask<TenantPubRequest, Map<String, Integer>, DistServerCallBatcherKey> callTask) {
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
    public void reset(boolean abort) {
        if (abort) {
            tasks = new ArrayDeque<>();
            batch = new HashMap<>(128);
        } else {
            batch.clear();
        }
    }

    @Override
    public CompletableFuture<Void> execute() {
        return execute(tasks, batch);
    }

    private CompletableFuture<Void> execute(
        Queue<ICallTask<TenantPubRequest, Map<String, Integer>, DistServerCallBatcherKey>> tasks,
        Map<String, Map<ClientInfo, Iterable<Message>>> batch) {
        Map<KVRangeSetting, Set<String>> topicsByRange = rangeLookup(batch);
        if (topicsByRange.isEmpty()) {
            // no candidate range
            ICallTask<TenantPubRequest, Map<String, Integer>, DistServerCallBatcherKey> task;
            while ((task = tasks.poll()) != null) {
                Map<String, Integer> fanOutResult = new HashMap<>();
                task.call().publisherMessagePacks().forEach(clientMessagePack -> clientMessagePack.getMessagePackList()
                    .forEach(topicMessagePack -> fanOutResult.put(topicMessagePack.getTopic(), 0)));
                task.resultPromise().complete(fanOutResult);
            }
            return CompletableFuture.completedFuture(null);
        } else {
            return parallelDist(topicsByRange, tasks, batch);
        }
    }

    private CompletableFuture<Void> parallelDist(Map<KVRangeSetting, Set<String>> topicsByRange,
                                                 Queue<ICallTask<TenantPubRequest, Map<String, Integer>, DistServerCallBatcherKey>> tasks,
                                                 Map<String, Map<ClientInfo, Iterable<Message>>> batch) {
        long reqId = System.nanoTime();
        Collection<ReplicaBatch> replicaBatches = replicaSelect(topicsByRange, batch);
        Map<CompletableFuture<KVRangeROReply>, ReplicaBatch> rangeQueryReplies = replicaBatches.stream()
            .collect(Collectors.toMap(entry -> {
                KVRangeReplica rangeReplica = entry.replica;
                Map<String, Map<ClientInfo, Iterable<Message>>> replicaBatch = entry.msgBatch;
                BatchDistRequest.Builder batchDistBuilder = BatchDistRequest.newBuilder()
                    .setReqId(reqId)
                    .setOrderKey(orderKey);
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
                if (rangeReplica.storeId == null) {
                    // no available replica for the range
                    return CompletableFuture.completedFuture(
                        KVRangeROReply.newBuilder().setReqId(reqId).setCode(ReplyCode.TryLater).build());
                }
                return distWorkerClient.query(rangeReplica.storeId, KVRangeRORequest.newBuilder()
                        .setReqId(reqId)
                        .setVer(rangeReplica.ver)
                        .setKvRangeId(rangeReplica.id)
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
            }, replicaBatch -> replicaBatch));
        return CompletableFuture.allOf(rangeQueryReplies.keySet().toArray(new CompletableFuture[0]))
            .thenAccept(v -> {
                Map<String, Integer> allFanOutByTopic = new HashMap<>();
                for (CompletableFuture<KVRangeROReply> replyFuture : rangeQueryReplies.keySet()) {
                    ReplicaBatch replicaBatch = rangeQueryReplies.get(replyFuture);
                    KVRangeROReply reply = replyFuture.join();
                    switch (reply.getCode()) {
                        case Ok -> {
                            Map<String, TopicFanout> topicFanoutByTenant = reply
                                .getRoCoProcResult()
                                .getDistService()
                                .getBatchDist()
                                .getResultMap();
                            for (String tenantId : topicFanoutByTenant.keySet()) {
                                assert tenantId.equals(batcherKey.tenantId());
                                Map<String, Integer> topicFanOut = topicFanoutByTenant.get(tenantId).getFanoutMap();
                                topicFanOut.forEach((topic, fanOut) -> allFanOutByTopic.compute(topic, (k, val) -> {
                                    if (val == null) {
                                        val = 0;
                                    }
                                    if (val < 0) {
                                        // already marked as error or need retry
                                        return val;
                                    }
                                    val += fanOut;
                                    return val;
                                }));
                            }
                        }
                        case BadVersion, TryLater -> {
                            for (String topic : replicaBatch.msgBatch.keySet()) {
                                allFanOutByTopic.put(topic, -1); // need retry
                            }
                        }
                        default -> {
                            for (String topic : replicaBatch.msgBatch.keySet()) {
                                allFanOutByTopic.put(topic, -2); // error
                            }
                        }
                    }
                }
                ICallTask<TenantPubRequest, Map<String, Integer>, DistServerCallBatcherKey> task;
                while ((task = tasks.poll()) != null) {
                    Map<String, Integer> fanOutResult = new HashMap<>();
                    for (PublisherMessagePack clientMsgPack : task.call().publisherMessagePacks()) {
                        for (PublisherMessagePack.TopicPack topicMsgPack : clientMsgPack.getMessagePackList()) {
                            int fanOut = allFanOutByTopic.getOrDefault(topicMsgPack.getTopic(), 0);
                            fanOutResult.put(topicMsgPack.getTopic(), fanOut);
                        }
                    }
                    task.resultPromise().complete(fanOutResult);
                }
            });
    }

    private Map<KVRangeSetting, Set<String>> rangeLookup(Map<String, Map<ClientInfo, Iterable<Message>>> batch) {
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

    private Collection<ReplicaBatch> replicaSelect(Map<KVRangeSetting, Set<String>> topicsByRange,
                                                   Map<String, Map<ClientInfo, Iterable<Message>>> batch) {
        Map<KVRangeReplica, ReplicaBatch> batchByReplica = new LinkedHashMap<>(); // preserve insertion order
        for (KVRangeSetting rangeSetting : topicsByRange.keySet()) {
            Map<String, Map<ClientInfo, Iterable<Message>>> rangeBatch = new HashMap<>();
            for (String topic : topicsByRange.get(rangeSetting)) {
                rangeBatch.put(topic, batch.get(topic));
            }
            Optional<String> inProcReplica = rangeSetting.inProcQueryReadyReplica();
            if (inProcReplica.isPresent()) {
                // build-in or single replica
                KVRangeReplica replica = new KVRangeReplica(rangeSetting.id(), rangeSetting.ver(), inProcReplica.get());
                batchByReplica.put(replica, new ReplicaBatch(replica, rangeBatch));
            } else {
                for (String topic : rangeBatch.keySet()) {
                    // bind replica based on tenantId, topic
                    int replicaSeq = Math.abs(Objects.hash(batcherKey.tenantId(), topic));
                    // replica bind
                    Optional<String> replicaStore = rangeSetting.getQueryReadyReplica(replicaSeq);
                    KVRangeReplica replica = new KVRangeReplica(rangeSetting.id(), rangeSetting.ver(),
                        replicaStore.orElse(null));
                    batchByReplica.computeIfAbsent(replica, k -> new ReplicaBatch(replica, new HashMap<>()))
                        .msgBatch.put(topic, rangeBatch.get(topic));
                }
            }
        }
        return batchByReplica.values();
    }

    private record KVRangeReplica(KVRangeId id, long ver, String storeId) {
    }

    private record ReplicaBatch(KVRangeReplica replica, Map<String, Map<ClientInfo, Iterable<Message>>> msgBatch) {

    }
}
