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
import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.tenantBeginKey;
import static org.apache.bifromq.util.TopicConst.NUL;
import static org.apache.bifromq.util.TopicUtil.fastJoin;
import static java.util.Collections.emptyMap;

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
import org.apache.bifromq.dist.rpc.proto.Fact;
import org.apache.bifromq.dist.trie.TopicFilterIterator;
import org.apache.bifromq.dist.trie.TopicTrieNode;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.PublisherMessagePack;
import org.apache.bifromq.type.TopicMessagePack;
import org.apache.bifromq.util.TopicUtil;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchDistServerCall implements IBatchCall<TenantPubRequest, DistServerCallResult, DistServerCallBatcherKey> {
    private final IBaseKVStoreClient distWorkerClient;
    private final DistServerCallBatcherKey batcherKey;
    private final String orderKey;
    private final Queue<ICallTask<TenantPubRequest, DistServerCallResult, DistServerCallBatcherKey>> tasks =
        new ArrayDeque<>();
    private Map<String, Map<ClientInfo, Iterable<Message>>> batch = new HashMap<>(128);

    BatchDistServerCall(IBaseKVStoreClient distWorkerClient, DistServerCallBatcherKey batcherKey) {
        this.distWorkerClient = distWorkerClient;
        this.batcherKey = batcherKey;
        this.orderKey = batcherKey.tenantId() + batcherKey.batcherId();
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
        batch = new HashMap<>(128);
    }

    @Override
    public CompletableFuture<Void> execute() {
        Collection<KVRangeSetting> candidates = rangeLookup();
        if (candidates.isEmpty()) {
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
            return parallelDist(candidates);
        }
    }

    private CompletableFuture<Void> parallelDist(Collection<KVRangeSetting> candidates) {
        long reqId = System.nanoTime();
        CompletableFuture<?>[] rangeQueryReplies = replicaSelect(candidates).entrySet().stream().map(entry -> {
            KVRangeReplica rangeReplica = entry.getKey();
            Map<String, Map<ClientInfo, Iterable<Message>>> replicaBatch = entry.getValue();
            BatchDistRequest.Builder batchDistBuilder =
                BatchDistRequest.newBuilder().setReqId(reqId).setOrderKey(orderKey);
            replicaBatch.forEach((topic, publisherMsgs) -> {
                String tenantId = batcherKey.tenantId();
                DistPack.Builder distPackBuilder = DistPack.newBuilder().setTenantId(tenantId);
                TopicMessagePack.Builder topicMsgPackBuilder = TopicMessagePack.newBuilder().setTopic(topic);
                publisherMsgs.forEach((publisher, msgs) -> {
                    TopicMessagePack.PublisherPack.Builder packBuilder = TopicMessagePack.PublisherPack.newBuilder();
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
                        Integer fanOut = allFanOutByTopic.get(topicMsgPack.getTopic());
                        if (fanOut == null) {
                            log.error("Illegal state: no result for topic: {}", topicMsgPack.getTopic());
                        }
                        fanOutResult.put(topicMsgPack.getTopic(), fanOut);
                    }
                }
                task.resultPromise().complete(new DistServerCallResult(DistServerCallResult.Code.OK, fanOutResult));
            }
        });
    }

    private Collection<KVRangeSetting> rangeLookup() {
        NavigableMap<Boundary, KVRangeSetting> effectiveRouter = distWorkerClient.latestEffectiveRouter();
        if (hasSingleItem(effectiveRouter.keySet())) {
            return effectiveRouter.values();
        }
        ByteString tenantStartKey = tenantBeginKey(batcherKey.tenantId());
        Boundary tenantBoundary = toBoundary(tenantStartKey, upperBound(tenantStartKey));
        Collection<KVRangeSetting> allCandidates = findByBoundary(tenantBoundary, effectiveRouter);
        if (hasSingleItem(allCandidates)) {
            return allCandidates;
        }
        TopicTrieNode.Builder<String> topicTrieBuilder = TopicTrieNode.builder(true);
        batch.keySet()
            .forEach(topic -> topicTrieBuilder.addTopic(TopicUtil.parse(batcherKey.tenantId(), topic, false), topic));

        TopicFilterIterator<String> topicFilterIterator = new TopicFilterIterator<>(topicTrieBuilder.build());
        List<KVRangeSetting> finalCandidates = new LinkedList<>();
        for (KVRangeSetting candidate : allCandidates) {
            Optional<Fact> factOpt = candidate.getFact(Fact.class);
            if (factOpt.isEmpty()) {
                finalCandidates.add(candidate);
                continue;
            }
            Fact fact = factOpt.get();
            if (!fact.hasFirstGlobalFilterLevels() || !fact.hasLastGlobalFilterLevels()) {
                // range is empty
                continue;
            }
            List<String> firstFilterLevels = fact.getFirstGlobalFilterLevels().getFilterLevelList();
            List<String> lastFilterLevels = fact.getLastGlobalFilterLevels().getFilterLevelList();
            topicFilterIterator.seek(firstFilterLevels);
            if (topicFilterIterator.isValid()) {
                // firstTopicFilter <= nextTopicFilter
                if (topicFilterIterator.key().equals(firstFilterLevels) ||
                    fastJoin(NUL, topicFilterIterator.key()).compareTo(fastJoin(NUL, lastFilterLevels)) <= 0) {
                    // if firstTopicFilter == nextTopicFilter || nextFilterLevels <= lastFilterLevels
                    // add to finalCandidates
                    finalCandidates.add(candidate);
                }
            } else {
                // endTopicFilter < firstTopicFilter, stop
                break;
            }
        }
        return finalCandidates;
    }

    private <E> boolean hasSingleItem(Collection<E> collection) {
        Iterator<E> iterator = collection.iterator();
        if (iterator.hasNext()) {
            iterator.next();
            return !iterator.hasNext();
        }
        return false;
    }

    private Map<KVRangeReplica, Map<String, Map<ClientInfo, Iterable<Message>>>> replicaSelect(
        Collection<KVRangeSetting> candidates) {
        Map<KVRangeReplica, Map<String, Map<ClientInfo, Iterable<Message>>>> batchByReplica = new HashMap<>();
        for (KVRangeSetting rangeSetting : candidates) {
            if (rangeSetting.hasInProcReplica() || rangeSetting.allReplicas.size() == 1) {
                // build-in or single replica
                KVRangeReplica replica =
                    new KVRangeReplica(rangeSetting.id, rangeSetting.ver, rangeSetting.randomReplica());
                batchByReplica.put(replica, batch);
            } else {
                for (String topic : batch.keySet()) {
                    // bind replica based on tenantId, topic
                    int hash = Objects.hash(batcherKey.tenantId(), topic);
                    int replicaIdx = Math.abs(hash) % rangeSetting.allReplicas.size();
                    // replica bind
                    KVRangeReplica replica =
                        new KVRangeReplica(rangeSetting.id, rangeSetting.ver, rangeSetting.allReplicas.get(replicaIdx));
                    batchByReplica.computeIfAbsent(replica, k -> new HashMap<>()).put(topic, batch.get(topic));
                }
            }
        }
        return batchByReplica;
    }

    private record KVRangeReplica(KVRangeId id, long ver, String storeId) {
    }
}
