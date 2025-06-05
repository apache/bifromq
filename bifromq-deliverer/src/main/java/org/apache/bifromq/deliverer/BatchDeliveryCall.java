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

package org.apache.bifromq.deliverer;

import static org.apache.bifromq.deliverer.DeliveryCallResult.BACK_PRESSURE_REJECTED;
import static org.apache.bifromq.deliverer.DeliveryCallResult.ERROR;
import static org.apache.bifromq.deliverer.DeliveryCallResult.NO_RECEIVER;
import static org.apache.bifromq.deliverer.DeliveryCallResult.NO_SUB;
import static org.apache.bifromq.deliverer.DeliveryCallResult.OK;
import static org.apache.bifromq.plugin.subbroker.TypeUtil.toMap;

import org.apache.bifromq.basescheduler.IBatchCall;
import org.apache.bifromq.basescheduler.ICallTask;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.plugin.subbroker.DeliveryPack;
import org.apache.bifromq.plugin.subbroker.DeliveryPackage;
import org.apache.bifromq.plugin.subbroker.DeliveryReply;
import org.apache.bifromq.plugin.subbroker.DeliveryRequest;
import org.apache.bifromq.plugin.subbroker.DeliveryResult;
import org.apache.bifromq.plugin.subbroker.IDeliverer;
import org.apache.bifromq.type.MatchInfo;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchDeliveryCall implements IBatchCall<DeliveryCall, DeliveryCallResult, DelivererKey> {
    private final IDistClient distClient;
    private final IDeliverer deliverer;
    private final DelivererKey batcherKey;
    private final Queue<ICallTask<DeliveryCall, DeliveryCallResult, DelivererKey>> tasks = new ArrayDeque<>(128);
    private Map<String, Map<TopicMessagePackHolder, Set<MatchInfo>>> batch = new HashMap<>(128);

    BatchDeliveryCall(IDistClient distClient, IDeliverer deliverer, DelivererKey batcherKey) {
        this.distClient = distClient;
        this.deliverer = deliverer;
        this.batcherKey = batcherKey;
    }

    @Override
    public void reset() {
        batch = new HashMap<>(128);
        tasks.clear();
    }

    @Override
    public void add(ICallTask<DeliveryCall, DeliveryCallResult, DelivererKey> callTask) {
        batch.computeIfAbsent(callTask.call().tenantId, k -> new LinkedHashMap<>(128))
            .computeIfAbsent(callTask.call().messagePackHolder, k -> new HashSet<>())
            .add(callTask.call().matchInfo);
        tasks.add(callTask);
    }

    @Override
    public CompletableFuture<Void> execute() {
        DeliveryRequest.Builder requestBuilder = DeliveryRequest.newBuilder();
        batch.forEach((tenantId, pack) -> {
            DeliveryPackage.Builder packageBuilder = DeliveryPackage.newBuilder();
            pack.forEach((msgPackWrapper, matchInfos) -> {
                DeliveryPack.Builder packBuilder = DeliveryPack.newBuilder().setMessagePack(msgPackWrapper.messagePack);
                matchInfos.forEach(packBuilder::addMatchInfo);
                packageBuilder.addPack(packBuilder.build());
            });
            requestBuilder.putPackage(tenantId, packageBuilder.build());
        });
        DeliveryRequest request = requestBuilder.build();
        return execute(request);
    }

    private CompletableFuture<Void> execute(DeliveryRequest request) {
        return deliverer.deliver(request)
            .exceptionally(e -> {
                log.error("Unexpected exception", e);
                return DeliveryReply.newBuilder().setCode(DeliveryReply.Code.ERROR).build();
            })
            .thenAccept(reply -> {
                switch (reply.getCode()) {
                    case OK -> {
                        ICallTask<DeliveryCall, DeliveryCallResult, DelivererKey> task;
                        Map<String, Map<MatchInfo, DeliveryResult.Code>> resultMap = toMap(reply.getResultMap());
                        Map<String, Set<MatchInfo>> staleMatchInfos = new HashMap<>();
                        while ((task = tasks.poll()) != null) {
                            DeliveryResult.Code result =
                                resultMap.getOrDefault(task.call().tenantId, Collections.emptyMap())
                                    .get(task.call().matchInfo);
                            if (result != null) {
                                if (result == DeliveryResult.Code.NO_SUB || result == DeliveryResult.Code.NO_RECEIVER) {
                                    staleMatchInfos.computeIfAbsent(task.call().tenantId, k -> new HashSet<>())
                                        .add(task.call().matchInfo);
                                }
                                switch (result) {
                                    case OK -> task.resultPromise().complete(OK);
                                    case NO_SUB -> task.resultPromise().complete(NO_SUB);
                                    case NO_RECEIVER -> task.resultPromise().complete(NO_RECEIVER);
                                    default -> task.resultPromise().complete(ERROR);
                                }
                            } else {
                                log.warn("No deliver result: tenantId={}, route={}, batcherKey={}",
                                    task.call().tenantId, task.call().matchInfo, task.call().delivererKey);
                                task.resultPromise().complete(OK);
                            }
                        }
                        for (Map.Entry<String, Set<MatchInfo>> entry : staleMatchInfos.entrySet()) {
                            String tenantId = entry.getKey();
                            Set<MatchInfo> matchInfos = entry.getValue();
                            for (MatchInfo matchInfo : matchInfos) {
                                log.debug(
                                    "Stale match info: tenantId={}, topicFilter={}, receiverId={}, delivererKey={}, subBrokerId={}",
                                    tenantId, matchInfo.getMatcher().getMqttTopicFilter(), matchInfo.getReceiverId(),
                                    batcherKey.delivererKey(), batcherKey.subBrokerId());
                                distClient.removeRoute(System.nanoTime(), tenantId, matchInfo.getMatcher(),
                                    matchInfo.getReceiverId(), batcherKey.delivererKey(), batcherKey.subBrokerId(),
                                    matchInfo.getIncarnation());
                            }
                        }
                    }
                    case BACK_PRESSURE_REJECTED -> {
                        ICallTask<DeliveryCall, DeliveryCallResult, DelivererKey> task;
                        while ((task = tasks.poll()) != null) {
                            task.resultPromise().complete(BACK_PRESSURE_REJECTED);
                        }
                    }
                    default -> {
                        assert reply.getCode() == DeliveryReply.Code.ERROR;
                        ICallTask<DeliveryCall, DeliveryCallResult, DelivererKey> task;
                        while ((task = tasks.poll()) != null) {
                            task.resultPromise().complete(ERROR);
                        }
                    }
                }
            });
    }
}

