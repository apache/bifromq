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

package org.apache.bifromq.dist.client.scheduler;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.baserpc.client.IRPCClient;
import org.apache.bifromq.basescheduler.IBatchCall;
import org.apache.bifromq.basescheduler.ICallTask;
import org.apache.bifromq.dist.client.PubResult;
import org.apache.bifromq.dist.rpc.proto.DistReply;
import org.apache.bifromq.dist.rpc.proto.DistRequest;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.PublisherMessagePack;

@Slf4j
class BatchPubCall implements IBatchCall<PubRequest, PubResult, PubCallBatcherKey> {
    private final IRPCClient.IRequestPipeline<DistRequest, DistReply> ppln;
    private Queue<ICallTask<PubRequest, PubResult, PubCallBatcherKey>> tasks = new ArrayDeque<>(64);
    private Map<ClientInfo, Map<String, PublisherMessagePack.TopicPack.Builder>> clientMsgPack = new HashMap<>(
        128);

    BatchPubCall(IRPCClient.IRequestPipeline<DistRequest, DistReply> ppln) {
        this.ppln = ppln;
    }

    @Override
    public void reset(boolean abort) {
        if (abort) {
            tasks = new ArrayDeque<>(64);
            clientMsgPack = new HashMap<>(128);
        }
    }

    @Override
    public void add(ICallTask<PubRequest, PubResult, PubCallBatcherKey> callTask) {
        tasks.add(callTask);
        clientMsgPack.computeIfAbsent(callTask.call().publisher, k -> new HashMap<>())
            .computeIfAbsent(callTask.call().topic,
                k -> PublisherMessagePack.TopicPack.newBuilder().setTopic(k))
            .addMessage(callTask.call().message);
    }

    @Override
    public CompletableFuture<Void> execute() {
        return execute(tasks, clientMsgPack);
    }

    private CompletableFuture<Void> execute(Queue<ICallTask<PubRequest, PubResult, PubCallBatcherKey>> tasks,
                                            Map<ClientInfo, Map<String, PublisherMessagePack.TopicPack.Builder>> clientMsgPack) {
        DistRequest.Builder requestBuilder = DistRequest.newBuilder().setReqId(System.nanoTime());
        Iterator<Map.Entry<ClientInfo, Map<String, PublisherMessagePack.TopicPack.Builder>>> itr =
            clientMsgPack.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<ClientInfo, Map<String, PublisherMessagePack.TopicPack.Builder>> entry = itr.next();
            ClientInfo publisher = entry.getKey();
            Map<String, PublisherMessagePack.TopicPack.Builder> topicPackMap = entry.getValue();
            PublisherMessagePack.Builder senderMsgPackBuilder = PublisherMessagePack.newBuilder()
                .setPublisher(publisher);
            for (PublisherMessagePack.TopicPack.Builder packBuilder : topicPackMap.values()) {
                senderMsgPackBuilder.addMessagePack(packBuilder);
            }
            requestBuilder.addMessages(senderMsgPackBuilder.build());
            itr.remove();
        }
        DistRequest request = requestBuilder.build();
        return ppln.invoke(request)
            .handle((reply, e) -> {
                if (e != null) {
                    ICallTask<PubRequest, PubResult, PubCallBatcherKey> task;
                    while ((task = tasks.poll()) != null) {
                        task.resultPromise().complete(PubResult.ERROR);
                    }
                } else {
                    switch (reply.getCode()) {
                        case OK -> {
                            try {
                                Map<ClientInfo, Map<String, Integer>> fanoutResultMap = new HashMap<>();
                                for (int i = 0; i < request.getMessagesCount(); i++) {
                                    PublisherMessagePack pubMsgPack = request.getMessages(i);
                                    DistReply.DistResult result = reply.getResults(i);
                                    fanoutResultMap.put(pubMsgPack.getPublisher(), result.getTopicMap());
                                }

                                ICallTask<PubRequest, PubResult, PubCallBatcherKey> task;
                                while ((task = tasks.poll()) != null) {
                                    Integer fanOut = fanoutResultMap.getOrDefault(task.call().publisher,
                                        Collections.emptyMap()).get(task.call().topic);
                                    if (fanOut == null) {
                                        // should not happen
                                        log.warn("Illegal state: no dist result for topic: {}", task.call().topic);
                                        fanOut = 0;
                                    }
                                    switch (fanOut) {
                                        case -2 -> task.resultPromise().complete(PubResult.ERROR);
                                        case -1 -> task.resultPromise().complete(PubResult.TRY_LATER);
                                        case 0 -> task.resultPromise().complete(PubResult.NO_MATCH);
                                        default -> task.resultPromise().complete(PubResult.OK);
                                    }
                                }
                            } catch (Throwable t) {
                                log.error("Unexpected exception", t);
                                ICallTask<PubRequest, PubResult, PubCallBatcherKey> task;
                                while ((task = tasks.poll()) != null) {
                                    task.resultPromise().complete(PubResult.ERROR);
                                }
                            }
                        }
                        case BACK_PRESSURE_REJECTED -> {
                            ICallTask<PubRequest, PubResult, PubCallBatcherKey> task;
                            while ((task = tasks.poll()) != null) {
                                task.resultPromise().complete(PubResult.BACK_PRESSURE_REJECTED);
                            }
                        }
                        default -> {
                            assert reply.getCode() == DistReply.Code.ERROR;
                            ICallTask<PubRequest, PubResult, PubCallBatcherKey> task;
                            while ((task = tasks.poll()) != null) {
                                task.resultPromise().complete(PubResult.ERROR);
                            }
                        }
                    }
                }
                return null;
            });
    }
}
