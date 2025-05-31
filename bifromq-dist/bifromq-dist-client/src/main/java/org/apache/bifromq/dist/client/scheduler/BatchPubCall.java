/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.dist.client.scheduler;

import org.apache.bifromq.base.util.AsyncRetry;
import org.apache.bifromq.base.util.exception.NeedRetryException;
import org.apache.bifromq.baserpc.client.IRPCClient;
import org.apache.bifromq.basescheduler.IBatchCall;
import org.apache.bifromq.basescheduler.ICallTask;
import org.apache.bifromq.dist.client.PubResult;
import org.apache.bifromq.dist.rpc.proto.DistReply;
import org.apache.bifromq.dist.rpc.proto.DistRequest;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.PublisherMessagePack;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

class BatchPubCall implements IBatchCall<PubRequest, PubResult, PubCallBatcherKey> {
    private final IRPCClient.IRequestPipeline<DistRequest, DistReply> ppln;
    private final Queue<ICallTask<PubRequest, PubResult, PubCallBatcherKey>> tasks = new ArrayDeque<>(64);
    private final long retryTimeoutNanos;
    private Map<ClientInfo, Map<String, PublisherMessagePack.TopicPack.Builder>> clientMsgPack = new HashMap<>(128);

    BatchPubCall(IRPCClient.IRequestPipeline<DistRequest, DistReply> ppln, long retryTimeoutNanos) {
        this.ppln = ppln;
        this.retryTimeoutNanos = retryTimeoutNanos;
    }

    @Override
    public void reset() {
        clientMsgPack = new HashMap<>(128);
        tasks.clear();
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
        DistRequest.Builder requestBuilder = DistRequest.newBuilder().setReqId(System.nanoTime());
        clientMsgPack.forEach((k, v) -> {
            PublisherMessagePack.Builder senderMsgPackBuilder = PublisherMessagePack.newBuilder().setPublisher(k);
            for (PublisherMessagePack.TopicPack.Builder packBuilder : v.values()) {
                senderMsgPackBuilder.addMessagePack(packBuilder);
            }
            requestBuilder.addMessages(senderMsgPackBuilder.build());
        });
        DistRequest request = requestBuilder.build();
        return AsyncRetry.exec(() -> execute(request), retryTimeoutNanos);
    }

    private CompletableFuture<Void> execute(DistRequest request) {
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        execute(request, onDone);
        return onDone;
    }

    private void execute(DistRequest request, CompletableFuture<Void> onDone) {
        ppln.invoke(request)
            .whenComplete((reply, e) -> {
                if (e != null) {
                    ICallTask<PubRequest, PubResult, PubCallBatcherKey> task;
                    while ((task = tasks.poll()) != null) {
                        task.resultPromise().complete(PubResult.ERROR);
                    }
                    onDone.complete(null);
                } else {
                    switch (reply.getCode()) {
                        case OK -> {
                            Map<ClientInfo, Map<String, Integer>> fanoutResultMap = new HashMap<>();
                            for (int i = 0; i < request.getMessagesCount(); i++) {
                                PublisherMessagePack pubMsgPack = request.getMessages(i);
                                DistReply.DistResult result = reply.getResults(i);
                                fanoutResultMap.put(pubMsgPack.getPublisher(), result.getTopicMap());
                            }

                            ICallTask<PubRequest, PubResult, PubCallBatcherKey> task;
                            while ((task = tasks.poll()) != null) {
                                Integer fanOut = fanoutResultMap.get(task.call().publisher).get(task.call().topic);
                                task.resultPromise().complete(fanOut > 0 ? PubResult.OK : PubResult.NO_MATCH);
                            }
                            onDone.complete(null);
                        }
                        case BACK_PRESSURE_REJECTED -> {
                            ICallTask<PubRequest, PubResult, PubCallBatcherKey> task;
                            while ((task = tasks.poll()) != null) {
                                task.resultPromise().complete(PubResult.BACK_PRESSURE_REJECTED);
                            }
                            onDone.complete(null);
                        }
                        case TRY_LATER -> onDone.completeExceptionally(new NeedRetryException("Retry later"));
                        default -> {
                            assert reply.getCode() == DistReply.Code.ERROR;
                            ICallTask<PubRequest, PubResult, PubCallBatcherKey> task;
                            while ((task = tasks.poll()) != null) {
                                task.resultPromise().complete(PubResult.ERROR);
                            }
                            onDone.complete(null);
                        }
                    }
                }
            });
    }
}
