/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.sessiondict.client.scheduler;

import org.apache.bifromq.baserpc.client.IRPCClient;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basescheduler.IBatchCall;
import org.apache.bifromq.basescheduler.ICallTask;
import org.apache.bifromq.sessiondict.client.type.OnlineCheckRequest;
import org.apache.bifromq.sessiondict.client.type.OnlineCheckResult;
import org.apache.bifromq.sessiondict.rpc.proto.ExistReply;
import org.apache.bifromq.sessiondict.rpc.proto.ExistRequest;

@Slf4j
class BatchSessionExistCall implements IBatchCall<OnlineCheckRequest, OnlineCheckResult, String> {
    private final IRPCClient.IRequestPipeline<ExistRequest, ExistReply> ppln;
    private final LinkedList<ICallTask<OnlineCheckRequest, OnlineCheckResult, String>> batchedTasks = new LinkedList<>();

    public BatchSessionExistCall(IRPCClient.IRequestPipeline<ExistRequest, ExistReply> ppln) {
        this.ppln = ppln;
    }

    @Override
    public void add(ICallTask<OnlineCheckRequest, OnlineCheckResult, String> task) {
        batchedTasks.add(task);
    }

    @Override
    public void reset() {

    }

    @Override
    public CompletableFuture<Void> execute() {
        ExistRequest.Builder reqBuilder = ExistRequest.newBuilder().setReqId(System.nanoTime());
        batchedTasks.forEach(task ->
            reqBuilder.addClient(ExistRequest.Client.newBuilder()
                .setUserId(task.call().userId())
                .setClientId(task.call().clientId())
                .build()));
        return ppln.invoke(reqBuilder.build())
            .handle((reply, e) -> {
                if (e != null) {
                    log.debug("Session exist call failed", e);
                    ICallTask<OnlineCheckRequest, OnlineCheckResult, String> task;
                    while ((task = batchedTasks.poll()) != null) {
                        task.resultPromise().complete(OnlineCheckResult.ERROR);
                    }
                } else {
                    switch (reply.getCode()) {
                        case OK -> {
                            ICallTask<OnlineCheckRequest, OnlineCheckResult, String> task;
                            assert reply.getExistCount() == batchedTasks.size();
                            int i = 0;
                            while ((task = batchedTasks.poll()) != null) {
                                task.resultPromise().complete(reply.getExist(i++)
                                    ? OnlineCheckResult.EXISTS : OnlineCheckResult.NOT_EXISTS);
                            }
                        }
                        default -> {
                            ICallTask<OnlineCheckRequest, OnlineCheckResult, String> task;
                            while ((task = batchedTasks.poll()) != null) {
                                task.resultPromise().complete(OnlineCheckResult.ERROR);
                            }
                        }
                    }
                }
                return null;
            });
    }
}
