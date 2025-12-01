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

package org.apache.bifromq.basekv.client.scheduler;

import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.client.IMutationPipeline;
import org.apache.bifromq.basekv.client.exception.BadRequestException;
import org.apache.bifromq.basekv.client.exception.BadVersionException;
import org.apache.bifromq.basekv.client.exception.InternalErrorException;
import org.apache.bifromq.basekv.client.exception.TryLaterException;
import org.apache.bifromq.basekv.store.proto.KVRangeRWRequest;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import org.apache.bifromq.basescheduler.IBatchCall;
import org.apache.bifromq.basescheduler.ICallTask;

@Slf4j
public abstract class BatchMutationCall<ReqT, RespT> implements IBatchCall<ReqT, RespT, MutationCallBatcherKey> {
    protected final MutationCallBatcherKey batcherKey;
    private final IMutationPipeline storePipeline;
    private Deque<ICallTask<ReqT, RespT, MutationCallBatcherKey>> pendingCallTasks = new ArrayDeque<>();

    protected BatchMutationCall(IMutationPipeline storePipeline, MutationCallBatcherKey batcherKey) {
        this.batcherKey = batcherKey;
        this.storePipeline = storePipeline;
    }

    @Override
    public final void add(ICallTask<ReqT, RespT, MutationCallBatcherKey> callTask) {
        pendingCallTasks.add(callTask);
    }

    protected MutationCallTaskBatch<ReqT, RespT> newBatch(long ver) {
        return new MutationCallTaskBatch<>(ver);
    }

    protected abstract RWCoProcInput makeBatch(Iterable<ICallTask<ReqT, RespT, MutationCallBatcherKey>> batchedTasks);

    protected abstract void handleOutput(Queue<ICallTask<ReqT, RespT, MutationCallBatcherKey>> batchedTasks,
                                         RWCoProcOutput output);

    protected abstract void handleException(ICallTask<ReqT, RespT, MutationCallBatcherKey> callTask, Throwable e);

    @Override
    public void reset(boolean abort) {
        if (abort) {
            pendingCallTasks = new ArrayDeque<>();
        }
    }

    @Override
    public CompletableFuture<Void> execute() {
        return executeBatches();
    }

    private CompletableFuture<Void> executeBatches() {
        CompletableFuture<Void> chained = CompletableFuture.completedFuture(null);
        MutationCallTaskBatch<ReqT, RespT> batchCallTask;
        while ((batchCallTask = buildNextBatch()) != null) {
            MutationCallTaskBatch<ReqT, RespT> current = batchCallTask;
            chained = chained.thenCompose(v -> fireSingleBatch(current));
        }
        return chained;
    }

    private MutationCallTaskBatch<ReqT, RespT> buildNextBatch() {
        if (pendingCallTasks.isEmpty()) {
            return null;
        }
        MutationCallTaskBatch<ReqT, RespT> batchCallTask = null;
        long batchVer = -1;
        int size = pendingCallTasks.size();
        for (int i = 0; i < size; i++) {
            ICallTask<ReqT, RespT, MutationCallBatcherKey> task = pendingCallTasks.pollFirst();
            if (task == null) {
                break;
            }
            if (batchCallTask == null) {
                batchVer = task.batcherKey().ver;
                batchCallTask = newBatch(batchVer);
                batchCallTask.add(task);
                continue;
            }
            if (task.batcherKey().ver != batchVer) {
                pendingCallTasks.addLast(task);
                continue;
            }
            if (batchCallTask.isBatchable(task)) {
                batchCallTask.add(task);
            } else {
                pendingCallTasks.addLast(task);
            }
        }
        return batchCallTask;
    }

    private CompletableFuture<Void> fireSingleBatch(MutationCallTaskBatch<ReqT, RespT> batchCallTask) {
        RWCoProcInput input = makeBatch(batchCallTask.batchedTasks);
        long reqId = System.nanoTime();
        return storePipeline
            .execute(KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(batchCallTask.ver)
                .setKvRangeId(batcherKey.id)
                .setRwCoProc(input)
                .build())
            .thenApply(reply -> {
                switch (reply.getCode()) {
                    case Ok -> {
                        return reply.getRwCoProcResult();
                    }
                    case TryLater -> throw new TryLaterException();
                    case BadVersion -> throw new BadVersionException();
                    case BadRequest -> throw new BadRequestException();
                    default -> throw new InternalErrorException();
                }
            })
            .handle(unwrap((v, e) -> {
                if (e != null) {
                    ICallTask<ReqT, RespT, MutationCallBatcherKey> callTask;
                    while ((callTask = batchCallTask.batchedTasks.poll()) != null) {
                        handleException(callTask, e);
                    }
                } else {
                    handleOutput(batchCallTask.batchedTasks, v);
                }
                return null;
            }));
    }

    protected static class MutationCallTaskBatch<CallT, CallResultT> {
        private final long ver;
        private final LinkedList<ICallTask<CallT, CallResultT, MutationCallBatcherKey>> batchedTasks =
            new LinkedList<>();

        protected MutationCallTaskBatch(long ver) {
            this.ver = ver;
        }

        protected void add(ICallTask<CallT, CallResultT, MutationCallBatcherKey> callTask) {
            this.batchedTasks.add(callTask);
        }

        protected boolean isBatchable(ICallTask<CallT, CallResultT, MutationCallBatcherKey> callTask) {
            return true;
        }
    }
}
