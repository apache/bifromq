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

package org.apache.bifromq.inbox.server.scheduler;

import static java.util.Collections.emptySet;

import org.apache.bifromq.basekv.client.IMutationPipeline;
import org.apache.bifromq.basekv.client.exception.BadVersionException;
import org.apache.bifromq.basekv.client.exception.TryLaterException;
import org.apache.bifromq.basekv.client.scheduler.BatchMutationCall;
import org.apache.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import org.apache.bifromq.baserpc.client.exception.ServerNotFoundException;
import org.apache.bifromq.basescheduler.ICallTask;
import org.apache.bifromq.inbox.rpc.proto.CommitReply;
import org.apache.bifromq.inbox.rpc.proto.CommitRequest;
import org.apache.bifromq.inbox.storage.proto.BatchCommitRequest;
import org.apache.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

class BatchCommitCall extends BatchMutationCall<CommitRequest, CommitReply> {
    protected BatchCommitCall(IMutationPipeline pipeline, MutationCallBatcherKey batcherKey) {
        super(pipeline, batcherKey);
    }

    @Override
    protected MutationCallTaskBatch<CommitRequest, CommitReply> newBatch(long ver) {
        return new BatchCommitCallTask(ver);
    }

    @Override
    protected RWCoProcInput makeBatch(
        Iterable<ICallTask<CommitRequest, CommitReply, MutationCallBatcherKey>> callTasks) {
        BatchCommitRequest.Builder reqBuilder = BatchCommitRequest.newBuilder();
        callTasks.forEach(call -> {
            CommitRequest req = call.call();
            BatchCommitRequest.Params.Builder paramsBuilder = BatchCommitRequest.Params.newBuilder()
                .setTenantId(req.getTenantId())
                .setInboxId(req.getInboxId())
                .setVersion(req.getVersion())
                .setNow(req.getNow());
            if (req.hasQos0UpToSeq()) {
                paramsBuilder.setQos0UpToSeq(req.getQos0UpToSeq());
            }
            if (req.hasSendBufferUpToSeq()) {
                paramsBuilder.setSendBufferUpToSeq(req.getSendBufferUpToSeq());
            }
            reqBuilder.addParams(paramsBuilder.build());
        });

        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchCommit(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<CommitRequest, CommitReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        assert batchedTasks.size() == output.getInboxService().getBatchCommit().getCodeCount();
        ICallTask<CommitRequest, CommitReply, MutationCallBatcherKey> task;
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            CommitReply.Builder replyBuilder = CommitReply.newBuilder().setReqId(task.call().getReqId());
            switch (output.getInboxService().getBatchCommit().getCode(i++)) {
                case OK -> task.resultPromise().complete(replyBuilder.setCode(CommitReply.Code.OK).build());
                case NO_INBOX -> task.resultPromise().complete(replyBuilder.setCode(CommitReply.Code.NO_INBOX).build());
                case CONFLICT -> task.resultPromise().complete(replyBuilder.setCode(CommitReply.Code.CONFLICT).build());
                default -> task.resultPromise().complete(replyBuilder.setCode(CommitReply.Code.ERROR).build());
            }
        }
    }

    @Override
    protected void handleException(ICallTask<CommitRequest, CommitReply, MutationCallBatcherKey> callTask,
                                   Throwable e) {
        if (e instanceof ServerNotFoundException) {
            callTask.resultPromise().complete(CommitReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(CommitReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof BadVersionException) {
            callTask.resultPromise().complete(CommitReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(CommitReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof TryLaterException) {
            callTask.resultPromise().complete(CommitReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(CommitReply.Code.TRY_LATER)
                .build());
            return;
        }
        callTask.resultPromise().completeExceptionally(e);
    }

    private static class BatchCommitCallTask extends MutationCallTaskBatch<CommitRequest, CommitReply> {
        private final Map<String, Set<InboxVersion>> inboxes = new HashMap<>();

        private BatchCommitCallTask(long ver) {
            super(ver);
        }

        @Override
        protected void add(ICallTask<CommitRequest, CommitReply, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.computeIfAbsent(callTask.call().getTenantId(), k -> new HashSet<>())
                .add(callTask.call().getVersion());
        }

        @Override
        protected boolean isBatchable(ICallTask<CommitRequest, CommitReply, MutationCallBatcherKey> callTask) {
            return !inboxes.getOrDefault(callTask.call().getTenantId(), emptySet())
                .contains(callTask.call().getVersion());
        }
    }
}
