/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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
import org.apache.bifromq.inbox.rpc.proto.SubReply;
import org.apache.bifromq.inbox.rpc.proto.SubRequest;
import org.apache.bifromq.inbox.storage.proto.BatchSubReply;
import org.apache.bifromq.inbox.storage.proto.BatchSubRequest;
import org.apache.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchSubCall extends BatchMutationCall<SubRequest, SubReply> {
    protected BatchSubCall(IMutationPipeline pipeline, MutationCallBatcherKey batcherKey) {
        super(pipeline, batcherKey);
    }

    @Override
    protected MutationCallTaskBatch<SubRequest, SubReply> newBatch(long ver) {
        return new BatchSubCallTask(ver);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterable<ICallTask<SubRequest, SubReply, MutationCallBatcherKey>> callTasks) {
        BatchSubRequest.Builder reqBuilder = BatchSubRequest.newBuilder();
        callTasks.forEach(call -> {
            SubRequest request = call.call();
            BatchSubRequest.Params.Builder paramsBuilder = BatchSubRequest.Params.newBuilder()
                .setTenantId(request.getTenantId())
                .setInboxId(request.getInboxId())
                .setVersion(request.getVersion())
                .setTopicFilter(request.getTopicFilter())
                .setMaxTopicFilters(request.getMaxTopicFilters())
                .setOption(request.getOption())
                .setNow(request.getNow());
            reqBuilder.addParams(paramsBuilder.build());
        });
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchSub(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<SubRequest, SubReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        assert batchedTasks.size() == output.getInboxService().getBatchSub().getCodeCount();
        ICallTask<SubRequest, SubReply, MutationCallBatcherKey> task;
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            SubReply.Builder replyBuilder = SubReply.newBuilder().setReqId(task.call().getReqId());
            BatchSubReply.Code code = output.getInboxService().getBatchSub().getCode(i++);
            switch (code) {
                case OK -> task.resultPromise().complete(replyBuilder.setCode(SubReply.Code.OK).build());
                case EXISTS -> task.resultPromise().complete(replyBuilder.setCode(SubReply.Code.EXISTS).build());
                case NO_INBOX -> task.resultPromise().complete(replyBuilder.setCode(SubReply.Code.NO_INBOX).build());
                case EXCEED_LIMIT ->
                    task.resultPromise().complete(replyBuilder.setCode(SubReply.Code.EXCEED_LIMIT).build());
                case CONFLICT -> task.resultPromise().complete(replyBuilder.setCode(SubReply.Code.CONFLICT).build());
                default -> {
                    log.error("Unknown error code: {}", code);
                    task.resultPromise().complete(replyBuilder.setCode(SubReply.Code.ERROR).build());
                }
            }
        }
    }

    @Override
    protected void handleException(ICallTask<SubRequest, SubReply, MutationCallBatcherKey> callTask, Throwable e) {
        if (e instanceof ServerNotFoundException) {
            callTask.resultPromise().complete(SubReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(SubReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof BadVersionException) {
            callTask.resultPromise().complete(SubReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(SubReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof TryLaterException) {
            callTask.resultPromise().complete(SubReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(SubReply.Code.TRY_LATER)
                .build());
            return;
        }
        callTask.resultPromise().completeExceptionally(e);
    }

    private static class BatchSubCallTask extends MutationCallTaskBatch<SubRequest, SubReply> {
        private final Map<String, Set<InboxVersion>> inboxes = new HashMap<>();

        private BatchSubCallTask(long ver) {
            super(ver);
        }

        @Override
        protected void add(ICallTask<SubRequest, SubReply, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.computeIfAbsent(callTask.call().getTenantId(), k -> new HashSet<>())
                .add(callTask.call().getVersion());
        }

        @Override
        protected boolean isBatchable(ICallTask<SubRequest, SubReply, MutationCallBatcherKey> callTask) {
            return !inboxes.getOrDefault(callTask.call().getTenantId(), emptySet())
                .contains(callTask.call().getVersion());
        }
    }
}
