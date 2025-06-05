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
import org.apache.bifromq.inbox.rpc.proto.DeleteReply;
import org.apache.bifromq.inbox.rpc.proto.DeleteRequest;
import org.apache.bifromq.inbox.storage.proto.BatchDeleteReply;
import org.apache.bifromq.inbox.storage.proto.BatchDeleteRequest;
import org.apache.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchDeleteCall extends BatchMutationCall<DeleteRequest, DeleteReply> {

    protected BatchDeleteCall(IMutationPipeline pipeline, MutationCallBatcherKey batcherKey) {
        super(pipeline, batcherKey);
    }

    @Override
    protected MutationCallTaskBatch<DeleteRequest, DeleteReply> newBatch(long ver) {
        return new BatchDeleteCallTask(ver);
    }

    @Override
    protected RWCoProcInput makeBatch(
        Iterable<ICallTask<DeleteRequest, DeleteReply, MutationCallBatcherKey>> callTasks) {
        BatchDeleteRequest.Builder reqBuilder = BatchDeleteRequest.newBuilder();
        callTasks.forEach(call -> {
            DeleteRequest request = call.call();
            BatchDeleteRequest.Params.Builder paramsBuilder = BatchDeleteRequest.Params.newBuilder()
                .setTenantId(request.getTenantId())
                .setInboxId(request.getInboxId())
                .setVersion(request.getVersion());
            reqBuilder.addParams(paramsBuilder.build());
        });
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchDelete(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<DeleteRequest, DeleteReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        ICallTask<DeleteRequest, DeleteReply, MutationCallBatcherKey> callTask;
        assert batchedTasks.size() == output.getInboxService().getBatchDelete().getResultCount();

        int i = 0;
        while ((callTask = batchedTasks.poll()) != null) {
            BatchDeleteReply.Result result = output.getInboxService().getBatchDelete().getResult(i++);
            DeleteReply.Builder replyBuilder = DeleteReply.newBuilder().setReqId(callTask.call().getReqId());
            switch (result.getCode()) {
                case OK -> callTask.resultPromise().complete(replyBuilder
                    .setCode(DeleteReply.Code.OK)
                    .putAllTopicFilters(result.getTopicFiltersMap())
                    .build());
                case NO_INBOX -> callTask.resultPromise().complete(replyBuilder
                    .setCode(DeleteReply.Code.NO_INBOX)
                    .build());
                case CONFLICT -> callTask.resultPromise().complete(replyBuilder
                    .setCode(DeleteReply.Code.CONFLICT)
                    .build());
                default -> {
                    log.error("Unexpected delete result: {}", result.getCode());
                    callTask.resultPromise().complete(replyBuilder
                        .setCode(DeleteReply.Code.ERROR)
                        .build());
                }
            }
        }
    }

    @Override
    protected void handleException(ICallTask<DeleteRequest, DeleteReply, MutationCallBatcherKey> callTask,
                                   Throwable e) {
        if (e instanceof ServerNotFoundException) {
            callTask.resultPromise().complete(DeleteReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(DeleteReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof BadVersionException) {
            callTask.resultPromise().complete(DeleteReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(DeleteReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof TryLaterException) {
            callTask.resultPromise().complete(DeleteReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(DeleteReply.Code.TRY_LATER)
                .build());
            return;
        }
        callTask.resultPromise().completeExceptionally(e);
    }

    private static class BatchDeleteCallTask extends MutationCallTaskBatch<DeleteRequest, DeleteReply> {
        private final Map<String, Set<InboxVersion>> inboxes = new HashMap<>();

        private BatchDeleteCallTask(long ver) {
            super(ver);
        }

        @Override
        protected void add(
            ICallTask<DeleteRequest, DeleteReply, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.computeIfAbsent(callTask.call().getTenantId(), k -> new HashSet<>())
                .add(callTask.call().getVersion());
        }

        @Override
        protected boolean isBatchable(
            ICallTask<DeleteRequest, DeleteReply, MutationCallBatcherKey> callTask) {
            return !inboxes.getOrDefault(callTask.call().getTenantId(), emptySet())
                .contains(callTask.call().getVersion());
        }
    }
}
