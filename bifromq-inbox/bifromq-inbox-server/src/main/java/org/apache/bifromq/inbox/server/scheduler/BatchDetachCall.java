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
import org.apache.bifromq.inbox.rpc.proto.DetachReply;
import org.apache.bifromq.inbox.rpc.proto.DetachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchDetachReply;
import org.apache.bifromq.inbox.storage.proto.BatchDetachRequest;
import org.apache.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.Replica;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchDetachCall extends BatchMutationCall<DetachRequest, DetachReply> {

    protected BatchDetachCall(IMutationPipeline pipeline, MutationCallBatcherKey batcherKey) {
        super(pipeline, batcherKey);
    }

    @Override
    protected MutationCallTaskBatch<DetachRequest, DetachReply> newBatch(long ver) {
        return new BatchDetachCallTask(ver);
    }

    @Override
    protected RWCoProcInput makeBatch(
        Iterable<ICallTask<DetachRequest, DetachReply, MutationCallBatcherKey>> callTasks) {
        BatchDetachRequest.Builder reqBuilder = BatchDetachRequest.newBuilder()
            .setLeader(Replica.newBuilder()
                .setRangeId(batcherKey.id)
                .setStoreId(batcherKey.leaderStoreId)
                .build());
        for (ICallTask<DetachRequest, DetachReply, MutationCallBatcherKey> callTask : callTasks) {
            DetachRequest request = callTask.call();
            BatchDetachRequest.Params.Builder paramsBuilder = BatchDetachRequest.Params.newBuilder()
                .setTenantId(request.getClient().getTenantId())
                .setInboxId(request.getInboxId())
                .setVersion(request.getVersion())
                .setExpirySeconds(request.getExpirySeconds())
                .setDiscardLWT(request.getDiscardLWT())
                .setNow(request.getNow());
            reqBuilder.addParams(paramsBuilder.build());
        }

        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchDetach(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<DetachRequest, DetachReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        ICallTask<DetachRequest, DetachReply, MutationCallBatcherKey> callTask;
        assert batchedTasks.size() == output.getInboxService().getBatchDetach().getCodeCount();

        int i = 0;
        while ((callTask = batchedTasks.poll()) != null) {
            BatchDetachReply.Code code = output.getInboxService().getBatchDetach().getCode(i++);
            DetachReply.Builder replyBuilder = DetachReply.newBuilder().setReqId(callTask.call().getReqId());
            switch (code) {
                case OK -> replyBuilder.setCode(DetachReply.Code.OK);
                case NO_INBOX -> replyBuilder.setCode(DetachReply.Code.NO_INBOX);
                case CONFLICT -> replyBuilder.setCode(DetachReply.Code.CONFLICT);
                default -> {
                    log.error("Unexpected detach result: {}", code);
                    replyBuilder.setCode(DetachReply.Code.ERROR);
                }
            }
            callTask.resultPromise().complete(replyBuilder.build());
        }
    }

    @Override
    protected void handleException(ICallTask<DetachRequest, DetachReply, MutationCallBatcherKey> callTask,
                                   Throwable e) {
        if (e instanceof ServerNotFoundException) {
            callTask.resultPromise()
                .complete(DetachReply.newBuilder()
                    .setCode(DetachReply.Code.TRY_LATER)
                    .build());
            return;
        }
        if (e instanceof BadVersionException) {
            callTask.resultPromise()
                .complete(DetachReply.newBuilder()
                    .setCode(DetachReply.Code.TRY_LATER)
                    .build());
            return;
        }
        if (e instanceof TryLaterException) {
            callTask.resultPromise()
                .complete(DetachReply.newBuilder()
                    .setCode(DetachReply.Code.TRY_LATER)
                    .build());
            return;
        }
        callTask.resultPromise().completeExceptionally(e);
    }

    private static class BatchDetachCallTask extends MutationCallTaskBatch<DetachRequest, DetachReply> {
        private final Map<String, Set<InboxVersion>> inboxes = new HashMap<>();

        private BatchDetachCallTask(long ver) {
            super(ver);
        }

        @Override
        protected void add(ICallTask<DetachRequest, DetachReply, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.computeIfAbsent(callTask.call().getClient().getTenantId(), k -> new HashSet<>())
                .add(callTask.call().getVersion());
        }

        @Override
        protected boolean isBatchable(ICallTask<DetachRequest, DetachReply, MutationCallBatcherKey> callTask) {
            return !inboxes.getOrDefault(callTask.call().getClient().getTenantId(), emptySet())
                .contains(callTask.call().getVersion());
        }
    }
}
