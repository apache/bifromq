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

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.client.IMutationPipeline;
import org.apache.bifromq.basekv.client.exception.BadVersionException;
import org.apache.bifromq.basekv.client.exception.TryLaterException;
import org.apache.bifromq.basekv.client.scheduler.BatchMutationCall;
import org.apache.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import org.apache.bifromq.baserpc.client.exception.ServerNotFoundException;
import org.apache.bifromq.basescheduler.ICallTask;
import org.apache.bifromq.inbox.rpc.proto.AttachReply;
import org.apache.bifromq.inbox.rpc.proto.AttachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchAttachRequest;
import org.apache.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import org.apache.bifromq.inbox.storage.proto.Replica;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchAttachCall extends BatchMutationCall<AttachRequest, AttachReply> {

    protected BatchAttachCall(IMutationPipeline pipeline, MutationCallBatcherKey batcherKey) {
        super(pipeline, batcherKey);
    }

    @Override
    protected MutationCallTaskBatch<AttachRequest, AttachReply> newBatch(long ver) {
        return new BatchAttachCallTask(ver);
    }


    @Override
    protected RWCoProcInput makeBatch(
        Iterable<ICallTask<AttachRequest, AttachReply, MutationCallBatcherKey>> callTasks) {
        BatchAttachRequest.Builder reqBuilder = BatchAttachRequest.newBuilder()
            .setLeader(Replica.newBuilder()
                .setRangeId(batcherKey.id)
                .setStoreId(batcherKey.leaderStoreId)
                .build());
        callTasks.forEach(call -> {
            AttachRequest request = call.call();
            BatchAttachRequest.Params.Builder paramsBuilder = BatchAttachRequest.Params.newBuilder()
                .setInboxId(request.getInboxId())
                .setIncarnation(HLC.INST.get()) // new incarnation
                .setExpirySeconds(request.getExpirySeconds())
                .setLimit(request.getLimit())
                .setDropOldest(request.getDropOldest())
                .setClient(request.getClient())
                .setNow(request.getNow());
            if (request.hasLwt()) {
                paramsBuilder.setLwt(request.getLwt());
            }
            reqBuilder.addParams(paramsBuilder.build());
        });
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchAttach(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<AttachRequest, AttachReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        ICallTask<AttachRequest, AttachReply, MutationCallBatcherKey> callTask;
        assert batchedTasks.size() == output.getInboxService().getBatchAttach().getVersionCount();

        int i = 0;
        while ((callTask = batchedTasks.poll()) != null) {
            callTask.resultPromise()
                .complete(AttachReply.newBuilder()
                    .setReqId(callTask.call().getReqId())
                    .setCode(AttachReply.Code.OK)
                    .setVersion(output.getInboxService().getBatchAttach().getVersion(i++))
                    .build());
        }
    }

    @Override
    protected void handleException(ICallTask<AttachRequest, AttachReply, MutationCallBatcherKey> callTask,
                                   Throwable e) {
        if (e instanceof ServerNotFoundException) {
            callTask.resultPromise().complete(AttachReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(AttachReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof BadVersionException) {
            callTask.resultPromise().complete(AttachReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(AttachReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof TryLaterException) {
            callTask.resultPromise().complete(AttachReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(AttachReply.Code.TRY_LATER)
                .build());
            return;
        }
        callTask.resultPromise().completeExceptionally(e);
    }

    private static class BatchAttachCallTask extends MutationCallTaskBatch<AttachRequest, AttachReply> {
        private final Map<String, Set<String>> inboxes = new HashMap<>();

        private BatchAttachCallTask(long ver) {
            super(ver);
        }

        @Override
        protected void add(ICallTask<AttachRequest, AttachReply, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.computeIfAbsent(callTask.call().getClient().getTenantId(), k -> new HashSet<>())
                .add(callTask.call().getInboxId());
        }

        @Override
        protected boolean isBatchable(ICallTask<AttachRequest, AttachReply, MutationCallBatcherKey> callTask) {
            return !inboxes.getOrDefault(callTask.call().getClient().getTenantId(), Collections.emptySet())
                .contains(callTask.call().getInboxId());
        }
    }
}
