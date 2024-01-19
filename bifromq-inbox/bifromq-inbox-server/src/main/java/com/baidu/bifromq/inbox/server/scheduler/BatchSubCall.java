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

package com.baidu.bifromq.inbox.server.scheduler;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.BatchMutationCall;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.inbox.records.ScopedInbox;
import com.baidu.bifromq.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchSubRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

public class BatchSubCall extends BatchMutationCall<SubRequest, SubReply> {
    protected BatchSubCall(KVRangeId rangeId,
                           IBaseKVStoreClient distWorkerClient,
                           Duration pipelineExpiryTime) {
        super(rangeId, distWorkerClient, pipelineExpiryTime);
    }

    @Override
    protected BatchCallTask<SubRequest, SubReply> newBatch(String storeId, long ver) {
        return new BatchSubCallTask(storeId, ver);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterator<SubRequest> subRequestIterator) {
        BatchSubRequest.Builder reqBuilder = BatchSubRequest.newBuilder();
        subRequestIterator.forEachRemaining(request -> reqBuilder.addParams(BatchSubRequest.Params.newBuilder()
            .setTenantId(request.getTenantId())
            .setInboxId(request.getInboxId())
            .setIncarnation(request.getIncarnation())
            .setVersion(request.getVersion())
            .setTopicFilter(request.getTopicFilter())
            .setSubQoS(request.getSubQoS())
            .setNow(request.getNow())
            .build()));
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchSub(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<CallTask<SubRequest, SubReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        assert batchedTasks.size() == output.getInboxService().getBatchSub().getCodeCount();
        CallTask<SubRequest, SubReply, MutationCallBatcherKey> task;
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            SubReply.Builder replyBuilder = SubReply.newBuilder().setReqId(task.call.getReqId());
            switch (output.getInboxService().getBatchSub().getCode(i++)) {
                case OK -> task.callResult.complete(replyBuilder.setCode(SubReply.Code.OK).build());
                case EXISTS -> task.callResult.complete(replyBuilder.setCode(SubReply.Code.EXISTS).build());
                case NO_INBOX -> task.callResult.complete(replyBuilder.setCode(SubReply.Code.NO_INBOX).build());
                case EXCEED_LIMIT -> task.callResult.complete(replyBuilder.setCode(SubReply.Code.EXCEED_LIMIT).build());
                case CONFLICT -> task.callResult.complete(replyBuilder.setCode(SubReply.Code.CONFLICT).build());
                case ERROR -> task.callResult.complete(replyBuilder.setCode(SubReply.Code.ERROR).build());
            }
        }

    }

    @Override
    protected void handleException(CallTask<SubRequest, SubReply, MutationCallBatcherKey> callTask, Throwable e) {
        callTask.callResult.complete(SubReply.newBuilder()
            .setReqId(callTask.call.getReqId())
            .setCode(SubReply.Code.ERROR)
            .build());

    }

    private static class BatchSubCallTask extends BatchCallTask<SubRequest, SubReply> {
        private final Set<ScopedInbox> inboxes = new HashSet<>();

        private BatchSubCallTask(String storeId, long ver) {
            super(storeId, ver);
        }

        @Override
        protected void add(CallTask<SubRequest, SubReply, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.add(new ScopedInbox(
                callTask.call.getTenantId(),
                callTask.call.getInboxId(),
                callTask.call.getIncarnation())
            );
        }

        @Override
        protected boolean isBatchable(CallTask<SubRequest, SubReply, MutationCallBatcherKey> callTask) {
            return !inboxes.contains(new ScopedInbox(
                callTask.call.getTenantId(),
                callTask.call.getInboxId(),
                callTask.call.getIncarnation()));
        }
    }
}
