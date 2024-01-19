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
import com.baidu.bifromq.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

public class BatchUnsubCall extends BatchMutationCall<UnsubRequest, UnsubReply> {
    protected BatchUnsubCall(KVRangeId rangeId,
                             IBaseKVStoreClient distWorkerClient,
                             Duration pipelineExpiryTime) {
        super(rangeId, distWorkerClient, pipelineExpiryTime);
    }

    @Override
    protected BatchCallTask<UnsubRequest, UnsubReply> newBatch(String storeId, long ver) {
        return new BatchUnsubCallTask(storeId, ver);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterator<UnsubRequest> unsubRequestIterator) {
        BatchUnsubRequest.Builder reqBuilder = BatchUnsubRequest.newBuilder();
        unsubRequestIterator.forEachRemaining(request -> reqBuilder.addParams(BatchUnsubRequest.Params.newBuilder()
            .setTenantId(request.getTenantId())
            .setInboxId(request.getInboxId())
            .setIncarnation(request.getIncarnation())
            .setVersion(request.getVersion())
            .setTopicFilter(request.getTopicFilter())
            .setNow(request.getNow())
            .build()));
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchUnsub(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<CallTask<UnsubRequest, UnsubReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        assert batchedTasks.size() == output.getInboxService().getBatchUnsub().getCodeCount();
        CallTask<UnsubRequest, UnsubReply, MutationCallBatcherKey> task;
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            UnsubReply.Builder replyBuilder = UnsubReply.newBuilder().setReqId(task.call.getReqId());
            switch (output.getInboxService().getBatchUnsub().getCode(i++)) {
                case OK -> task.callResult.complete(replyBuilder.setCode(UnsubReply.Code.OK).build());
                case NO_INBOX -> task.callResult.complete(replyBuilder.setCode(UnsubReply.Code.NO_INBOX).build());
                case NO_SUB -> task.callResult.complete(replyBuilder.setCode(UnsubReply.Code.NO_SUB).build());
                case CONFLICT -> task.callResult.complete(replyBuilder.setCode(UnsubReply.Code.CONFLICT).build());
                case ERROR -> task.callResult.complete(replyBuilder.setCode(UnsubReply.Code.ERROR).build());
            }
        }
    }

    @Override
    protected void handleException(CallTask<UnsubRequest, UnsubReply, MutationCallBatcherKey> callTask, Throwable e) {
        callTask.callResult.complete(UnsubReply.newBuilder()
            .setReqId(callTask.call.getReqId())
            .setCode(UnsubReply.Code.ERROR)
            .build());
    }

    private static class BatchUnsubCallTask extends BatchCallTask<UnsubRequest, UnsubReply> {
        private final Set<ScopedInbox> inboxes = new HashSet<>();

        private BatchUnsubCallTask(String storeId, long ver) {
            super(storeId, ver);
        }

        @Override
        protected void add(CallTask<UnsubRequest, UnsubReply, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.add(new ScopedInbox(
                callTask.call.getTenantId(),
                callTask.call.getInboxId(),
                callTask.call.getIncarnation())
            );
        }

        @Override
        protected boolean isBatchable(CallTask<UnsubRequest, UnsubReply, MutationCallBatcherKey> callTask) {
            return !inboxes.contains(new ScopedInbox(
                callTask.call.getTenantId(),
                callTask.call.getInboxId(),
                callTask.call.getIncarnation()));
        }
    }
}
