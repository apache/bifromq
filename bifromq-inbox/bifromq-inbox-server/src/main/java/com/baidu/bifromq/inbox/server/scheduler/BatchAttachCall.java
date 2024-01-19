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
import com.baidu.bifromq.inbox.rpc.proto.AttachReply;
import com.baidu.bifromq.inbox.rpc.proto.AttachRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchAttachReply;
import com.baidu.bifromq.inbox.storage.proto.BatchAttachRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

public class BatchAttachCall extends BatchMutationCall<AttachRequest, AttachReply> {

    protected BatchAttachCall(KVRangeId rangeId,
                              IBaseKVStoreClient distWorkerClient,
                              Duration pipelineExpiryTime) {
        super(rangeId, distWorkerClient, pipelineExpiryTime);
    }

    @Override
    protected BatchCallTask<AttachRequest, AttachReply> newBatch(String storeId, long ver) {
        return new BatchAttachCallTask(storeId, ver);
    }


    @Override
    protected RWCoProcInput makeBatch(Iterator<AttachRequest> reqIterator) {
        BatchAttachRequest.Builder reqBuilder = BatchAttachRequest.newBuilder();
        reqIterator.forEachRemaining(request -> {
            BatchAttachRequest.Params.Builder paramsBuilder = BatchAttachRequest.Params.newBuilder()
                .setInboxId(request.getInboxId())
                .setIncarnation(request.getIncarnation()) // new incarnation
                .setVersion(request.getVersion())
                .setExpirySeconds(request.getExpirySeconds())
                .setKeepAliveSeconds(request.getKeepAliveSeconds())
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
    protected void handleOutput(Queue<CallTask<AttachRequest, AttachReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        CallTask<AttachRequest, AttachReply, MutationCallBatcherKey> callTask;
        assert batchedTasks.size() == output.getInboxService().getBatchAttach().getResultCount();

        int i = 0;
        while ((callTask = batchedTasks.poll()) != null) {
            BatchAttachReply.Result result = output.getInboxService().getBatchAttach().getResult(i++);
            AttachReply.Builder replyBuilder = AttachReply.newBuilder().setReqId(callTask.call.getReqId());
            switch (result.getCode()) {
                case OK -> callTask.callResult.complete(replyBuilder
                    .setCode(AttachReply.Code.OK)
                    .addAllTopicFilters(result.getTopicFilterList())
                    .build());
                case NO_INBOX -> callTask.callResult.complete(replyBuilder
                    .setCode(AttachReply.Code.NO_INBOX)
                    .build());
                case CONFLICT -> callTask.callResult.complete(replyBuilder
                    .setCode(AttachReply.Code.CONFLICT)
                    .build());
                case ERROR -> callTask.callResult.complete(replyBuilder
                    .setCode(AttachReply.Code.ERROR)
                    .build());
            }
        }
    }

    @Override
    protected void handleException(CallTask<AttachRequest, AttachReply, MutationCallBatcherKey> callTask,
                                   Throwable e) {
        callTask.callResult.complete(AttachReply.newBuilder()
            .setReqId(callTask.call.getReqId())
            .setCode(AttachReply.Code.ERROR)
            .build());
    }

    private static class BatchAttachCallTask extends BatchCallTask<AttachRequest, AttachReply> {
        private final Set<ScopedInbox> inboxes = new HashSet<>();

        private BatchAttachCallTask(String storeId, long ver) {
            super(storeId, ver);
        }

        @Override
        protected void add(CallTask<AttachRequest, AttachReply, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.add(new ScopedInbox(
                callTask.call.getClient().getTenantId(),
                callTask.call.getInboxId(),
                callTask.call.getIncarnation())
            );
        }

        @Override
        protected boolean isBatchable(CallTask<AttachRequest, AttachReply, MutationCallBatcherKey> callTask) {
            return !inboxes.contains(new ScopedInbox(
                callTask.call.getClient().getTenantId(),
                callTask.call.getInboxId(),
                callTask.call.getIncarnation()));
        }
    }
}
