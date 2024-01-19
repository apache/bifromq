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
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteReply;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

public class BatchDeleteCall extends BatchMutationCall<BatchDeleteRequest.Params, BatchDeleteReply.Result> {

    protected BatchDeleteCall(KVRangeId rangeId,
                              IBaseKVStoreClient distWorkerClient,
                              Duration pipelineExpiryTime) {
        super(rangeId, distWorkerClient, pipelineExpiryTime);
    }

    @Override
    protected BatchCallTask<BatchDeleteRequest.Params, BatchDeleteReply.Result> newBatch(String storeId, long ver) {
        return new BatchDeleteCallTask(storeId, ver);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterator<BatchDeleteRequest.Params> reqIterator) {
        BatchDeleteRequest.Builder reqBuilder = BatchDeleteRequest.newBuilder();
        reqIterator.forEachRemaining(reqBuilder::addParams);

        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchDelete(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(
        Queue<CallTask<BatchDeleteRequest.Params, BatchDeleteReply.Result, MutationCallBatcherKey>> batchedTasks,
        RWCoProcOutput output) {
        CallTask<BatchDeleteRequest.Params, BatchDeleteReply.Result, MutationCallBatcherKey> callTask;
        assert batchedTasks.size() == output.getInboxService().getBatchDelete().getResultCount();

        int i = 0;
        while ((callTask = batchedTasks.poll()) != null) {
            callTask.callResult.complete(output.getInboxService().getBatchDelete().getResult(i++));
        }
    }

    @Override
    protected void handleException(
        CallTask<BatchDeleteRequest.Params, BatchDeleteReply.Result, MutationCallBatcherKey> callTask,
        Throwable e) {
        callTask.callResult.complete(BatchDeleteReply.Result.newBuilder()
            .setCode(BatchDeleteReply.Code.ERROR)
            .build());
    }

    private static class BatchDeleteCallTask extends BatchCallTask<BatchDeleteRequest.Params, BatchDeleteReply.Result> {
        private final Set<ScopedInbox> inboxes = new HashSet<>();

        private BatchDeleteCallTask(String storeId, long ver) {
            super(storeId, ver);
        }

        @Override
        protected void add(
            CallTask<BatchDeleteRequest.Params, BatchDeleteReply.Result, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.add(new ScopedInbox(
                callTask.call.getTenantId(),
                callTask.call.getInboxId(),
                callTask.call.getIncarnation())
            );
        }

        @Override
        protected boolean isBatchable(
            CallTask<BatchDeleteRequest.Params, BatchDeleteReply.Result, MutationCallBatcherKey> callTask) {
            return !inboxes.contains(new ScopedInbox(
                callTask.call.getTenantId(),
                callTask.call.getInboxId(),
                callTask.call.getIncarnation()));
        }
    }
}
