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

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedTopicFilter;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.BatchMutationCall;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import java.time.Duration;
import java.util.Iterator;
import java.util.Queue;

public class BatchUnsubCall extends BatchMutationCall<UnsubRequest, UnsubReply> {
    protected BatchUnsubCall(KVRangeId rangeId,
                             IBaseKVStoreClient distWorkerClient,
                             Duration pipelineExpiryTime) {
        super(rangeId, distWorkerClient, pipelineExpiryTime);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterator<UnsubRequest> unsubRequestIterator) {
        BatchUnsubRequest.Builder reqBuilder = BatchUnsubRequest.newBuilder();
        unsubRequestIterator.forEachRemaining(request -> reqBuilder.addTopicFilters(
            scopedTopicFilter(request.getTenantId(), request.getInboxId(),
                request.getTopicFilter())));
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchUnsub(reqBuilder.setReqId(reqId).build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<CallTask<UnsubRequest, UnsubReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        CallTask<UnsubRequest, UnsubReply, MutationCallBatcherKey> task;
        while ((task = batchedTasks.poll()) != null) {
            task.callResult.complete(UnsubReply.newBuilder()
                .setReqId(task.call.getReqId())
                .setResult(UnsubReply.Result.forNumber(output.getInboxService().getBatchUnsub()
                    .getResultsMap()
                    .get(scopedTopicFilter(task.call.getTenantId(),
                        task.call.getInboxId(), task.call.getTopicFilter()).toStringUtf8())
                    .getNumber()))
                .build());
        }
    }

    @Override
    protected void handleException(CallTask<UnsubRequest, UnsubReply, MutationCallBatcherKey> callTask, Throwable e) {
        callTask.callResult.complete(UnsubReply.newBuilder()
            .setReqId(callTask.call.getReqId())
            .setResult(UnsubReply.Result.ERROR)
            .build());
    }
}
