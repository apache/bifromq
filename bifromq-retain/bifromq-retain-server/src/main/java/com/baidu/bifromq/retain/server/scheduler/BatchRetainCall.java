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

package com.baidu.bifromq.retain.server.scheduler;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.BatchMutationCall;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basescheduler.ICallTask;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.retain.rpc.proto.RetainRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainResult;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceRWCoProcInput;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BatchRetainCall extends BatchMutationCall<RetainRequest, RetainReply> {

    protected BatchRetainCall(KVRangeId rangeId,
                              IBaseKVStoreClient retainStoreClient,
                              Duration pipelineExpiryTime) {
        super(rangeId, retainStoreClient, pipelineExpiryTime);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterator<RetainRequest> retainRequestIterator) {
        return RWCoProcInput.newBuilder()
            .setRetainService(RetainServiceRWCoProcInput.newBuilder()
                .setBatchRetain(BatchRetainCallHelper.makeBatch(retainRequestIterator))
                .build()).build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<RetainRequest, RetainReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        ICallTask<RetainRequest, RetainReply, MutationCallBatcherKey> task;
        while ((task = batchedTasks.poll()) != null) {
            RetainReply.Builder replyBuilder = RetainReply.newBuilder()
                .setReqId(task.call().getReqId());
            Map<String, RetainResult> resultMap = output.getRetainService()
                .getBatchRetain()
                .getResultsMap();
            RetainResult topicMap = resultMap.get(task.call().getPublisher().getTenantId());
            if (topicMap == null) {
                log.error("tenantId not found in result map, tenantId: {}", task.call().getPublisher().getTenantId());
                task.resultPromise().complete(replyBuilder.setResult(RetainReply.Result.ERROR).build());
                continue;
            }
            RetainResult.Code result = topicMap.getResultsMap().get(task.call().getTopic());
            if (result == null) {
                log.error("topic not found in result map, tenantId: {}, topic: {}",
                    task.call().getPublisher().getTenantId(), task.call().getTopic());
                task.resultPromise().complete(replyBuilder.setResult(RetainReply.Result.ERROR).build());
                continue;
            }
            switch (result) {
                case RETAINED -> replyBuilder.setResult(RetainReply.Result.RETAINED);
                case CLEARED -> replyBuilder.setResult(RetainReply.Result.CLEARED);
                case ERROR -> replyBuilder.setResult(RetainReply.Result.ERROR);
            }
            task.resultPromise().complete(replyBuilder.build());
        }
    }

    @Override
    protected void handleException(ICallTask<RetainRequest, RetainReply, MutationCallBatcherKey> callTask,
                                   Throwable e) {
        callTask.resultPromise().complete(RetainReply.newBuilder()
            .setReqId(callTask.call().getReqId())
            .setResult(RetainReply.Result.ERROR)
            .build());
    }
}
