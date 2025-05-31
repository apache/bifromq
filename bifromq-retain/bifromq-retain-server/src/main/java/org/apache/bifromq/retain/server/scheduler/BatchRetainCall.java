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

package org.apache.bifromq.retain.server.scheduler;

import org.apache.bifromq.basekv.client.IMutationPipeline;
import org.apache.bifromq.basekv.client.exception.BadVersionException;
import org.apache.bifromq.basekv.client.exception.TryLaterException;
import org.apache.bifromq.basekv.client.scheduler.BatchMutationCall;
import org.apache.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import org.apache.bifromq.baserpc.client.exception.ServerNotFoundException;
import org.apache.bifromq.basescheduler.ICallTask;
import org.apache.bifromq.retain.rpc.proto.RetainReply;
import org.apache.bifromq.retain.rpc.proto.RetainRequest;
import org.apache.bifromq.retain.rpc.proto.RetainResult;
import org.apache.bifromq.retain.rpc.proto.RetainServiceRWCoProcInput;
import com.google.common.collect.Iterables;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BatchRetainCall extends BatchMutationCall<RetainRequest, RetainReply> {

    protected BatchRetainCall(IMutationPipeline pipeline, MutationCallBatcherKey batcherKey) {
        super(pipeline, batcherKey);
    }

    @Override
    protected RWCoProcInput makeBatch(
        Iterable<ICallTask<RetainRequest, RetainReply, MutationCallBatcherKey>> callTasks) {
        Iterator<RetainRequest> retainRequestIterator = Iterables.transform(callTasks, ICallTask::call).iterator();
        return RWCoProcInput.newBuilder().setRetainService(RetainServiceRWCoProcInput.newBuilder()
            .setBatchRetain(BatchRetainCallHelper.makeBatch(retainRequestIterator)).build()).build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<RetainRequest, RetainReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        ICallTask<RetainRequest, RetainReply, MutationCallBatcherKey> task;
        while ((task = batchedTasks.poll()) != null) {
            RetainReply.Builder replyBuilder = RetainReply.newBuilder().setReqId(task.call().getReqId());
            Map<String, RetainResult> resultMap = output.getRetainService().getBatchRetain().getResultsMap();
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
                default -> {
                    log.error("unknown result code:{}", result);
                    replyBuilder.setResult(RetainReply.Result.ERROR);
                }
            }
            task.resultPromise().complete(replyBuilder.build());
        }
    }

    @Override
    protected void handleException(ICallTask<RetainRequest, RetainReply, MutationCallBatcherKey> callTask,
                                   Throwable e) {
        if (e instanceof ServerNotFoundException) {
            callTask.resultPromise().complete(RetainReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setResult(RetainReply.Result.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof BadVersionException) {
            callTask.resultPromise().complete(RetainReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setResult(RetainReply.Result.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof TryLaterException) {
            callTask.resultPromise().complete(RetainReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setResult(RetainReply.Result.TRY_LATER)
                .build());
            return;
        }
        callTask.resultPromise().completeExceptionally(e);
    }
}
