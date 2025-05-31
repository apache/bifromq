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

import org.apache.bifromq.basekv.client.IQueryPipeline;
import org.apache.bifromq.basekv.client.exception.BadVersionException;
import org.apache.bifromq.basekv.client.exception.TryLaterException;
import org.apache.bifromq.basekv.client.scheduler.BatchQueryCall;
import org.apache.bifromq.basekv.client.scheduler.QueryCallBatcherKey;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.baserpc.client.exception.ServerNotFoundException;
import org.apache.bifromq.basescheduler.ICallTask;
import org.apache.bifromq.inbox.rpc.proto.ExistReply;
import org.apache.bifromq.inbox.rpc.proto.ExistRequest;
import org.apache.bifromq.inbox.storage.proto.BatchExistRequest;
import org.apache.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import java.util.Iterator;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchExistCall extends BatchQueryCall<ExistRequest, ExistReply> {
    protected BatchExistCall(IQueryPipeline pipeline, QueryCallBatcherKey batcherKey) {
        super(pipeline, batcherKey);
    }

    @Override
    protected ROCoProcInput makeBatch(Iterator<ExistRequest> reqIterator) {
        BatchExistRequest.Builder reqBuilder = BatchExistRequest.newBuilder();
        reqIterator.forEachRemaining(
            request -> reqBuilder
                .addParams(BatchExistRequest.Params.newBuilder()
                    .setTenantId(request.getTenantId())
                    .setInboxId(request.getInboxId())
                    .setNow(request.getNow())
                    .build()));
        long reqId = System.nanoTime();
        return ROCoProcInput.newBuilder()
            .setInboxService(InboxServiceROCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchExist(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<ExistRequest, ExistReply, QueryCallBatcherKey>> batchedTasks,
                                ROCoProcOutput output) {
        ICallTask<ExistRequest, ExistReply, QueryCallBatcherKey> task;
        assert batchedTasks.size() == output.getInboxService().getBatchExist().getExistCount();
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            boolean exist = output.getInboxService().getBatchExist().getExist(i++);
            task.resultPromise().complete(ExistReply.newBuilder()
                .setReqId(task.call().getReqId())
                .setCode(exist ? ExistReply.Code.EXIST : ExistReply.Code.NO_INBOX)
                .build());
        }
    }

    @Override
    protected void handleException(ICallTask<ExistRequest, ExistReply, QueryCallBatcherKey> callTask,
                                   Throwable e) {
        if (e instanceof ServerNotFoundException) {
            callTask.resultPromise().complete(ExistReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(ExistReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof BadVersionException) {
            callTask.resultPromise().complete(ExistReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(ExistReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof TryLaterException) {
            callTask.resultPromise().complete(ExistReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(ExistReply.Code.TRY_LATER)
                .build());
            return;
        }
        callTask.resultPromise().completeExceptionally(e);
    }
}
