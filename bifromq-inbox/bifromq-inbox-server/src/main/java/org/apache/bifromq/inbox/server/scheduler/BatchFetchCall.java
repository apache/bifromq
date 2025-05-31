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
import org.apache.bifromq.inbox.storage.proto.BatchFetchRequest;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import org.apache.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import java.util.Iterator;
import java.util.Queue;

class BatchFetchCall extends BatchQueryCall<FetchRequest, Fetched> {
    protected BatchFetchCall(IQueryPipeline pipeline, QueryCallBatcherKey batcherKey) {
        super(pipeline, batcherKey);
    }

    @Override
    protected ROCoProcInput makeBatch(Iterator<FetchRequest> reqIterator) {
        BatchFetchRequest.Builder reqBuilder = BatchFetchRequest.newBuilder();
        reqIterator.forEachRemaining(request -> {
            BatchFetchRequest.Params.Builder paramsBuilder = BatchFetchRequest.Params.newBuilder()
                .setTenantId(request.tenantId())
                .setInboxId(request.inboxId())
                .setIncarnation(request.incarnation())
                .setMaxFetch(request.params().getMaxFetch());
            if (request.params().hasQos0StartAfter()) {
                paramsBuilder.setQos0StartAfter(request.params().getQos0StartAfter());
            }
            if (request.params().hasSendBufferStartAfter()) {
                paramsBuilder.setSendBufferStartAfter(request.params().getSendBufferStartAfter());
            }
            reqBuilder.addParams(paramsBuilder.build());
        });
        long reqId = System.nanoTime();
        return ROCoProcInput.newBuilder()
            .setInboxService(InboxServiceROCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchFetch(reqBuilder
                    .build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(
        Queue<ICallTask<FetchRequest, Fetched, QueryCallBatcherKey>> batchedTasks,
        ROCoProcOutput output) {
        ICallTask<FetchRequest, Fetched, QueryCallBatcherKey> task;
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            task.resultPromise().complete(output.getInboxService().getBatchFetch().getResult(i++));
        }
    }

    @Override
    protected void handleException(ICallTask<FetchRequest, Fetched, QueryCallBatcherKey> callTask, Throwable e) {
        if (e instanceof ServerNotFoundException) {
            callTask.resultPromise().complete(Fetched.newBuilder()
                .setResult(Fetched.Result.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof BadVersionException) {
            callTask.resultPromise().complete(Fetched.newBuilder()
                .setResult(Fetched.Result.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof TryLaterException) {
            callTask.resultPromise().complete(Fetched.newBuilder()
                .setResult(Fetched.Result.TRY_LATER)
                .build());
            return;
        }
        callTask.resultPromise().completeExceptionally(e);
    }
}
