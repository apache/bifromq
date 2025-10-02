/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.inbox.server.scheduler;

import java.util.Iterator;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.client.IQueryPipeline;
import org.apache.bifromq.basekv.client.exception.BadVersionException;
import org.apache.bifromq.basekv.client.exception.TryLaterException;
import org.apache.bifromq.basekv.client.scheduler.BatchQueryCall;
import org.apache.bifromq.basekv.client.scheduler.QueryCallBatcherKey;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.baserpc.client.exception.ServerNotFoundException;
import org.apache.bifromq.basescheduler.ICallTask;
import org.apache.bifromq.inbox.rpc.proto.InboxStateReply;
import org.apache.bifromq.inbox.rpc.proto.InboxStateRequest;
import org.apache.bifromq.inbox.storage.proto.BatchFetchInboxStateReply;
import org.apache.bifromq.inbox.storage.proto.BatchFetchInboxStateRequest;
import org.apache.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;

@Slf4j
class BatchInboxStateCall extends BatchQueryCall<InboxStateRequest, InboxStateReply> {
    protected BatchInboxStateCall(IQueryPipeline pipeline, QueryCallBatcherKey batcherKey) {
        super(pipeline, batcherKey);
    }

    @Override
    protected ROCoProcInput makeBatch(Iterator<InboxStateRequest> reqIterator) {
        BatchFetchInboxStateRequest.Builder reqBuilder = BatchFetchInboxStateRequest.newBuilder();
        reqIterator.forEachRemaining(
            request -> reqBuilder
                .addParams(BatchFetchInboxStateRequest.Params.newBuilder()
                    .setTenantId(request.getTenantId())
                    .setInboxId(request.getInboxId())
                    .setNow(request.getNow())
                    .build()));
        long reqId = System.nanoTime();
        return ROCoProcInput.newBuilder()
            .setInboxService(InboxServiceROCoProcInput.newBuilder()
                .setReqId(reqId)
                .setFetchInboxState(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<InboxStateRequest, InboxStateReply, QueryCallBatcherKey>> batchedTasks,
                                ROCoProcOutput output) {
        ICallTask<InboxStateRequest, InboxStateReply, QueryCallBatcherKey> task;
        assert batchedTasks.size() == output.getInboxService().getFetchInboxState().getResultCount();
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            BatchFetchInboxStateReply.Result result = output.getInboxService().getFetchInboxState().getResult(i++);
            switch (result.getCode()) {
                case OK -> task.resultPromise().complete(InboxStateReply.newBuilder()
                    .setReqId(task.call().getReqId())
                    .setCode(InboxStateReply.Code.OK)
                    .setState(result.getState())
                    .build());
                case NO_INBOX -> task.resultPromise().complete(InboxStateReply.newBuilder()
                    .setReqId(task.call().getReqId())
                    .setCode(InboxStateReply.Code.NO_INBOX)
                    .build());
                default -> task.resultPromise().complete(InboxStateReply.newBuilder()
                    .setReqId(task.call().getReqId())
                    .setCode(InboxStateReply.Code.EXPIRED)
                    .build());
            }
        }
    }

    @Override
    protected void handleException(ICallTask<InboxStateRequest, InboxStateReply, QueryCallBatcherKey> callTask,
                                   Throwable e) {
        if (e instanceof ServerNotFoundException) {
            callTask.resultPromise().complete(InboxStateReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(InboxStateReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof BadVersionException) {
            callTask.resultPromise().complete(InboxStateReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(InboxStateReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof TryLaterException) {
            callTask.resultPromise().complete(InboxStateReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(InboxStateReply.Code.TRY_LATER)
                .build());
            return;
        }
        callTask.resultPromise().completeExceptionally(e);
    }
}
