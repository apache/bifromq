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

import org.apache.bifromq.basekv.client.IQueryPipeline;
import org.apache.bifromq.basekv.client.exception.BadVersionException;
import org.apache.bifromq.basekv.client.exception.TryLaterException;
import org.apache.bifromq.basekv.client.scheduler.BatchQueryCall;
import org.apache.bifromq.basekv.client.scheduler.QueryCallBatcherKey;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.baserpc.client.exception.ServerNotFoundException;
import org.apache.bifromq.basescheduler.ICallTask;
import org.apache.bifromq.inbox.rpc.proto.SendLWTReply;
import org.apache.bifromq.inbox.rpc.proto.SendLWTRequest;
import org.apache.bifromq.inbox.storage.proto.BatchSendLWTReply;
import org.apache.bifromq.inbox.storage.proto.BatchSendLWTRequest;
import org.apache.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import java.util.Iterator;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchSendLWTCall extends BatchQueryCall<SendLWTRequest, SendLWTReply> {
    protected BatchSendLWTCall(IQueryPipeline pipeline, QueryCallBatcherKey batcherKey) {
        super(pipeline, batcherKey);
    }

    @Override
    protected ROCoProcInput makeBatch(Iterator<SendLWTRequest> reqIterator) {
        BatchSendLWTRequest.Builder reqBuilder = BatchSendLWTRequest.newBuilder();
        reqIterator.forEachRemaining(
            request -> reqBuilder
                .addParams(BatchSendLWTRequest.Params.newBuilder()
                    .setTenantId(request.getTenantId())
                    .setInboxId(request.getInboxId())
                    .setVersion(request.getVersion())
                    .setNow(request.getNow())
                    .build()));
        long reqId = System.nanoTime();
        return ROCoProcInput.newBuilder()
            .setInboxService(InboxServiceROCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchSendLWT(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<SendLWTRequest, SendLWTReply, QueryCallBatcherKey>> batchedTasks,
                                ROCoProcOutput output) {
        ICallTask<SendLWTRequest, SendLWTReply, QueryCallBatcherKey> task;
        assert batchedTasks.size() == output.getInboxService().getBatchSendLWT().getCodeCount();
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            BatchSendLWTReply.Code code = output.getInboxService().getBatchSendLWT().getCode(i++);
            switch (code) {
                case OK -> task.resultPromise().complete(SendLWTReply.newBuilder()
                    .setReqId(task.call().getReqId())
                    .setCode(SendLWTReply.Code.OK)
                    .build());
                case NO_INBOX -> task.resultPromise().complete(SendLWTReply.newBuilder()
                    .setReqId(task.call().getReqId())
                    .setCode(SendLWTReply.Code.NO_INBOX)
                    .build());
                case CONFLICT -> task.resultPromise().complete(SendLWTReply.newBuilder()
                    .setReqId(task.call().getReqId())
                    .setCode(SendLWTReply.Code.CONFLICT)
                    .build());
                case TRY_LATER -> task.resultPromise().complete(SendLWTReply.newBuilder()
                    .setReqId(task.call().getReqId())
                    .setCode(SendLWTReply.Code.TRY_LATER)
                    .build());
                case ERROR -> task.resultPromise().complete(SendLWTReply.newBuilder()
                    .setReqId(task.call().getReqId())
                    .setCode(SendLWTReply.Code.ERROR)
                    .build());
            }
        }
    }

    @Override
    protected void handleException(ICallTask<SendLWTRequest, SendLWTReply, QueryCallBatcherKey> callTask,
                                   Throwable e) {
        if (e instanceof ServerNotFoundException) {
            callTask.resultPromise().complete(SendLWTReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(SendLWTReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof BadVersionException) {
            callTask.resultPromise().complete(SendLWTReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(SendLWTReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof TryLaterException) {
            callTask.resultPromise().complete(SendLWTReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(SendLWTReply.Code.TRY_LATER)
                .build());
            return;
        }
        callTask.resultPromise().completeExceptionally(e);
    }
}
