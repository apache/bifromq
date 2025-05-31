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

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.client.IQueryPipeline;
import org.apache.bifromq.basekv.client.exception.BadVersionException;
import org.apache.bifromq.basekv.client.exception.TryLaterException;
import org.apache.bifromq.basekv.client.scheduler.BatchQueryCall;
import org.apache.bifromq.basekv.client.scheduler.QueryCallBatcherKey;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.baserpc.client.exception.ServerNotFoundException;
import org.apache.bifromq.basescheduler.ICallTask;
import org.apache.bifromq.inbox.record.TenantInboxInstance;
import org.apache.bifromq.inbox.storage.proto.BatchCheckSubReply;
import org.apache.bifromq.inbox.storage.proto.BatchCheckSubRequest;
import org.apache.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import org.apache.bifromq.plugin.subbroker.CheckReply;
import java.util.Iterator;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchCheckSubCall extends BatchQueryCall<CheckMatchInfo, CheckReply.Code> {
    protected BatchCheckSubCall(IQueryPipeline pipeline, QueryCallBatcherKey batcherKey) {
        super(pipeline, batcherKey);
    }

    @Override
    protected ROCoProcInput makeBatch(Iterator<CheckMatchInfo> reqIterator) {
        BatchCheckSubRequest.Builder reqBuilder = BatchCheckSubRequest.newBuilder().setNow(HLC.INST.getPhysical());
        reqIterator.forEachRemaining(request -> {
            TenantInboxInstance tenantInboxInstance = TenantInboxInstance.from(request.tenantId(), request.matchInfo());
            reqBuilder.addParams(BatchCheckSubRequest.Params.newBuilder()
                .setTenantId(tenantInboxInstance.tenantId())
                .setInboxId(tenantInboxInstance.instance().inboxId())
                .setIncarnation(tenantInboxInstance.instance().incarnation())
                .setTopicFilter(request.matchInfo().getMatcher().getMqttTopicFilter())
                .build());
        });
        long reqId = System.nanoTime();
        return ROCoProcInput.newBuilder().setInboxService(
                InboxServiceROCoProcInput.newBuilder().setReqId(reqId).setBatchCheckSub(reqBuilder.build()).build())
            .build();
    }

    @Override
    protected void handleOutput(
        Queue<ICallTask<CheckMatchInfo, CheckReply.Code, QueryCallBatcherKey>> batchedTasks,
        ROCoProcOutput output) {
        ICallTask<CheckMatchInfo, CheckReply.Code, QueryCallBatcherKey> task;
        assert batchedTasks.size() == output.getInboxService().getBatchCheckSub().getCodeCount();
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            BatchCheckSubReply.Code code = output.getInboxService().getBatchCheckSub().getCode(i++);
            switch (code) {
                case OK -> task.resultPromise().complete(CheckReply.Code.OK);
                case NO_MATCH -> task.resultPromise().complete(CheckReply.Code.NO_SUB);
                case NO_INBOX -> task.resultPromise().complete(CheckReply.Code.NO_RECEIVER);
                default -> task.resultPromise().complete(CheckReply.Code.ERROR);
            }
        }
    }

    @Override
    protected void handleException(ICallTask<CheckMatchInfo, CheckReply.Code, QueryCallBatcherKey> callTask,
                                   Throwable e) {
        if (e instanceof ServerNotFoundException) {
            callTask.resultPromise().complete(CheckReply.Code.TRY_LATER);
            return;
        }
        if (e instanceof BadVersionException) {
            callTask.resultPromise().complete(CheckReply.Code.TRY_LATER);
            return;
        }
        if (e instanceof TryLaterException) {
            callTask.resultPromise().complete(CheckReply.Code.TRY_LATER);
            return;
        }
        callTask.resultPromise().completeExceptionally(e);
    }
}
