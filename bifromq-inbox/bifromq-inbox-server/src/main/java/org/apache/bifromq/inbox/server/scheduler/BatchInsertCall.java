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

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import org.apache.bifromq.basekv.client.IMutationPipeline;
import org.apache.bifromq.basekv.client.scheduler.BatchMutationCall;
import org.apache.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import org.apache.bifromq.basescheduler.ICallTask;
import org.apache.bifromq.inbox.record.InboxInstance;
import org.apache.bifromq.inbox.record.TenantInboxInstance;
import org.apache.bifromq.inbox.storage.proto.BatchInsertRequest;
import org.apache.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import org.apache.bifromq.inbox.storage.proto.InsertRequest;
import org.apache.bifromq.inbox.storage.proto.InsertResult;
import org.apache.bifromq.inbox.storage.proto.SubMessagePack;
import org.apache.bifromq.type.TopicMessagePack;

class BatchInsertCall extends BatchMutationCall<InsertRequest, InsertResult> {
    protected BatchInsertCall(IMutationPipeline pipeline, MutationCallBatcherKey batcherKey) {
        super(pipeline, batcherKey);
    }

    @Override
    protected MutationCallTaskBatch<InsertRequest, InsertResult> newBatch(long ver) {
        return new BatchInsertCallTask(ver);
    }

    @Override
    protected RWCoProcInput makeBatch(
        Iterable<ICallTask<InsertRequest, InsertResult, MutationCallBatcherKey>> callTasks) {
        BatchInsertRequest.Builder reqBuilder = BatchInsertRequest.newBuilder();
        // legacy non-compact format for backward compatibility
        // callTasks.forEach(call -> reqBuilder.addRequest(call.call()));

        // build message pool and insert references;
        IdentityHashMap<TopicMessagePack, Integer> poolIndex = new IdentityHashMap<>();
        List<TopicMessagePack> pool = new LinkedList<>();
        List<BatchInsertRequest.InsertRef> insertRefs = new LinkedList<>();

        for (ICallTask<InsertRequest, InsertResult, MutationCallBatcherKey> call : callTasks) {
            InsertRequest req = call.call();
            BatchInsertRequest.InsertRef.Builder refBuilder = BatchInsertRequest.InsertRef.newBuilder()
                .setTenantId(req.getTenantId())
                .setInboxId(req.getInboxId())
                .setIncarnation(req.getIncarnation());
            for (SubMessagePack subPack : req.getMessagePackList()) {
                TopicMessagePack msgPack = subPack.getMessages();
                Integer idx = poolIndex.get(msgPack);
                if (idx == null) {
                    idx = pool.size();
                    poolIndex.put(msgPack, idx);
                    pool.add(msgPack);
                }
                BatchInsertRequest.SubRef.Builder subRef = BatchInsertRequest.SubRef.newBuilder()
                    .addAllMatchedRoute(subPack.getMatchedRouteList())
                    .setMessagePackIndex(idx);
                refBuilder.addSubRef(subRef.build());
            }
            insertRefs.add(refBuilder.build());
        }
        reqBuilder.addAllTopicMessagePack(pool);
        reqBuilder.addAllInsertRef(insertRefs);

        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchInsert(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(
        Queue<ICallTask<InsertRequest, InsertResult, MutationCallBatcherKey>> batchedTasks,
        RWCoProcOutput output) {
        assert batchedTasks.size() == output.getInboxService().getBatchInsert().getResultCount();
        ICallTask<InsertRequest, InsertResult, MutationCallBatcherKey> task;
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            task.resultPromise().complete(output.getInboxService().getBatchInsert().getResult(i++));
        }
    }

    @Override
    protected void handleException(ICallTask<InsertRequest, InsertResult, MutationCallBatcherKey> callTask,
                                   Throwable e) {
        callTask.resultPromise().completeExceptionally(e);
    }

    private static class BatchInsertCallTask extends MutationCallTaskBatch<InsertRequest, InsertResult> {
        private final Set<TenantInboxInstance> inboxes = new HashSet<>();

        private BatchInsertCallTask(long ver) {
            super(ver);
        }

        @Override
        protected void add(ICallTask<InsertRequest, InsertResult, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.add(new TenantInboxInstance(
                callTask.call().getTenantId(),
                new InboxInstance(callTask.call().getInboxId(), callTask.call().getIncarnation()))
            );
        }

        @Override
        protected boolean isBatchable(
            ICallTask<InsertRequest, InsertResult, MutationCallBatcherKey> callTask) {
            return !inboxes.contains(new TenantInboxInstance(callTask.call().getTenantId(),
                new InboxInstance(callTask.call().getInboxId(), callTask.call().getIncarnation()))
            );
        }
    }
}
