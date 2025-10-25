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

package org.apache.bifromq.basekv.client.scheduler;

import static org.testng.Assert.assertEquals;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bifromq.basekv.client.IQueryPipeline;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.proto.KVRangeROReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.basescheduler.ICallTask;
import org.testng.annotations.Test;

public class BatchQueryCallTest {
    @Test
    public void onlyOneQueryPerBatch() {
        CountingQueryPipeline pipeline = new CountingQueryPipeline();
        QueryCallBatcherKey key = new QueryCallBatcherKey(KVRangeId.newBuilder().setId(1).build(),
            "storeA", 0, 1L, false);
        DummyBatchQueryCall call = new DummyBatchQueryCall(pipeline, key);

        // add multiple tasks into the same batch
        DummyTask t1 = new DummyTask(key);
        DummyTask t2 = new DummyTask(key);
        call.add(t1);
        call.add(t2);

        // execute and wait
        call.execute().join();

        // only 1 underlying query should be fired
        assertEquals(pipeline.count.get(), 1);
        // ensure tasks completed
        t1.resultPromise().join();
        t2.resultPromise().join();
    }

    private static class CountingQueryPipeline implements IQueryPipeline {
        final AtomicInteger count = new AtomicInteger();

        @Override
        public CompletableFuture<KVRangeROReply> query(KVRangeRORequest request) {
            count.incrementAndGet();
            return CompletableFuture.completedFuture(KVRangeROReply.newBuilder()
                .setCode(ReplyCode.Ok)
                .build());
        }

        @Override
        public void close() {
        }
    }

    private static class DummyBatchQueryCall extends BatchQueryCall<Integer, Integer> {
        DummyBatchQueryCall(IQueryPipeline pipeline, QueryCallBatcherKey key) {
            super(pipeline, key);
        }

        @Override
        protected ROCoProcInput makeBatch(java.util.Iterator<Integer> reqIterator) {
            return ROCoProcInput.getDefaultInstance();
        }

        @Override
        protected void handleOutput(Queue<ICallTask<Integer, Integer, QueryCallBatcherKey>> batchedTasks,
                                    ROCoProcOutput output) {
            ICallTask<Integer, Integer, QueryCallBatcherKey> task;
            while ((task = batchedTasks.poll()) != null) {
                task.resultPromise().complete(1);
            }
        }

        @Override
        protected void handleException(ICallTask<Integer, Integer, QueryCallBatcherKey> callTask, Throwable e) {
            callTask.resultPromise().completeExceptionally(e);
        }
    }

    private static class DummyTask implements ICallTask<Integer, Integer, QueryCallBatcherKey> {
        private final QueryCallBatcherKey key;
        private final CompletableFuture<Integer> promise = new CompletableFuture<>();

        DummyTask(QueryCallBatcherKey key) {
            this.key = key;
        }

        @Override
        public Integer call() {
            return 1;
        }

        @Override
        public CompletableFuture<Integer> resultPromise() {
            return promise;
        }

        @Override
        public QueryCallBatcherKey batcherKey() {
            return key;
        }

        @Override
        public long ts() {
            return System.nanoTime();
        }
    }
}

