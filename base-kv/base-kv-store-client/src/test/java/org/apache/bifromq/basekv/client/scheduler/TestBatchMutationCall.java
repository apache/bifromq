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

import org.apache.bifromq.basekv.client.IMutationPipeline;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import org.apache.bifromq.basescheduler.ICallTask;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

public class TestBatchMutationCall extends BatchMutationCall<ByteString, ByteString> {
    protected TestBatchMutationCall(IMutationPipeline pipeline, MutationCallBatcherKey batcherKey) {
        super(pipeline, batcherKey);
    }

    @Override
    protected MutationCallTaskBatch<ByteString, ByteString> newBatch(long ver) {
        return new TestBatchCallTask(ver);
    }

    @Override
    protected RWCoProcInput makeBatch(
        Iterable<ICallTask<ByteString, ByteString, MutationCallBatcherKey>> batchedTasks) {
        Iterator<ByteString> byteStringIterator = Iterables.transform(batchedTasks, ICallTask::call).iterator();
        ByteString finalBS = byteStringIterator.hasNext() ? byteStringIterator.next() : ByteString.empty();
        while (byteStringIterator.hasNext()) {
            finalBS = finalBS.concat(ByteString.copyFromUtf8("_")).concat(byteStringIterator.next());
        }
        return RWCoProcInput.newBuilder()
            .setRaw(finalBS)
            .build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<ByteString, ByteString, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        ICallTask<ByteString, ByteString, MutationCallBatcherKey> task;
        while ((task = batchedTasks.poll()) != null) {
            // just echo the request
            task.resultPromise().complete(task.call());
        }
    }

    @Override
    protected void handleException(ICallTask<ByteString, ByteString, MutationCallBatcherKey> callTask, Throwable e) {
        callTask.resultPromise().completeExceptionally(e);
    }

    private static class TestBatchCallTask extends MutationCallTaskBatch<ByteString, ByteString> {
        private final Set<ByteString> keys = new HashSet<>();

        protected TestBatchCallTask(long ver) {
            super(ver);
        }

        @Override
        protected void add(ICallTask<ByteString, ByteString, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            keys.add(callTask.call());
        }

        @Override
        protected boolean isBatchable(ICallTask<ByteString, ByteString, MutationCallBatcherKey> callTask) {
            return !keys.contains(callTask.call());
        }
    }
}
