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

package com.baidu.bifromq.basekv.client.scheduler;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basescheduler.CallTask;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Iterator;
import java.util.Queue;

public class TestBatchQueryCall extends BatchQueryCall<ByteString, ByteString> {
    protected TestBatchQueryCall(KVRangeId rangeId,
                                 IBaseKVStoreClient storeClient,
                                 boolean linearizable,
                                 Duration pipelineExpiryTime) {
        super(rangeId, storeClient, linearizable, pipelineExpiryTime);
    }

    @Override
    protected ROCoProcInput makeBatch(Iterator<ByteString> byteStringIterator) {
        ByteString finalBS = ByteString.empty();
        while (byteStringIterator.hasNext()) {
            finalBS = finalBS.concat(byteStringIterator.next());
        }
        return ROCoProcInput.newBuilder()
            .setRaw(finalBS)
            .build();
    }

    @Override
    protected void handleOutput(Queue<CallTask<ByteString, ByteString, QueryCallBatcherKey>> batchedTasks,
                                ROCoProcOutput output) {
        CallTask<ByteString, ByteString, QueryCallBatcherKey> task;
        while ((task = batchedTasks.poll()) != null) {
            // just echo the request
            task.callResult.complete(task.call);
        }

    }

    @Override
    protected void handleException(CallTask<ByteString, ByteString, QueryCallBatcherKey> callTask, Throwable e) {
        callTask.callResult.completeExceptionally(e);
    }
}
