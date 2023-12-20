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
import com.baidu.bifromq.basescheduler.Batcher;
import com.google.protobuf.ByteString;
import java.time.Duration;

public class TestMutationCallScheduler extends MutationCallScheduler<ByteString, ByteString> {
    private final Duration pipelineExpire;

    public TestMutationCallScheduler(String name,
                                     IBaseKVStoreClient storeClient,
                                     Duration tolerableLatency,
                                     Duration burstLatency,
                                     Duration pipelineExpire) {
        super(name, storeClient, tolerableLatency, burstLatency);
        this.pipelineExpire = pipelineExpire;
    }

    public TestMutationCallScheduler(String name,
                                     IBaseKVStoreClient storeClient,
                                     Duration tolerableLatency,
                                     Duration burstLatency,
                                     Duration pipelineExpire,
                                     Duration batcherExpiry) {
        super(name, storeClient, tolerableLatency, burstLatency, batcherExpiry);
        this.pipelineExpire = pipelineExpire;
    }

    @Override
    protected ByteString rangeKey(ByteString call) {
        return call;
    }

    @Override
    protected Batcher<ByteString, ByteString, MutationCallBatcherKey> newBatcher(String name,
                                                                                 long tolerableLatencyNanos,
                                                                                 long burstLatencyNanos,
                                                                                 MutationCallBatcherKey mutationCallBatcherKey) {
        return new TestMutationCallBatcher(
            name,
            tolerableLatencyNanos,
            burstLatencyNanos,
            mutationCallBatcherKey,
            storeClient,
            pipelineExpire
        );
    }
}
