/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.dist.client.scheduler;

import static java.util.Collections.emptyMap;

import org.apache.bifromq.baserpc.client.IRPCClient;
import org.apache.bifromq.basescheduler.IBatchCall;
import org.apache.bifromq.basescheduler.IBatchCallBuilder;
import org.apache.bifromq.basescheduler.IBatchCallBuilderFactory;
import org.apache.bifromq.dist.client.PubResult;
import org.apache.bifromq.dist.rpc.proto.DistReply;
import org.apache.bifromq.dist.rpc.proto.DistRequest;
import org.apache.bifromq.dist.rpc.proto.DistServiceGrpc;
import org.apache.bifromq.sysprops.props.DataPlaneMaxBurstLatencyMillis;
import java.time.Duration;

public class BatchPubCallBuilderFactory implements IBatchCallBuilderFactory<PubRequest, PubResult, PubCallBatcherKey> {
    private final IRPCClient rpcClient;
    private final long retryTimeoutNanos;

    public BatchPubCallBuilderFactory(IRPCClient rpcClient) {
        this.rpcClient = rpcClient;
        this.retryTimeoutNanos = Duration.ofMillis(DataPlaneMaxBurstLatencyMillis.INSTANCE.get()).toNanos();
    }

    @Override
    public IBatchCallBuilder<PubRequest, PubResult, PubCallBatcherKey> newBuilder(String name,
                                                                                  PubCallBatcherKey batcherKey) {
        IRPCClient.IRequestPipeline<DistRequest, DistReply> ppln =
            rpcClient.createRequestPipeline(batcherKey.tenantId(), null, null, emptyMap(),
                DistServiceGrpc.getDistMethod());
        return new IBatchCallBuilder<>() {
            @Override
            public IBatchCall<PubRequest, PubResult, PubCallBatcherKey> newBatchCall() {
                return new BatchPubCall(ppln, retryTimeoutNanos);
            }

            @Override
            public void close() {
                ppln.close();
            }
        };
    }
}
