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

package com.baidu.bifromq.dist.server.scheduler;

import static com.baidu.bifromq.dist.entity.EntityUtil.toMatchRecordKey;
import static com.baidu.bifromq.dist.entity.EntityUtil.toQInboxId;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.CONTROL_PLANE_BURST_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.CONTROL_PLANE_TOLERABLE_LATENCY_MS;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcher;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.dist.rpc.proto.MatchReply;
import com.baidu.bifromq.dist.rpc.proto.MatchRequest;
import com.google.protobuf.ByteString;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MatchCallScheduler extends MutationCallScheduler<MatchRequest, MatchReply>
    implements IMatchCallScheduler {

    public MatchCallScheduler(IBaseKVStoreClient distWorkerClient) {
        super("dist_server_sub_batcher",
            distWorkerClient,
            Duration.ofMillis(CONTROL_PLANE_TOLERABLE_LATENCY_MS.get()),
            Duration.ofMillis(CONTROL_PLANE_BURST_LATENCY_MS.get()));
    }

    @Override
    protected Batcher<MatchRequest, MatchReply, MutationCallBatcherKey> newBatcher(String name,
                                                                                   long tolerableLatencyNanos,
                                                                                   long burstLatencyNanos,
                                                                                   MutationCallBatcherKey batcherKey) {
        return new MatchCallBatcher(name, tolerableLatencyNanos, burstLatencyNanos, batcherKey, storeClient);
    }

    protected ByteString rangeKey(MatchRequest call) {
        String qInboxId = toQInboxId(call.getBroker(), call.getInboxId(), call.getDelivererKey());
        return toMatchRecordKey(call.getTenantId(), call.getTopicFilter(), qInboxId);
    }

    private static class MatchCallBatcher extends MutationCallBatcher<MatchRequest, MatchReply> {
        private MatchCallBatcher(String name,
                                 long tolerableLatencyNanos,
                                 long burstLatencyNanos,
                                 MutationCallBatcherKey batcherKey,
                                 IBaseKVStoreClient distWorkerClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, batcherKey, distWorkerClient);
        }

        @Override
        protected IBatchCall<MatchRequest, MatchReply, MutationCallBatcherKey> newBatch() {
            return new BatchMatchCall(batcherKey.id, storeClient, Duration.ofMinutes(5));
        }
    }
}
