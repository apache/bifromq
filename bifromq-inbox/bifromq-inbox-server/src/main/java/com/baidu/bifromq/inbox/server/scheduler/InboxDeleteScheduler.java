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

package com.baidu.bifromq.inbox.server.scheduler;

import static com.baidu.bifromq.inbox.util.KeyUtil.inboxKeyPrefix;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.CONTROL_PLANE_BURST_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.CONTROL_PLANE_TOLERABLE_LATENCY_MS;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcher;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteReply;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteRequest;
import com.google.protobuf.ByteString;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxDeleteScheduler extends MutationCallScheduler<BatchDeleteRequest.Params, BatchDeleteReply.Result>
    implements IInboxDeleteScheduler {

    public InboxDeleteScheduler(IBaseKVStoreClient inboxStoreClient) {
        super("inbox_server_attach", inboxStoreClient, Duration.ofMillis(CONTROL_PLANE_TOLERABLE_LATENCY_MS.get()),
            Duration.ofMillis(CONTROL_PLANE_BURST_LATENCY_MS.get()));
    }

    @Override
    protected Batcher<BatchDeleteRequest.Params, BatchDeleteReply.Result, MutationCallBatcherKey> newBatcher(
        String name,
        long tolerableLatencyNanos,
        long burstLatencyNanos,
        MutationCallBatcherKey range) {
        return new InboxDeleteBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, storeClient);
    }

    @Override
    protected ByteString rangeKey(BatchDeleteRequest.Params request) {
        return inboxKeyPrefix(request.getTenantId(), request.getInboxId(), request.getIncarnation());
    }

    private static class InboxDeleteBatcher
        extends MutationCallBatcher<BatchDeleteRequest.Params, BatchDeleteReply.Result> {
        private InboxDeleteBatcher(String name,
                                   long expectLatencyNanos,
                                   long maxTolerableLatencyNanos,
                                   MutationCallBatcherKey range,
                                   IBaseKVStoreClient inboxStoreClient) {
            super(name, expectLatencyNanos, maxTolerableLatencyNanos, range, inboxStoreClient);
        }

        @Override
        protected IBatchCall<BatchDeleteRequest.Params, BatchDeleteReply.Result, MutationCallBatcherKey> newBatch() {
            return new BatchDeleteCall(batcherKey.id, storeClient, Duration.ofMinutes(5));
        }
    }
}
