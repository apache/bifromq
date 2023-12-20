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

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.CONTROL_PLANE_BURST_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.CONTROL_PLANE_TOLERABLE_LATENCY_MS;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcher;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.inbox.rpc.proto.UnsubRequest;
import com.google.protobuf.ByteString;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxUnSubScheduler extends MutationCallScheduler<UnsubRequest, UnsubReply>
    implements IInboxUnsubScheduler {
    public InboxUnSubScheduler(IBaseKVStoreClient inboxStoreClient) {
        super("inbox_server_unsub", inboxStoreClient, Duration.ofMillis(CONTROL_PLANE_TOLERABLE_LATENCY_MS.get()),
            Duration.ofMillis(CONTROL_PLANE_BURST_LATENCY_MS.get()));

    }

    @Override
    protected Batcher<UnsubRequest, UnsubReply, MutationCallBatcherKey> newBatcher(String name,
                                                                                   long tolerableLatencyNanos,
                                                                                   long burstLatencyNanos,
                                                                                   MutationCallBatcherKey batchKey) {
        return new InboxUnSubBatcher(name, tolerableLatencyNanos, burstLatencyNanos, batchKey, storeClient);
    }

    @Override
    protected ByteString rangeKey(UnsubRequest request) {
        return scopedInboxId(request.getTenantId(), request.getInboxId());
    }

    private static class InboxUnSubBatcher extends MutationCallBatcher<UnsubRequest, UnsubReply> {
        InboxUnSubBatcher(String name,
                          long tolerableLatencyNanos,
                          long burstLatencyNanos,
                          MutationCallBatcherKey batchKey,
                          IBaseKVStoreClient inboxStoreClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, batchKey, inboxStoreClient);
        }

        @Override
        protected IBatchCall<UnsubRequest, UnsubReply, MutationCallBatcherKey> newBatch() {
            return new BatchUnsubCall(batcherKey.id, storeClient, Duration.ofMinutes(5));
        }
    }
}
