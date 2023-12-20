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
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_BURST_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_TOLERABLE_LATENCY_MS;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcher;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.google.protobuf.ByteString;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxCommitScheduler extends MutationCallScheduler<CommitRequest, CommitReply>
    implements IInboxCommitScheduler {
    public InboxCommitScheduler(IBaseKVStoreClient inboxStoreClient) {
        super("inbox_server_commit", inboxStoreClient, Duration.ofMillis(DATA_PLANE_TOLERABLE_LATENCY_MS.get()),
            Duration.ofMillis(DATA_PLANE_BURST_LATENCY_MS.get()));

    }

    @Override
    protected Batcher<CommitRequest, CommitReply, MutationCallBatcherKey> newBatcher(String name,
                                                                                     long tolerableLatencyNanos,
                                                                                     long burstLatencyNanos,
                                                                                     MutationCallBatcherKey batchKey) {
        return new InboxCommitBatcher(name, tolerableLatencyNanos, burstLatencyNanos, batchKey, storeClient);
    }

    @Override
    protected ByteString rangeKey(CommitRequest request) {
        return scopedInboxId(request.getTenantId(), request.getInboxId());
    }

    private static class InboxCommitBatcher extends MutationCallBatcher<CommitRequest, CommitReply> {
        InboxCommitBatcher(String name,
                           long tolerableLatencyNanos,
                           long burstLatencyNanos,
                           MutationCallBatcherKey batchKey,
                           IBaseKVStoreClient inboxStoreClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, batchKey, inboxStoreClient);
        }

        @Override
        protected IBatchCall<CommitRequest, CommitReply, MutationCallBatcherKey> newBatch() {
            return new BatchCommitCall(batcherKey.id, storeClient, Duration.ofMinutes(5));
        }
    }
}
