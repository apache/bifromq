/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

public abstract class InboxQueryScheduler<Req, Resp>
    extends BatchCallScheduler<Req, Resp, InboxQueryScheduler.BatchKey> {
    protected final IBaseKVStoreClient kvStoreClient;
    private final int queuesPerRange;

    public InboxQueryScheduler(int queuesPerRange, IBaseKVStoreClient kvStoreClient, String name) {
        super(name);
        Preconditions.checkArgument(queuesPerRange > 0, "Queues per range must be positive");
        this.kvStoreClient = kvStoreClient;
        this.queuesPerRange = queuesPerRange;
    }

    @Override
    protected final Optional<BatchKey> find(Req request) {
        Optional<KVRangeSetting> range = kvStoreClient.findByKey(rangeKey(request));
        return range.map(kvRangeSetting -> new BatchKey(kvRangeSetting, selectQueue(queuesPerRange, request)));
    }

    protected abstract int selectQueue(int maxQueueIdx, Req request);

    protected abstract ByteString rangeKey(Req request);

    @AllArgsConstructor
    @EqualsAndHashCode
    static class BatchKey {
        final KVRangeSetting rangeSetting;
        final int queueId;
    }
}
