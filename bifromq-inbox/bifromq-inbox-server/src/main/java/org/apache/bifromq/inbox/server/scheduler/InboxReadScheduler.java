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

package org.apache.bifromq.inbox.server.scheduler;

import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.scheduler.BatchQueryCall;
import org.apache.bifromq.basekv.client.scheduler.IBatchQueryCallBuilder;
import org.apache.bifromq.basekv.client.scheduler.QueryCallScheduler;
import org.apache.bifromq.sysprops.props.DataPlaneMaxBurstLatencyMillis;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public abstract class InboxReadScheduler<ReqT, RespT, InboxBatchQueryT extends BatchQueryCall<ReqT, RespT>>
    extends QueryCallScheduler<ReqT, RespT, InboxBatchQueryT> {
    protected final int queuesPerRange;

    public InboxReadScheduler(IBatchQueryCallBuilder<ReqT, RespT, InboxBatchQueryT> batchQueryCallBuilder,
                              int queuesPerRange, IBaseKVStoreClient inboxStoreClient) {
        super(batchQueryCallBuilder, Duration.ofSeconds(DataPlaneMaxBurstLatencyMillis.INSTANCE.get()).toNanos(),
            inboxStoreClient);
        Preconditions.checkArgument(queuesPerRange > 0, "Queues per range must be positive");
        this.queuesPerRange = queuesPerRange;
    }

    @Override
    protected int selectQueue(ReqT request) {
        return ThreadLocalRandom.current().nextInt(0, queuesPerRange);
    }
}
