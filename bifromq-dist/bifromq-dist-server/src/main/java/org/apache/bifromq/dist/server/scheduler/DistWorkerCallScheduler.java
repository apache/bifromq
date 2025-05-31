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

package org.apache.bifromq.dist.server.scheduler;

import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basescheduler.BatchCallScheduler;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Scheduling dist worker calls by batching dist server calls.
 */
@Slf4j
public class DistWorkerCallScheduler
    extends BatchCallScheduler<TenantPubRequest, DistServerCallResult, DistServerCallBatcherKey>
    implements IDistWorkerCallScheduler {

    /**
     * Constructor of DistWorkerCallScheduler.
     *
     * @param distWorkerClient  the dist worker client
     */
    public DistWorkerCallScheduler(IBaseKVStoreClient distWorkerClient) {
        super((name, batcherKey) -> () -> new BatchDistServerCall(distWorkerClient, batcherKey), Long.MAX_VALUE);
    }

    @Override
    protected Optional<DistServerCallBatcherKey> find(TenantPubRequest request) {
        return Optional.of(new DistServerCallBatcherKey(request.tenantId(), request.callQueueIdx()));
    }
}
