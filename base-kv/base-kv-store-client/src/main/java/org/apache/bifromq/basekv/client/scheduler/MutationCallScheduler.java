/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.basekv.client.scheduler;

import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByKey;

import com.google.protobuf.ByteString;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basescheduler.BatchCallScheduler;

/**
 * The abstract class for base-kv mutation call scheduler.
 *
 * @param <ReqT> the type of the request
 * @param <RespT> the type of the response
 * @param <BatchCallT> the type of the batch call
 */
@Slf4j
public abstract class MutationCallScheduler<ReqT, RespT, BatchCallT extends BatchMutationCall<ReqT, RespT>>
    extends BatchCallScheduler<ReqT, RespT, MutationCallBatcherKey> {
    protected final IBaseKVStoreClient storeClient;

    public MutationCallScheduler(IBatchMutationCallBuilder<ReqT, RespT, BatchCallT> batchCallBuilder,
                                 long maxBurstLatency,
                                 IBaseKVStoreClient storeClient) {
        super(new BatchMutationCallBuilderFactory<>(storeClient, batchCallBuilder), maxBurstLatency);
        this.storeClient = storeClient;
    }

    @Override
    protected final Optional<MutationCallBatcherKey> find(ReqT call) {
        Optional<KVRangeSetting> rangeSetting = findByKey(rangeKey(call), storeClient.latestEffectiveRouter());
        return rangeSetting.map(setting -> new MutationCallBatcherKey(setting.id(), setting.leader(), setting.ver()));
    }

    protected abstract ByteString rangeKey(ReqT call);
}