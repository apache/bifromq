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

import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basescheduler.BatchCallScheduler;
import com.google.protobuf.ByteString;
import java.util.Optional;

/**
 * The abstract class for base-kv query call scheduler.
 *
 * @param <ReqT> the type of the request
 * @param <RespT> the type of the response
 * @param <BatchCallT> the type of the batch call
 */
public abstract class QueryCallScheduler<ReqT, RespT, BatchCallT extends BatchQueryCall<ReqT, RespT>>
    extends BatchCallScheduler<ReqT, RespT, QueryCallBatcherKey> {
    protected final IBaseKVStoreClient storeClient;

    public QueryCallScheduler(IBatchQueryCallBuilder<ReqT, RespT, BatchCallT> batchCallBuilder,
                              long maxBurstLatency,
                              IBaseKVStoreClient storeClient) {
        super(new BatchQueryCallBuilderFactory<>(storeClient, batchCallBuilder), maxBurstLatency);
        this.storeClient = storeClient;
    }

    protected String selectStore(KVRangeSetting setting, ReqT request) {
        return setting.leader;
    }

    protected abstract int selectQueue(ReqT request);

    protected abstract boolean isLinearizable(ReqT request);

    protected abstract ByteString rangeKey(ReqT request);

    @Override
    protected final Optional<QueryCallBatcherKey> find(ReqT req) {
        return findByKey(rangeKey(req), storeClient.latestEffectiveRouter())
            .map(range -> new QueryCallBatcherKey(range.id, selectStore(range, req),
                selectQueue(req), range.ver, isLinearizable(req)));
    }
}
