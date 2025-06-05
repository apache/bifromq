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

import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.IQueryPipeline;
import org.apache.bifromq.basescheduler.IBatchCall;
import org.apache.bifromq.basescheduler.IBatchCallBuilder;
import org.apache.bifromq.basescheduler.IBatchCallBuilderFactory;

final class BatchQueryCallBuilderFactory<ReqT, RespT, BatchCallT extends BatchQueryCall<ReqT, RespT>>
    implements IBatchCallBuilderFactory<ReqT, RespT, QueryCallBatcherKey> {
    private final IBaseKVStoreClient storeClient;
    private final IBatchQueryCallBuilder<ReqT, RespT, BatchCallT> batchCallBuilder;

    BatchQueryCallBuilderFactory(IBaseKVStoreClient storeClient,
                                 IBatchQueryCallBuilder<ReqT, RespT, BatchCallT> batchBuilder) {
        this.storeClient = storeClient;
        this.batchCallBuilder = batchBuilder;
    }

    @Override
    public IBatchCallBuilder<ReqT, RespT, QueryCallBatcherKey> newBuilder(String name,
                                                                          QueryCallBatcherKey batcherKey) {
        IQueryPipeline storePipeline;
        if (batcherKey.linearizable) {
            storePipeline = storeClient.createLinearizedQueryPipeline(batcherKey.storeId);
        } else {
            storePipeline = storeClient.createQueryPipeline(batcherKey.storeId);
        }

        return new IBatchCallBuilder<>() {
            @Override
            public IBatchCall<ReqT, RespT, QueryCallBatcherKey> newBatchCall() {
                return batchCallBuilder.newBatchCall(storePipeline, batcherKey);
            }

            @Override
            public void close() {
                storePipeline.close();
            }
        };
    }
}
