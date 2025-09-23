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

package org.apache.bifromq.dist.server.scheduler;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basescheduler.IBatchCallBuilder;
import org.apache.bifromq.basescheduler.IBatchCallBuilderFactory;

@Slf4j
public class BatchDistServerCallBuilderFactory
    implements IBatchCallBuilderFactory<TenantPubRequest, DistServerCallResult, DistServerCallBatcherKey> {
    private final IBaseKVStoreClient distWorkerClient;
    private final LoadingCache<String, TenantRangeLookupCache> tenantRangeLookupCaches;

    public BatchDistServerCallBuilderFactory(IBaseKVStoreClient distWorkerClient,
                                             long maxCachedTopics,
                                             Duration cachedRouteExpiry) {
        this.distWorkerClient = distWorkerClient;
        this.tenantRangeLookupCaches = Caffeine.newBuilder()
            .weakValues()
            .build(tenantId -> new TenantRangeLookupCache(tenantId, cachedRouteExpiry, maxCachedTopics));
    }

    @Override
    public IBatchCallBuilder<TenantPubRequest, DistServerCallResult, DistServerCallBatcherKey> newBuilder(String name,
                                                                                                          DistServerCallBatcherKey batcherKey) {
        return () -> new BatchDistServerCall(distWorkerClient, batcherKey,
            tenantRangeLookupCaches.get(batcherKey.tenantId()));
    }
}
