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

package org.apache.bifromq.dist.worker.cache.task;

import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.dist.worker.cache.IMatchedRoutes;
import org.apache.bifromq.dist.worker.cache.RouteCacheKey;

public class ReloadEntryTask extends TenantRouteCacheTask {
    public final String topic;
    public final RouteCacheKey cacheKey;
    public final int maxPersistentFanouts;
    public final int maxGroupFanouts;
    public final CompletableFuture<IMatchedRoutes> future = new CompletableFuture<>();


    private ReloadEntryTask(String topic, RouteCacheKey cacheKey, int maxPersistentFanouts, int maxGroupFanouts) {
        this.topic = topic;
        this.cacheKey = cacheKey;
        this.maxPersistentFanouts = maxPersistentFanouts;
        this.maxGroupFanouts = maxGroupFanouts;
    }

    public static ReloadEntryTask of(String topic,
                                     RouteCacheKey cacheKey,
                                     int maxPersistentFanouts,
                                     int maxGroupFanouts) {
        return new ReloadEntryTask(topic, cacheKey, maxPersistentFanouts, maxGroupFanouts);
    }

    @Override
    public CacheTaskType type() {
        return CacheTaskType.Reload;
    }
}
