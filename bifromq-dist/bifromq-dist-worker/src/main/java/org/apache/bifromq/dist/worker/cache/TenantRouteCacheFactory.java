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

package org.apache.bifromq.dist.worker.cache;

import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

class TenantRouteCacheFactory implements ITenantRouteCacheFactory {
    private final Executor matchExecutor;
    private final ThreadLocalKVReader threadLocalReader;
    private final Timer internalMatchTimer;
    private final Duration expiry;

    public TenantRouteCacheFactory(Supplier<IKVCloseableReader> readerSupplier,
                                   Duration expiry,
                                   Executor matchExecutor,
                                   String... tags) {
        this.matchExecutor = matchExecutor;
        this.threadLocalReader = new ThreadLocalKVReader(readerSupplier);
        this.expiry = expiry;
        internalMatchTimer = Timer.builder("dist.match.internal")
            .tags(tags)
            .register(Metrics.globalRegistry);
    }


    @Override
    public Duration expiry() {
        return expiry;
    }

    @Override
    public ITenantRouteCache create(String tenantId) {
        return new TenantRouteCache(tenantId,
            new TenantRouteMatcher(tenantId, threadLocalReader, internalMatchTimer), expiry, matchExecutor);
    }

    @Override
    public void close() {
        threadLocalReader.close();
        Metrics.globalRegistry.remove(internalMatchTimer);
    }
}
