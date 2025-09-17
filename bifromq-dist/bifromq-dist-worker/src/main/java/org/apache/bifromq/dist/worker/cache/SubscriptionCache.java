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

import static org.apache.bifromq.basekv.utils.BoundaryUtil.intersect;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.dist.worker.cache.SubscriptionCache.TenantKey.noRefreshExpiry;
import static org.apache.bifromq.dist.worker.cache.SubscriptionCache.TenantKey.refreshExpiry;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.tenantBeginKey;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.dist.worker.cache.task.RefreshEntriesTask;
import org.apache.bifromq.dist.worker.schema.Matching;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.sysprops.props.DistCachedRoutesFanoutCheckIntervalSeconds;
import org.apache.bifromq.sysprops.props.DistTopicMatchExpirySeconds;

/**
 * Cache for subscription matching.
 */
@Slf4j
public class SubscriptionCache implements ISubscriptionCache {
    private final ITenantRouteCacheFactory tenantRouteCacheFactory;
    private final LoadingCache<TenantKey, ITenantRouteCache> tenantCache;
    private volatile Boundary boundary;

    public SubscriptionCache(KVRangeId id,
                             Supplier<IKVCloseableReader> rangeReaderProvider,
                             ISettingProvider settingProvider,
                             IEventCollector eventCollector,
                             Executor matchExecutor) {
        this(id, new TenantRouteCacheFactory(rangeReaderProvider, settingProvider, eventCollector,
            Duration.ofSeconds(DistTopicMatchExpirySeconds.INSTANCE.get()),
            Duration.ofSeconds(DistCachedRoutesFanoutCheckIntervalSeconds.INSTANCE.get()),
            matchExecutor, "id", KVRangeIdUtil.toString(id)), Ticker.systemTicker());
    }

    public SubscriptionCache(KVRangeId id,
                             ITenantRouteCacheFactory tenantRouteCacheFactory,
                             Ticker ticker) {
        this.tenantRouteCacheFactory = tenantRouteCacheFactory;
        long expiryNanos = tenantRouteCacheFactory.expiry().multipliedBy(2).toNanos();
        tenantCache = Caffeine.newBuilder()
            .ticker(ticker)
            .expireAfter(new Expiry<TenantKey, ITenantRouteCache>() {
                @Override
                public long expireAfterCreate(TenantKey key, ITenantRouteCache value, long currentTime) {
                    return expiryNanos;
                }

                @Override
                public long expireAfterUpdate(TenantKey key, ITenantRouteCache value, long currentTime,
                                              long currentDuration) {
                    return expiryNanos;
                }

                @Override
                public long expireAfterRead(TenantKey key, ITenantRouteCache value, long currentTime,
                                            long currentDuration) {
                    return key.refreshExpiry ? expiryNanos : currentDuration;
                }
            })
            .scheduler(Scheduler.systemScheduler())
            .evictionListener((key, cachedTenantRoutes, cause) -> {
                if (cachedTenantRoutes != null) {
                    cachedTenantRoutes.destroy();
                }
            })
            .build(k -> tenantRouteCacheFactory.create(k.tenantId));
    }

    @Override
    public CompletableFuture<Set<Matching>> get(String tenantId, String topic) {
        ITenantRouteCache routesCache = tenantCache.get(refreshExpiry(tenantId));
        ByteString tenantStartKey = tenantBeginKey(tenantId);
        Boundary tenantBoundary = intersect(toBoundary(tenantStartKey, upperBound(tenantStartKey)), boundary);
        return routesCache.getMatch(topic, tenantBoundary);
    }

    @Override
    public boolean isCached(String tenantId, List<String> filterLevels) {
        ITenantRouteCache cache = tenantCache.getIfPresent(noRefreshExpiry(tenantId));
        if (cache != null) {
            return cache.isCached(filterLevels);
        }
        return false;
    }

    @Override
    public void refresh(Map<String, RefreshEntriesTask> topicFiltersByTenant) {
        topicFiltersByTenant.forEach((tenantId, topicFilters) -> {
            ITenantRouteCache cache = tenantCache.getIfPresent(noRefreshExpiry(tenantId));
            if (cache != null) {
                cache.refresh(topicFilters);
            }
        });
    }

    @Override
    public void reset(Boundary boundary) {
        this.boundary = boundary;
    }

    @Override
    public void close() {
        tenantCache.invalidateAll();
        tenantRouteCacheFactory.close();
    }

    @EqualsAndHashCode
    static class TenantKey {
        final String tenantId;
        @EqualsAndHashCode.Exclude
        final boolean refreshExpiry;

        private TenantKey(String tenantId, boolean refreshExpiry) {
            this.tenantId = tenantId;
            this.refreshExpiry = refreshExpiry;
        }

        static TenantKey refreshExpiry(String tenantId) {
            return new TenantKey(tenantId, true);
        }

        static TenantKey noRefreshExpiry(String tenantId) {
            return new TenantKey(tenantId, false);
        }
    }
}
