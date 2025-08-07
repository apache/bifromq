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

import static java.util.Collections.singleton;
import static org.apache.bifromq.plugin.settingprovider.Setting.MaxGroupFanout;
import static org.apache.bifromq.plugin.settingprovider.Setting.MaxPersistentFanout;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import com.github.benmanes.caffeine.cache.Weigher;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.dist.worker.TopicIndex;
import org.apache.bifromq.dist.worker.schema.Matching;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.metrics.TenantMetric;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.sysprops.props.DistMaxCachedRoutesPerTenant;
import org.apache.bifromq.type.RouteMatcher;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

class TenantRouteCache implements ITenantRouteCache {
    private final String tenantId;
    private final AsyncLoadingCache<RouteCacheKey, IMatchedRoutes> routesCache;
    private final TopicIndex<RouteCacheKey> index;

    TenantRouteCache(String tenantId,
                     ITenantRouteMatcher matcher,
                     ISettingProvider settingProvider,
                     Duration expiryAfterAccess,
                     Duration fanoutCheckInterval,
                     Executor matchExecutor) {
        this(tenantId, matcher, settingProvider, expiryAfterAccess, fanoutCheckInterval, Ticker.systemTicker(),
            matchExecutor);
    }

    TenantRouteCache(String tenantId,
                     ITenantRouteMatcher matcher,
                     ISettingProvider settingProvider,
                     Duration expiryAfterAccess,
                     Duration fanoutCheckInterval,
                     Ticker ticker,
                     Executor matchExecutor) {
        this.tenantId = tenantId;
        index = new TopicIndex<>();
        routesCache = Caffeine.newBuilder()
            .scheduler(Scheduler.systemScheduler())
            .ticker(ticker)
            .executor(matchExecutor)
            .maximumWeight(DistMaxCachedRoutesPerTenant.INSTANCE.get())
            .weigher(new Weigher<RouteCacheKey, IMatchedRoutes>() {
                @Override
                public @NonNegative int weigh(RouteCacheKey key, IMatchedRoutes value) {
                    return value.routes().size();
                }
            })
            .expireAfterAccess(expiryAfterAccess)
            .refreshAfterWrite(fanoutCheckInterval)
            .evictionListener((key, value, cause) -> {
                if (key != null) {
                    index.remove(key.topic, key);
                }
            })
            .buildAsync(new CacheLoader<>() {
                @Override
                public @Nullable IMatchedRoutes load(RouteCacheKey key) {
                    int maxPersistentFanouts = settingProvider.provide(MaxPersistentFanout, tenantId);
                    int maxGroupFanouts = settingProvider.provide(MaxGroupFanout, tenantId);
                    ITenantMeter.get(tenantId).recordCount(TenantMetric.MqttRouteCacheMissCount);
                    Map<String, IMatchedRoutes> results = matcher.matchAll(singleton(key.topic),
                        maxPersistentFanouts, maxGroupFanouts);
                    index.add(key.topic, key);
                    return results.get(key.topic);
                }

                @Override
                public Map<RouteCacheKey, IMatchedRoutes> loadAll(Set<? extends RouteCacheKey> keys) {
                    ITenantMeter.get(tenantId).recordCount(TenantMetric.MqttRouteCacheMissCount, keys.size());
                    Map<String, RouteCacheKey> topicToKeyMap = new HashMap<>();
                    keys.forEach(k -> topicToKeyMap.put(k.topic(), k));
                    int maxPersistentFanouts = settingProvider.provide(MaxPersistentFanout, tenantId);
                    int maxGroupFanouts = settingProvider.provide(MaxGroupFanout, tenantId);
                    Map<String, IMatchedRoutes> resultMap = matcher.matchAll(topicToKeyMap.keySet(),
                        maxPersistentFanouts, maxGroupFanouts);
                    Map<RouteCacheKey, IMatchedRoutes> result = new HashMap<>();
                    for (Map.Entry<String, IMatchedRoutes> entry : resultMap.entrySet()) {
                        RouteCacheKey key = topicToKeyMap.get(entry.getKey());
                        result.put(key, entry.getValue());
                        index.add(key.topic, key);
                    }
                    return result;
                }

                @Override
                public @Nullable IMatchedRoutes reload(RouteCacheKey key, IMatchedRoutes oldValue) {
                    int maxPersistentFanouts = settingProvider.provide(MaxPersistentFanout, tenantId);
                    int maxGroupFanouts = settingProvider.provide(MaxGroupFanout, tenantId);
                    if (needRefresh(oldValue, maxPersistentFanouts, maxGroupFanouts)) {
                        Map<String, IMatchedRoutes> results = matcher.matchAll(singleton(key.topic),
                            maxPersistentFanouts, maxGroupFanouts);
                        return results.get(key.topic);
                    } else if (oldValue.maxPersistentFanout() != maxPersistentFanouts
                        || oldValue.maxGroupFanout() != maxGroupFanouts) {
                        return new IMatchedRoutes() {
                            @Override
                            public int maxPersistentFanout() {
                                return maxPersistentFanouts;
                            }

                            @Override
                            public int maxGroupFanout() {
                                return maxGroupFanouts;
                            }

                            @Override
                            public int persistentFanout() {
                                return oldValue.persistentFanout();
                            }

                            @Override
                            public int groupFanout() {
                                return oldValue.groupFanout();
                            }

                            @Override
                            public Set<Matching> routes() {
                                return oldValue.routes();
                            }
                        };
                    } else {
                        return oldValue;
                    }
                }
            });
        ITenantMeter.gauging(tenantId, TenantMetric.MqttRouteCacheSize, routesCache.synchronous()::estimatedSize);
    }

    private boolean needRefresh(IMatchedRoutes cachedRoutes, int maxPersistentFanouts, int maxGroupFanouts) {
        return needRefresh(maxPersistentFanouts, cachedRoutes.maxPersistentFanout(), cachedRoutes.persistentFanout())
            || needRefresh(maxGroupFanouts, cachedRoutes.maxGroupFanout(), cachedRoutes.groupFanout());
    }

    private boolean needRefresh(int currentLimit, int previousLimit, int currentFanout) {
        // current limit increases and cachedRoutes reached the previous limit, need refresh
        // current limit decreases and cachedRoutes exceeded the current limit, need refresh
        return (previousLimit < currentLimit && currentFanout == previousLimit)
            || (previousLimit > currentLimit && currentFanout > currentLimit);
    }

    @Override
    public boolean isCached(List<String> filterLevels) {
        return !index.match(filterLevels).isEmpty();
    }

    @Override
    public void refresh(NavigableSet<RouteMatcher> routeMatchers) {
        routeMatchers.forEach(topicFilter -> {
            for (RouteCacheKey cacheKey : index.match(topicFilter.getFilterLevelList())) {
                // we must invalidate the cache entry first to ensure the refresh will trigger a load
                routesCache.synchronous().invalidate(cacheKey);
                routesCache.synchronous().refresh(cacheKey);
            }
        });
    }

    @Override
    public CompletableFuture<Set<Matching>> getMatch(String topic, Boundary currentTenantRange) {
        return routesCache.get(new RouteCacheKey(topic, currentTenantRange)).thenApply(IMatchedRoutes::routes);
    }

    @Override
    public void destroy() {
        ITenantMeter.stopGauging(tenantId, TenantMetric.MqttRouteCacheSize);
    }

    private record RouteCacheKey(String topic, Boundary matchRecordBoundary) {
    }
}
