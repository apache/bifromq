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

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import com.github.benmanes.caffeine.cache.Weigher;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.dist.worker.TopicIndex;
import org.apache.bifromq.dist.worker.cache.task.AddRoutesTask;
import org.apache.bifromq.dist.worker.cache.task.LoadEntryTask;
import org.apache.bifromq.dist.worker.cache.task.RefreshEntriesTask;
import org.apache.bifromq.dist.worker.cache.task.ReloadEntryTask;
import org.apache.bifromq.dist.worker.cache.task.RemoveRoutesTask;
import org.apache.bifromq.dist.worker.cache.task.TenantRouteCacheTask;
import org.apache.bifromq.dist.worker.schema.cache.GroupMatching;
import org.apache.bifromq.dist.worker.schema.cache.Matching;
import org.apache.bifromq.dist.worker.schema.cache.NormalMatching;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.metrics.TenantMetric;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.sysprops.props.DistMaxCachedRoutesPerTenant;
import org.apache.bifromq.type.RouteMatcher;
import org.checkerframework.checker.index.qual.NonNegative;
import org.jspecify.annotations.NonNull;

@Slf4j
class TenantRouteCache implements ITenantRouteCache {
    private final String tenantId;
    private final ISettingProvider settingProvider;
    private final ITenantRouteMatcher matcher;
    private final AsyncLoadingCache<RouteCacheKey, IMatchedRoutes> routesCache;
    private final TopicIndex<RouteCacheKey> index;
    private final Executor matchExecutor;
    private final ConcurrentLinkedDeque<TenantRouteCacheTask> tasks = new ConcurrentLinkedDeque<>();
    private final AtomicBoolean taskRunning = new AtomicBoolean(false);
    private final String[] tags;

    TenantRouteCache(KVRangeId rangeId,
                     String tenantId,
                     ITenantRouteMatcher matcher,
                     ISettingProvider settingProvider,
                     Duration expiryAfterAccess,
                     Duration fanoutCheckInterval,
                     Executor matchExecutor) {
        this(rangeId, tenantId, matcher, settingProvider, expiryAfterAccess, fanoutCheckInterval, Ticker.systemTicker(),
            matchExecutor);
    }

    TenantRouteCache(KVRangeId rangeId,
                     String tenantId,
                     ITenantRouteMatcher matcher,
                     ISettingProvider settingProvider,
                     Duration expiryAfterAccess,
                     Duration fanoutCheckInterval,
                     Ticker ticker,
                     Executor matchExecutor) {
        this.tenantId = tenantId;
        this.matcher = matcher;
        this.matchExecutor = matchExecutor;
        this.settingProvider = settingProvider;
        index = new TopicIndex<>();
        routesCache = Caffeine.newBuilder()
            .scheduler(Scheduler.systemScheduler())
            .ticker(ticker)
            .executor(matchExecutor)
            .maximumWeight(DistMaxCachedRoutesPerTenant.INSTANCE.get())
            .weigher(new Weigher<RouteCacheKey, IMatchedRoutes>() {
                @Override
                public @NonNegative int weigh(RouteCacheKey key, IMatchedRoutes value) {
                    return Math.max(1, value.routes().size());
                }
            })
            .expireAfterAccess(expiryAfterAccess)
            .refreshAfterWrite(fanoutCheckInterval)
            .removalListener(
                (RouteCacheKey key, IMatchedRoutes value, RemovalCause cause) -> index.remove(key.topic, key))
            .recordStats()
            .buildAsync(new AsyncCacheLoader<>() {
                @Override
                public CompletableFuture<IMatchedRoutes> asyncLoad(RouteCacheKey key, Executor executor) {
                    LoadEntryTask task = LoadEntryTask.of(key.topic, key);
                    submitCacheTask(task);
                    return task.future;
                }

                @Override
                public CompletableFuture<IMatchedRoutes> asyncReload(RouteCacheKey key,
                                                                     @NonNull IMatchedRoutes oldValue,
                                                                     Executor executor) {
                    int maxPersistentFanouts = settingProvider.provide(MaxPersistentFanout, tenantId);
                    int maxGroupFanouts = settingProvider.provide(MaxGroupFanout, tenantId);
                    if (oldValue.adjust(maxPersistentFanouts, maxGroupFanouts)
                        == IMatchedRoutes.AdjustResult.ReloadNeeded) {
                        ReloadEntryTask task = ReloadEntryTask.of(key.topic, key, maxPersistentFanouts,
                            maxGroupFanouts);
                        submitCacheTask(task);
                        return task.future;
                    }
                    return CompletableFuture.completedFuture(oldValue);
                }
            });
        tags = new String[] {"id", KVRangeIdUtil.toString(rangeId)};
        ITenantMeter.counting(tenantId, TenantMetric.MqttRouteCacheHitCount, routesCache.synchronous(),
            cache -> cache.stats().hitCount(), tags);
        ITenantMeter.counting(tenantId, TenantMetric.MqttRouteCacheMissCount, routesCache.synchronous(),
            cache -> cache.stats().missCount(), tags);
        ITenantMeter.counting(tenantId, TenantMetric.MqttRouteCacheEvictCount, routesCache.synchronous(),
            cache -> cache.stats().evictionCount(), tags);
        ITenantMeter.gauging(tenantId, TenantMetric.MqttRouteCacheSize, routesCache.synchronous()::estimatedSize, tags);
    }

    @Override
    public boolean isCached(List<String> filterLevels) {
        return !index.match(filterLevels).isEmpty();
    }

    @Override
    public void refresh(RefreshEntriesTask task) {
        // Enqueue and schedule an async refresh task; ensure only one runner active
        submitCacheTask(task);
    }

    private void submitCacheTask(TenantRouteCacheTask task) {
        tasks.add(task);
        tryScheduleTask();
    }

    private void tryScheduleTask() {
        if (taskRunning.compareAndSet(false, true)) {
            matchExecutor.execute(this::runTaskLoop);
        }
    }

    private void runTaskLoop() {
        TenantRouteCacheTask task;
        List<CompletableFuture<Void>> loadFutures = new ArrayList<>(tasks.size());
        int maxPersistentFanouts = settingProvider.provide(MaxPersistentFanout, tenantId);
        int maxGroupFanouts = settingProvider.provide(MaxGroupFanout, tenantId);
        outer:
        while ((task = tasks.poll()) != null) {
            switch (task.type()) {
                case Load -> {
                    LoadEntryTask loadEntryTask = (LoadEntryTask) task;
                    loadFutures.add(CompletableFuture.runAsync(() -> {
                        String topic = loadEntryTask.topic;
                        RouteCacheKey cacheKey = loadEntryTask.cacheKey;
                        Map<String, IMatchedRoutes> results = matcher.matchAll(singleton(topic),
                            maxPersistentFanouts, maxGroupFanouts);
                        IMatchedRoutes matchedRoutes = results.get(topic);
                        // update reference
                        cacheKey.cachedMatchedRoutes.set(matchedRoutes);
                        // sync index
                        index.add(cacheKey.topic, cacheKey);
                        loadEntryTask.future.complete(matchedRoutes);
                    }, matchExecutor));
                }
                case Reload -> {
                    ReloadEntryTask reloadEntryTask = (ReloadEntryTask) task;
                    loadFutures.add(CompletableFuture.runAsync(() -> {
                        String topic = reloadEntryTask.topic;
                        RouteCacheKey cacheKey = reloadEntryTask.cacheKey;
                        Map<String, IMatchedRoutes> results = matcher.matchAll(singleton(topic),
                            reloadEntryTask.maxPersistentFanouts, reloadEntryTask.maxGroupFanouts);
                        IMatchedRoutes matchedRoutes = results.get(topic);
                        // update reference
                        cacheKey.cachedMatchedRoutes.set(matchedRoutes);
                        reloadEntryTask.future.complete(matchedRoutes);
                    }, matchExecutor));
                }
                case AddRoutes, RemoveRoutes -> {
                    if (!loadFutures.isEmpty()) {
                        tasks.addFirst(task);
                        break outer;
                    } else {
                        switch (task.type()) {
                            case AddRoutes -> {
                                AddRoutesTask addTask = (AddRoutesTask) task;
                                for (RouteMatcher topicFilter : addTask.routes.keySet()) {
                                    Set<Matching> newMatchings = addTask.routes.get(topicFilter);
                                    List<String> filterLevels = topicFilter.getFilterLevelList();
                                    Set<RouteCacheKey> keys = index.match(filterLevels);
                                    switch (topicFilter.getType()) {
                                        case Normal -> {
                                            for (RouteCacheKey cacheKey : keys) {
                                                for (Matching matching : newMatchings) {
                                                    cacheKey.cachedMatchedRoutes.get()
                                                        .addNormalMatching((NormalMatching) matching);
                                                }
                                            }
                                        }
                                        case OrderedShare, UnorderedShare -> {
                                            for (RouteCacheKey cacheKey : keys) {
                                                for (Matching matching : newMatchings) {
                                                    cacheKey.cachedMatchedRoutes.get()
                                                        .putGroupMatching((GroupMatching) matching);
                                                }
                                            }
                                        }
                                        default -> {
                                            // do nothing
                                        }
                                    }
                                }
                            }
                            case RemoveRoutes -> {
                                RemoveRoutesTask removeTask = (RemoveRoutesTask) task;
                                for (RouteMatcher topicFilter : removeTask.routes.keySet()) {
                                    Set<Matching> removedMatchings = removeTask.routes.get(topicFilter);
                                    List<String> filterLevels = topicFilter.getFilterLevelList();
                                    Set<RouteCacheKey> keys = index.match(filterLevels);
                                    switch (topicFilter.getType()) {
                                        case Normal -> {
                                            for (RouteCacheKey cacheKey : keys) {
                                                for (Matching matching : removedMatchings) {
                                                    cacheKey.cachedMatchedRoutes.get()
                                                        .removeNormalMatching((NormalMatching) matching);
                                                }
                                            }
                                        }
                                        case OrderedShare, UnorderedShare -> {
                                            for (RouteCacheKey cacheKey : keys) {
                                                for (Matching matching : removedMatchings) {
                                                    GroupMatching groupMatching = (GroupMatching) matching;
                                                    if (groupMatching.receivers().isEmpty()) {
                                                        cacheKey.cachedMatchedRoutes.get()
                                                            .removeGroupMatching(groupMatching);
                                                    } else {
                                                        cacheKey.cachedMatchedRoutes.get()
                                                            .putGroupMatching(groupMatching);
                                                    }
                                                }
                                            }
                                        }
                                        default -> {
                                            // do nothing
                                        }
                                    }
                                }
                            }
                            default -> {
                                // do nothing
                            }
                        }
                    }
                }
                default -> {
                    // do nothing
                }
            }
        }
        CompletableFuture.allOf(loadFutures.toArray(new CompletableFuture[0]))
            .whenComplete((v, e) -> {
                taskRunning.set(false);
                if (!tasks.isEmpty()) {
                    tryScheduleTask();
                }
            });
    }

    @Override
    public CompletableFuture<Set<Matching>> getMatch(String topic, Boundary currentTenantRange) {
        return routesCache.get(new RouteCacheKey(topic, currentTenantRange)).thenApply(IMatchedRoutes::routes);
    }

    @Override
    public void destroy() {
        routesCache.synchronous().asMap().keySet().forEach(key -> index.remove(key.topic, key));
        routesCache.synchronous().invalidateAll();
        ITenantMeter.stopCounting(tenantId, TenantMetric.MqttRouteCacheMissCount, tags);
        ITenantMeter.stopCounting(tenantId, TenantMetric.MqttRouteCacheHitCount, tags);
        ITenantMeter.stopCounting(tenantId, TenantMetric.MqttRouteCacheEvictCount, tags);
        ITenantMeter.stopGauging(tenantId, TenantMetric.MqttRouteCacheSize, tags);
    }
}
