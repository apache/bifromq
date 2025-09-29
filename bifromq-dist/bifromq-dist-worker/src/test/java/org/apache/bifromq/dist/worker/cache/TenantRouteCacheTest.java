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

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.dist.worker.schema.cache.Matchings.normalMatching;
import static org.apache.bifromq.dist.worker.schema.cache.Matchings.receiverUrl;
import static org.apache.bifromq.dist.worker.schema.cache.Matchings.unorderedGroupMatching;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Ticker;
import java.time.Duration;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.dist.worker.Comparators;
import org.apache.bifromq.dist.worker.MeterTest;
import org.apache.bifromq.dist.worker.TopicIndex;
import org.apache.bifromq.dist.worker.cache.task.AddRoutesTask;
import org.apache.bifromq.dist.worker.cache.task.RemoveRoutesTask;
import org.apache.bifromq.dist.worker.schema.cache.GroupMatching;
import org.apache.bifromq.dist.worker.schema.cache.Matching;
import org.apache.bifromq.dist.worker.schema.cache.NormalMatching;
import org.apache.bifromq.metrics.TenantMetric;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.util.TopicUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TenantRouteCacheTest extends MeterTest {
    private static final KVRangeId RANGE_ID = KVRangeIdUtil.generate();
    private static final String TENANT_ID = "tenantA";
    private static final String TOPIC = "sensor/temperature";
    private static final Duration EXPIRY = Duration.ofMinutes(1);
    private static final Duration FANOUT_CHECK = Duration.ofMillis(200);

    private final Set<TenantRouteCache> caches = ConcurrentHashMap.newKeySet();

    private ManualTicker ticker;
    private ITenantRouteMatcher matcher;
    private IEventCollector eventCollector;
    private ISettingProvider settingProvider;
    private ExecutorService executorToShutdown;

    @BeforeMethod
    public void setUp() {
        super.setup();
        ticker = new ManualTicker();
        matcher = mock(ITenantRouteMatcher.class);
        settingProvider = mock(ISettingProvider.class);
        eventCollector = mock(IEventCollector.class);
        executorToShutdown = null;
    }

    @AfterMethod
    public void tearDown() {
        caches.forEach(TenantRouteCache::destroy);
        caches.clear();
        if (executorToShutdown != null) {
            executorToShutdown.shutdownNow();
            executorToShutdown = null;
        }
        super.tearDown();
    }

    @Test
    public void shouldLoadAndIndexRoutesOnFirstAccess() {
        TenantRouteCache cache = newCache(directExecutor());
        NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(10, 5, Set.of(existing));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);
        when(matcher.matchAll(eq(Set.of(TOPIC)), eq(10), eq(5))).thenReturn(Map.of(TOPIC, matchedRoutes));

        Set<Matching> result = cache.getMatch(TOPIC, FULL_BOUNDARY).join();

        assertEquals(result, Set.of(existing));
        assertTrue(cache.isCached(TopicUtil.from(TOPIC).getFilterLevelList()));
        verify(matcher, times(1)).matchAll(eq(Set.of(TOPIC)), eq(10), eq(5));
    }

    @Test
    public void shouldReuseCachedValueWithoutReload() {
        TenantRouteCache cache = newCache(directExecutor());
        NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(10, 5, Set.of(existing));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);
        when(matcher.matchAll(eq(Set.of(TOPIC)), eq(10), eq(5))).thenReturn(Map.of(TOPIC, matchedRoutes));

        Set<Matching> first = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
        Set<Matching> second = cache.getMatch(TOPIC, FULL_BOUNDARY).join();

        assertEquals(first, Set.of(existing));
        assertEquals(second, Set.of(existing));
        verify(matcher, times(1)).matchAll(eq(Set.of(TOPIC)), eq(10), eq(5));
    }

    @Test
    public void shouldReloadWhensReloadReturned() {
        TenantRouteCache cache = newCache(directExecutor());
        NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        NormalMatching extra = normalMatching(TENANT_ID, TOPIC, 1, "receiverB", "delivererB", 2);
        IMatchedRoutes initial = mockRoutesWithBackingSet(1, 5, Set.of(existing));
        IMatchedRoutes reloaded = mockRoutesWithBackingSet(2, 5, Set.of(existing, extra));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(1, 2);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5, 5);
        when(matcher.matchAll(eq(Set.of(TOPIC)), eq(1), eq(5))).thenReturn(Map.of(TOPIC, initial));
        when(matcher.matchAll(eq(Set.of(TOPIC)), eq(2), eq(5))).thenReturn(Map.of(TOPIC, reloaded));

        Set<Matching> first = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
        assertEquals(first, Set.of(existing));

        ticker.advance(FANOUT_CHECK.plusMillis(1));
        await().atMost(Duration.ofSeconds(5)).until(() -> cache.getMatch(TOPIC, FULL_BOUNDARY).join().contains(extra));

        verify(matcher, times(2)).matchAll(eq(Set.of(TOPIC)), anyInt(), anyInt());
    }

    @Test
    public void shouldNotReloadWhenAdjustedReturned() {
        TenantRouteCache cache = newCache(directExecutor());
        NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(10, 5, Set.of(existing));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10, 20);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5, 5);
        when(matcher.matchAll(eq(Set.of(TOPIC)), eq(10), eq(5))).thenReturn(Map.of(TOPIC, matchedRoutes));

        Set<Matching> first = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
        assertEquals(first, Set.of(existing));

        ticker.advance(FANOUT_CHECK.plusMillis(1));
        Set<Matching> second = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
        assertEquals(second, Set.of(existing));

        verify(matcher, times(1)).matchAll(eq(Set.of(TOPIC)), anyInt(), anyInt());
    }

    @Test
    public void shouldApplyAddRoutesTask() {
        NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(10, 5, Set.of(existing));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);
        when(matcher.matchAll(eq(Set.of(TOPIC)), eq(10), eq(5))).thenReturn(Map.of(TOPIC, matchedRoutes));

        TenantRouteCache cache = newCache(directExecutor());

        assertTrue(cache.getMatch(TOPIC, FULL_BOUNDARY).join().contains(existing));

        NormalMatching newNormal = normalMatching(TENANT_ID, TOPIC, 1, "receiverB", "delivererB", 2);
        GroupMatching newGroup = unorderedGroupMatching(TENANT_ID, "sensor/#", "groupA",
            Map.of(receiverUrl(1, "receiverC", "delivererC"), 1L));

        NavigableMap<RouteMatcher, Set<Matching>> additions = new TreeMap<>(Comparators.RouteMatcherComparator);
        additions.put(newNormal.matcher, Set.of(newNormal));
        additions.put(newGroup.matcher, Set.of(newGroup));

        cache.refresh(AddRoutesTask.of(additions));

        await().atMost(Duration.ofSeconds(5)).until(() -> {
            Set<Matching> current = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
            return current.contains(newNormal) && current.contains(newGroup);
        });
    }

    @Test
    public void shouldApplyRemoveRoutesTask() {
        TenantRouteCache cache = newCache(directExecutor());
        NormalMatching normal = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        GroupMatching removableGroup = unorderedGroupMatching(TENANT_ID, "sensor/#", "groupRemove",
            Map.of(receiverUrl(1, "receiverB", "delivererB"), 1L));
        GroupMatching updatableGroup = unorderedGroupMatching(TENANT_ID, "sensor/+", "groupUpdate",
            Map.of(receiverUrl(1, "receiverC", "delivererC"), 1L,
                receiverUrl(1, "receiverD", "delivererD"), 1L));
        IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(10, 5, Set.of(normal, removableGroup, updatableGroup));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);
        when(matcher.matchAll(eq(Set.of(TOPIC)), eq(10), eq(5))).thenReturn(Map.of(TOPIC, matchedRoutes));

        cache.getMatch(TOPIC, FULL_BOUNDARY).join();

        GroupMatching emptyGroup = unorderedGroupMatching(TENANT_ID, "sensor/#", "groupRemove", Map.of());
        GroupMatching reducedGroup = unorderedGroupMatching(TENANT_ID, "sensor/+",
            "groupUpdate", Map.of(receiverUrl(1, "receiverC", "delivererC"), 1L));

        NavigableMap<RouteMatcher, Set<Matching>> removals = new TreeMap<>(Comparators.RouteMatcherComparator);
        removals.put(normal.matcher, Set.of(normal));
        removals.put(removableGroup.matcher, Set.of(emptyGroup));
        removals.put(updatableGroup.matcher, Set.of(reducedGroup));

        cache.refresh(RemoveRoutesTask.of(removals));

        await().atMost(Duration.ofSeconds(5)).until(() -> {
            Set<Matching> current = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
            boolean normalRemoved = !current.contains(normal);
            boolean groupRemoved = !current.contains(removableGroup);
            boolean groupUpdated = current.contains(reducedGroup)
                && current.stream().noneMatch(m -> m instanceof GroupMatching gm
                && gm.matcher.equals(updatableGroup.matcher)
                && gm.receivers().equals(updatableGroup.receivers()));
            return normalRemoved && groupRemoved && groupUpdated;
        });
    }

    @Test
    public void shouldQueueTasksUntilLoadCompletes() throws Exception {
        executorToShutdown = Executors.newSingleThreadExecutor();
        TenantRouteCache cache = newCache(executorToShutdown);
        NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(10, 5, Set.of(existing));

        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch allowLoad = new CountDownLatch(1);

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);
        when(matcher.matchAll(eq(Set.of(TOPIC)), eq(10), eq(5))).thenAnswer(invocation -> {
            loadStarted.countDown();
            allowLoad.await(5, TimeUnit.SECONDS);
            return Map.of(TOPIC, matchedRoutes);
        });

        CompletableFuture<Set<Matching>> future = cache.getMatch(TOPIC, FULL_BOUNDARY);
        assertFalse(future.isDone());
        assertTrue(loadStarted.await(5, TimeUnit.SECONDS));

        NormalMatching newNormal = normalMatching(TENANT_ID, TOPIC, 1, "receiverB", "delivererB", 2);
        NavigableMap<RouteMatcher, Set<Matching>> additions = new TreeMap<>(Comparators.RouteMatcherComparator);
        additions.put(newNormal.matcher, Set.of(newNormal));
        cache.refresh(AddRoutesTask.of(additions));

        assertFalse(future.isDone());
        allowLoad.countDown();

        Set<Matching> initial = future.join();
        assertTrue(initial.contains(existing));

        await().atMost(Duration.ofSeconds(5))
            .until(() -> cache.getMatch(TOPIC, FULL_BOUNDARY).join().contains(newNormal));
    }

    @Test
    public void shouldDestroyMeters() {
        TenantRouteCache cache = new TenantRouteCache(RANGE_ID, TENANT_ID, matcher, settingProvider, EXPIRY,
            FANOUT_CHECK,
            directExecutor());
        caches.add(cache);
        assertGauge(TENANT_ID, TenantMetric.MqttRouteCacheSize);

        cache.destroy();
        assertNoGauge(TENANT_ID, TenantMetric.MqttRouteCacheSize);
    }

    @Test
    public void shouldClampPersistentFanoutOnDecreaseNoReload() {
        TenantRouteCache cache = newCache(directExecutor());

        NormalMatching p1 = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        NormalMatching p2 = normalMatching(TENANT_ID, TOPIC, 1, "receiverB", "delivererB", 2);
        IMatchedRoutes initial = mockRoutesWithBackingSet(2, 5, Set.of(p1, p2));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(2, 1);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5, 5);
        when(matcher.matchAll(eq(Set.of(TOPIC)), anyInt(), anyInt())).thenReturn(Map.of(TOPIC, initial));

        Set<Matching> first = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
        // Start with 2 persistent routes
        long persistentCount = first.stream().filter(m -> m instanceof NormalMatching nm && nm.subBrokerId() == 1)
            .count();
        assertEquals(persistentCount, 2);

        ticker.advance(FANOUT_CHECK.plusMillis(1));
        Set<Matching> afterClamp = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
        long clampedCount = afterClamp.stream().filter(m -> m instanceof NormalMatching nm && nm.subBrokerId() == 1)
            .count();
        assertEquals(clampedCount, 1);

        // No reload triggered
        verify(matcher, times(1)).matchAll(any(), anyInt(), anyInt());
    }

    @Test
    public void shouldClampGroupFanoutOnDecreaseNoReload() {
        TenantRouteCache cache = newCache(directExecutor());

        GroupMatching g1 = unorderedGroupMatching(TENANT_ID, "sensor/#", "g1",
            Map.of(receiverUrl(1, "r1", "d1"), 1L));
        GroupMatching g2 = unorderedGroupMatching(TENANT_ID, "sensor/#", "g2",
            Map.of(receiverUrl(1, "r2", "d2"), 1L));
        IMatchedRoutes initial = mockRoutesWithBackingSet(10, 2, Set.of(g1, g2));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10, 10);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(2, 1);
        when(matcher.matchAll(eq(Set.of(TOPIC)), anyInt(), anyInt())).thenReturn(Map.of(TOPIC, initial));

        Set<Matching> first = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
        long groupCount = first.stream().filter(m -> m instanceof GroupMatching).count();
        assertEquals(groupCount, 2);

        ticker.advance(FANOUT_CHECK.plusMillis(1));
        Set<Matching> afterClamp = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
        long clampedCount = afterClamp.stream().filter(m -> m instanceof GroupMatching).count();
        assertEquals(clampedCount, 1);

        verify(matcher, times(1)).matchAll(any(), anyInt(), anyInt());
    }

    @Test
    public void shouldBoundZeroRouteTopicsByMaxWeight() {
        // Configure a small max cached routes to validate bounding behavior
        String propKey = org.apache.bifromq.sysprops.props.DistMaxCachedRoutesPerTenant.INSTANCE.propKey();
        String original = System.getProperty(propKey);
        try {
            System.setProperty(propKey, String.valueOf(10));
            org.apache.bifromq.sysprops.props.DistMaxCachedRoutesPerTenant.INSTANCE.resolve();

            TenantRouteCache cache = newCache(directExecutor());

            // All topics have no matching routes (weight should be at least 1 after fix)
            IMatchedRoutes emptyRoutes = mockRoutesWithBackingSet(10, 10, Set.of());
            when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
            when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(10);
            when(matcher.matchAll(any(), anyInt(), anyInt())).thenAnswer(invocation -> {
                @SuppressWarnings("unchecked")
                Set<String> topics = (Set<String>) invocation.getArgument(0);
                String t = topics.iterator().next();
                return Map.of(t, emptyRoutes);
            });

            // Access many distinct topics rapidly; cache should stay bounded by max weight
            int total = 100;
            for (int i = 0; i < total; i++) {
                String topic = "sensor/t" + i;
                cache.getMatch(topic, FULL_BOUNDARY).join();
            }

            // Inspect internal cache size and index coverage
            var routesCacheField = TenantRouteCache.class.getDeclaredField("routesCache");
            routesCacheField.setAccessible(true);
            @SuppressWarnings("unchecked")
            com.github.benmanes.caffeine.cache.AsyncLoadingCache<RouteCacheKey, IMatchedRoutes> routesCache =
                (com.github.benmanes.caffeine.cache.AsyncLoadingCache<RouteCacheKey, IMatchedRoutes>)
                    routesCacheField.get(cache);
            long cached = routesCache.synchronous().estimatedSize();
            // With weight=1 per entry, estimated size should be bounded by 10 (allow small overhead for race) 
            assertTrue(cached <= 12, "Cache size should be bounded by max weight");

            var indexField = TenantRouteCache.class.getDeclaredField("index");
            indexField.setAccessible(true);
            @SuppressWarnings("unchecked")
            org.apache.bifromq.dist.worker.TopicIndex<RouteCacheKey> index =
                (org.apache.bifromq.dist.worker.TopicIndex<RouteCacheKey>) indexField.get(cache);

            Set<RouteCacheKey> indexed = index.match("#");
            assertTrue(indexed.size() <= 12, "Index entries should be bounded along with cache");
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (original == null) {
                System.clearProperty(propKey);
            } else {
                System.setProperty(propKey, original);
            }
            org.apache.bifromq.sysprops.props.DistMaxCachedRoutesPerTenant.INSTANCE.resolve();
        }
    }

    @Test
    public void shouldCleanupIndexOnExpiryAndExplicitInvalidation() throws Exception {
        TenantRouteCache cache = newCache(directExecutor());

        NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(10, 5, Set.of(existing));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);
        when(matcher.matchAll(eq(Set.of(TOPIC)), eq(10), eq(5))).thenReturn(Map.of(TOPIC, matchedRoutes));

        // 1) Initial load adds key into index
        assertTrue(cache.getMatch(TOPIC, FULL_BOUNDARY).join().contains(existing));

        // Reflect to access private fields: routesCache and index
        var routesCacheField = TenantRouteCache.class.getDeclaredField("routesCache");
        routesCacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        AsyncLoadingCache<RouteCacheKey, IMatchedRoutes> routesCache = (AsyncLoadingCache<RouteCacheKey, IMatchedRoutes>)
            routesCacheField.get(cache);

        var indexField = TenantRouteCache.class.getDeclaredField("index");
        indexField.setAccessible(true);
        @SuppressWarnings("unchecked")
        TopicIndex<RouteCacheKey> index = (TopicIndex<RouteCacheKey>) indexField.get(cache);

        // Ensure index has exactly one key for the topic
        Set<RouteCacheKey> initialKeys = index.get(TOPIC);
        assertEquals(initialKeys.size(), 1);
        RouteCacheKey firstKey = initialKeys.iterator().next();

        // 2) Expire by advancing ticker and forcing cleanup
        ticker.advance(EXPIRY.plusMillis(1));
        routesCache.synchronous().cleanUp();

        // After expiry cleanup, the old key should be removed from both cache and index
        assertFalse(routesCache.synchronous().asMap().containsKey(firstKey));
        assertFalse(index.get(TOPIC).contains(firstKey));

        // 3) Load again to have a fresh key, then explicitly invalidate it
        cache.getMatch(TOPIC, FULL_BOUNDARY).join();
        Set<RouteCacheKey> afterReloadKeys = index.get(TOPIC);
        assertEquals(afterReloadKeys.size(), 1);
        RouteCacheKey secondKey = afterReloadKeys.iterator().next();

        // Explicit invalidation should trigger removalListener and clean index
        routesCache.synchronous().invalidate(secondKey);

        // Ensure explicit invalidation cleaned the index entry
        assertFalse(index.get(TOPIC).contains(secondKey));
    }

    private TenantRouteCache newCache(Executor executor) {
        TenantRouteCache cache = new TenantRouteCache(RANGE_ID, TENANT_ID, matcher, settingProvider,
            EXPIRY, FANOUT_CHECK, ticker, executor);
        caches.add(cache);
        return cache;
    }

    private IMatchedRoutes mockRoutesWithBackingSet(int maxPersistentFanout, int maxGroupFanout, Set<Matching> seed) {
        MatchedRoutes routes = new MatchedRoutes(TENANT_ID, TOPIC, eventCollector, maxPersistentFanout, maxGroupFanout);
        for (Matching m : seed) {
            if (m instanceof NormalMatching nm) {
                routes.addNormalMatching(nm);
            } else if (m instanceof GroupMatching gm) {
                routes.putGroupMatching(gm);
            }
        }
        return routes;
    }

    private static final class ManualTicker implements Ticker {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long read() {
            return nanos.get();
        }

        void advance(Duration duration) {
            nanos.addAndGet(duration.toNanos());
        }
    }
}
