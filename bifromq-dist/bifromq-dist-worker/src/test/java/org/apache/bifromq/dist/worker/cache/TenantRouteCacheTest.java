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
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import org.apache.bifromq.dist.worker.Comparators;
import org.apache.bifromq.dist.worker.MeterTest;
import org.apache.bifromq.dist.worker.schema.Matching;
import org.apache.bifromq.metrics.TenantMetric;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.util.TopicUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TenantRouteCacheTest extends MeterTest {

    private TenantRouteCache cache;
    private ManualTicker mockTicker;
    private ITenantRouteMatcher mockMatcher;
    private ISettingProvider mockSettingProvider;
    private String tenantId;
    private Duration expiryDuration;
    private Duration fanoutCheckDuration;

    @BeforeMethod
    public void setup() {
        super.setup();
        tenantId = "tenant1";
        mockTicker = new ManualTicker();
        mockMatcher = mock(ITenantRouteMatcher.class);
        mockSettingProvider = mock(ISettingProvider.class);
        expiryDuration = Duration.ofMinutes(1);
        fanoutCheckDuration = Duration.ofSeconds(2);
        cache = new TenantRouteCache(tenantId, mockMatcher, mockSettingProvider, expiryDuration,
            fanoutCheckDuration, directExecutor());
    }

    @AfterMethod
    public void tearDown() {
        cache.destroy();
        super.tearDown();
    }

    @SuppressWarnings("checkstyle:WhitespaceAround")
    @Test
    public void getMatch() {
        String topic = "home/sensor/temperature";
        when(mockSettingProvider.provide(eq(Setting.MaxPersistentFanout), eq(tenantId))).thenReturn(10);
        when(mockSettingProvider.provide(eq(Setting.MaxGroupFanout), eq(tenantId))).thenReturn(5);
        when(mockMatcher.matchAll(eq(Set.of(topic)), eq(10), eq(5)))
            .thenReturn(Map.of(topic, mockResult(1, 0, 0)));

        Set<Matching> cachedMatchings = cache.getMatch(topic, FULL_BOUNDARY).join();
        assertNotNull(cachedMatchings);
        assertEquals(cachedMatchings.size(), 1);
    }

    @Test
    public void isCached() {
        String topic = "home/sensor/temperature";
        when(mockSettingProvider.provide(eq(Setting.MaxPersistentFanout), eq(tenantId))).thenReturn(10);
        when(mockSettingProvider.provide(eq(Setting.MaxGroupFanout), eq(tenantId))).thenReturn(5);
        when(mockMatcher.matchAll(eq(Set.of(topic)), eq(10), eq(5)))
            .thenReturn(Map.of(topic, mockResult(0, 0, 0)));
        cache.getMatch(topic, FULL_BOUNDARY).join();

        assertTrue(cache.isCached(TopicUtil.from(topic).getFilterLevelList()));
    }

    @Test
    public void refreshToAddMatch() {
        String topic = "home/sensor/temperature";
        when(mockSettingProvider.provide(eq(Setting.MaxPersistentFanout), eq(tenantId))).thenReturn(10);
        when(mockSettingProvider.provide(eq(Setting.MaxGroupFanout), eq(tenantId))).thenReturn(5);
        when(mockMatcher.matchAll(eq(Set.of(topic)), eq(10), eq(5)))
            .thenReturn(
                Map.of(topic, mockResult(1, 0, 0)),
                Map.of(topic, mockResult(3, 0, 0))
            );

        Set<Matching> cachedMatchings = cache.getMatch(topic, FULL_BOUNDARY).join();
        assertEquals(cachedMatchings.size(), 1);

        NavigableSet<RouteMatcher> routeMatchers = Sets.newTreeSet(Comparators.RouteMatcherComparator);
        routeMatchers.add(TopicUtil.from("#"));
        cache.refresh(routeMatchers);

        await().until(() -> cache.getMatch(topic, FULL_BOUNDARY).join().size() == 3);
    }

    @Test
    public void refreshToRemoveMatch() {
        String topic = "home/sensor/temperature";
        when(mockSettingProvider.provide(eq(Setting.MaxPersistentFanout), eq(tenantId))).thenReturn(10);
        when(mockSettingProvider.provide(eq(Setting.MaxGroupFanout), eq(tenantId))).thenReturn(5);
        when(mockMatcher.matchAll(eq(Set.of(topic)), eq(10), eq(5)))
            .thenReturn(Map.of(topic, mockResult(2, 0, 0)));

        Set<Matching> cachedMatchings = cache.getMatch(topic, FULL_BOUNDARY).join();
        assertEquals(cachedMatchings.size(), 2);

        when(mockMatcher.matchAll(eq(Set.of(topic)), eq(10), eq(5)))
            .thenReturn(Map.of(topic, mockResult(0, 0, 0)));

        NavigableSet<RouteMatcher> routeMatchers = Sets.newTreeSet(Comparators.RouteMatcherComparator);
        routeMatchers.add(TopicUtil.from("#"));
        cache.refresh(routeMatchers);

        await().until(() -> cache.getMatch(topic, FULL_BOUNDARY).join().isEmpty());
    }

    @Test
    public void testPersistentFanoutCheck() {
        String topic = "home/sensor/temperature";
        when(mockSettingProvider.provide(eq(Setting.MaxPersistentFanout), eq(tenantId))).thenReturn(10, 5, 10);
        when(mockSettingProvider.provide(eq(Setting.MaxGroupFanout), eq(tenantId))).thenReturn(5);
        when(mockMatcher.matchAll(eq(Set.of(topic)), eq(10), eq(5)))
            .thenReturn(Map.of(topic, mockResult(0, 10, 0)));

        Set<Matching> cachedMatchings = cache.getMatch(topic, FULL_BOUNDARY).join();
        assertEquals(cachedMatchings.size(), 10);

        when(mockMatcher.matchAll(eq(Set.of(topic)), eq(5), eq(5)))
            .thenReturn(Map.of(topic, mockResult(0, 5, 0)));

        NavigableSet<RouteMatcher> routeMatchers = Sets.newTreeSet(Comparators.RouteMatcherComparator);
        routeMatchers.add(TopicUtil.from("#"));
        // set maxPersistentFanout to 5
        cache.refresh(routeMatchers);
        await().until(() -> cache.getMatch(topic, FULL_BOUNDARY).join().size() == 5);

        // set maxPersistentFanout to 10
        cache.refresh(routeMatchers);

        await().until(() -> cache.getMatch(topic, FULL_BOUNDARY).join().size() == 10);
    }

    @Test
    public void testGroupFanoutCheck() {
        String topic = "home/sensor/temperature";
        when(mockSettingProvider.provide(eq(Setting.MaxPersistentFanout), eq(tenantId))).thenReturn(10);
        when(mockSettingProvider.provide(eq(Setting.MaxGroupFanout), eq(tenantId))).thenReturn(5, 3, 5);
        when(mockMatcher.matchAll(eq(Set.of(topic)), eq(10), eq(5)))
            .thenReturn(Map.of(topic, mockResult(0, 0, 5)));

        Set<Matching> cachedMatchings = cache.getMatch(topic, FULL_BOUNDARY).join();
        assertEquals(cachedMatchings.size(), 5);

        when(mockMatcher.matchAll(eq(Set.of(topic)), eq(10), eq(3)))
            .thenReturn(Map.of(topic, mockResult(0, 0, 3)));

        NavigableSet<RouteMatcher> routeMatchers = Sets.newTreeSet(Comparators.RouteMatcherComparator);
        routeMatchers.add(TopicUtil.from("#"));
        // set maxGroupFanout to 3
        cache.refresh(routeMatchers);
        await().until(() -> cache.getMatch(topic, FULL_BOUNDARY).join().size() == 3);

        // set maxGroupFanout to 5
        cache.refresh(routeMatchers);
        await().until(() -> cache.getMatch(topic, FULL_BOUNDARY).join().size() == 5);
    }

    @Test
    public void reloadOnPersistentFanoutLimitDecrease() {
        String topic = "home/sensor/temperature";
        when(mockSettingProvider.provide(eq(Setting.MaxPersistentFanout), eq(tenantId))).thenReturn(10, 5);
        when(mockSettingProvider.provide(eq(Setting.MaxGroupFanout), eq(tenantId))).thenReturn(5);

        when(mockMatcher.matchAll(eq(Set.of(topic)), eq(10), eq(5)))
            .thenReturn(Map.of(topic, mockResult(0, 10, 0)));

        assertEquals(cache.getMatch(topic, FULL_BOUNDARY).join().size(), 10);

        when(mockMatcher.matchAll(eq(Set.of(topic)), eq(5), eq(5)))
            .thenReturn(Map.of(topic, mockResult(0, 5, 0)));

        mockTicker.advance(Duration.ofMillis(10));
        await().until(() -> cache.getMatch(topic, FULL_BOUNDARY).join().size() == 5);
    }

    @Test
    public void reloadOnPersistentFanoutLimitIncreaseWhenClamped() {
        String topic = "home/sensor/temperature";
        when(mockSettingProvider.provide(eq(Setting.MaxPersistentFanout), eq(tenantId))).thenReturn(5, 10);
        when(mockSettingProvider.provide(eq(Setting.MaxGroupFanout), eq(tenantId))).thenReturn(5);

        when(mockMatcher.matchAll(eq(Set.of(topic)), eq(5), eq(5)))
            .thenReturn(Map.of(topic, mockResult(0, 5, 0)));

        assertEquals(cache.getMatch(topic, FULL_BOUNDARY).join().size(), 5);

        when(mockMatcher.matchAll(eq(Set.of(topic)), eq(10), eq(5)))
            .thenReturn(Map.of(topic, mockResult(0, 10, 0)));

        mockTicker.advance(Duration.ofMillis(10));
        await().until(() -> cache.getMatch(topic, FULL_BOUNDARY).join().size() == 10);
    }

    @Test
    public void reloadDoesNotRematchWhenIncreaseWithoutClamp() {
        String topic = "home/sensor/temperature";
        when(mockSettingProvider.provide(eq(Setting.MaxPersistentFanout), eq(tenantId))).thenReturn(10, 20);
        when(mockSettingProvider.provide(eq(Setting.MaxGroupFanout), eq(tenantId))).thenReturn(5);
        when(mockMatcher.matchAll(eq(Set.of(topic)), eq(10), eq(5)))
            .thenReturn(Map.of(topic, mockResult(0, 3, 0)));

        Set<Matching> first = cache.getMatch(topic, FULL_BOUNDARY).join();
        assertEquals(first.size(), 3);

        mockTicker.advance(Duration.ofMillis(10));

        Set<Matching> second = cache.getMatch(topic, FULL_BOUNDARY).join();
        assertEquals(second.size(), 3);

        verify(mockMatcher, times(1)).matchAll(eq(Set.of(topic)), eq(10), eq(5));
    }

    @Test
    public void testDestroy() {
        TenantRouteCache cache = new TenantRouteCache(tenantId, mockMatcher, mockSettingProvider,
            expiryDuration, fanoutCheckDuration, directExecutor());
        assertGauge(tenantId, TenantMetric.MqttRouteCacheSize);
        cache.destroy();
        assertNoGauge(tenantId, TenantMetric.MqttRouteCacheSize);
    }

    private IMatchedRoutes mockResult(int transientFanout, int persistentFanout, int groupFanout) {
        Set<Matching> matchings = Sets.newHashSet();
        for (int i = 0; i < transientFanout; i++) {
            Matching transientMatching = new Matching(tenantId, RouteMatcher.newBuilder()
                .setMqttTopicFilter("transient/topic/" + i)
                .build()) {
                @Override
                public Type type() {
                    return Matching.Type.Normal;
                }
            };
            matchings.add(transientMatching);
        }
        for (int i = 0; i < persistentFanout; i++) {
            Matching transientMatching = new Matching(tenantId, RouteMatcher.newBuilder()
                .setMqttTopicFilter("transient/topic/" + i)
                .build()) {
                @Override
                public Type type() {
                    return Matching.Type.Normal;
                }
            };
            matchings.add(transientMatching);
        }
        for (int i = 0; i < groupFanout; i++) {
            Matching transientMatching = new Matching(tenantId, RouteMatcher.newBuilder()
                .setMqttTopicFilter("transient/topic/" + i)
                .build()) {
                @Override
                public Type type() {
                    return Type.Group;
                }
            };
            matchings.add(transientMatching);
        }
        return new IMatchedRoutes() {
            @Override
            public int maxPersistentFanout() {
                return persistentFanout;
            }

            @Override
            public int maxGroupFanout() {
                return groupFanout;
            }

            @Override
            public int persistentFanout() {
                return persistentFanout;
            }

            @Override
            public int groupFanout() {
                return groupFanout;
            }

            @Override
            public Set<Matching> routes() {
                return matchings;
            }
        };
    }

    private static final class ManualTicker implements Ticker {
        private long nanos = 0L;

        @Override
        public long read() {
            return nanos;
        }

        void advance(Duration d) {
            nanos += d.toNanos();
        }
    }
}