/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.dist.worker.cache;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.dist.worker.Comparators;
import org.apache.bifromq.dist.worker.MeterTest;
import org.apache.bifromq.dist.worker.schema.Matching;
import org.apache.bifromq.metrics.TenantMetric;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.util.TopicUtil;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TenantRouteCacheTest extends MeterTest {

    private TenantRouteCache cache;
    private Ticker mockTicker;
    private ITenantRouteMatcher mockMatcher;
    private String tenantId;
    private Duration expiryDuration;

    @BeforeMethod
    public void setup() {
        super.setup();
        tenantId = "tenant1";
        mockTicker = mock(Ticker.class);
        mockMatcher = mock(ITenantRouteMatcher.class);
        expiryDuration = Duration.ofMinutes(1);
        cache = new TenantRouteCache(tenantId, mockMatcher, expiryDuration, directExecutor());
    }

    @AfterMethod
    public void tearDown() {
        cache.destroy();
        super.tearDown();
    }

    @Test
    public void getMatch() {
        String topic = "home/sensor/temperature";
        Matching normalMatching = mock(Matching.class);
        when(mockMatcher.matchAll(eq(Set.of(topic)))).thenReturn(Map.of(topic, Set.of(normalMatching)));

        Set<Matching> cachedMatchings = cache.getMatch(topic, FULL_BOUNDARY).join();
        assertNotNull(cachedMatchings);
        assertTrue(cachedMatchings.contains(normalMatching));
    }

    @Test
    public void isCached() {
        String topic = "home/sensor/temperature";
        Matching normalMatching = mock(Matching.class);
        when(mockMatcher.matchAll(eq(Set.of(topic)))).thenReturn(Map.of(topic, Set.of(normalMatching)));
        cache.getMatch(topic, FULL_BOUNDARY).join();

        assertTrue(cache.isCached(TopicUtil.from(topic).getFilterLevelList()));
    }

    @Test
    public void refreshToAddMatch() {
        String topic = "home/sensor/temperature";
        Matching normalMatching = mock(Matching.class);
        when(normalMatching.type()).thenReturn(Matching.Type.Normal);
        when(mockMatcher.matchAll(eq(Set.of(topic)))).thenReturn(Map.of(topic, Set.of(normalMatching)));

        Set<Matching> cachedMatchings = cache.getMatch(topic, FULL_BOUNDARY).join();
        assertTrue(cachedMatchings.contains(normalMatching));

        Matching normalMatching1 = mock(Matching.class);
        when(normalMatching1.type()).thenReturn(Matching.Type.Normal);

        Matching normalMatching2 = mock(Matching.class);
        when(normalMatching2.type()).thenReturn(Matching.Type.Normal);

        when(mockMatcher.matchAll(eq(Set.of(topic)))).thenReturn(
            Map.of(topic, Set.of(normalMatching, normalMatching1, normalMatching2)));

        NavigableSet<RouteMatcher> routeMatchers = Sets.newTreeSet(Comparators.RouteMatcherComparator);
        routeMatchers.add(TopicUtil.from("#"));
        cache.refresh(routeMatchers);

        await().until(() -> cache.getMatch(topic, FULL_BOUNDARY).join().size() == 3);
    }

    @Test
    public void refreshToRemoveMatch() {
        String topic = "home/sensor/temperature";
        Matching normalMatching = mock(Matching.class);
        when(normalMatching.type()).thenReturn(Matching.Type.Normal);

        Matching normalMatching1 = mock(Matching.class);
        when(normalMatching1.type()).thenReturn(Matching.Type.Normal);

        when(mockMatcher.matchAll(eq(Set.of(topic)))).thenReturn(
            Map.of(topic, Sets.newHashSet(normalMatching, normalMatching1)));

        Set<Matching> cachedMatchings = cache.getMatch(topic, FULL_BOUNDARY).join();
        assertTrue(cachedMatchings.contains(normalMatching));
        assertTrue(cachedMatchings.contains(normalMatching1));

        when(mockMatcher.matchAll(eq(Set.of(topic)))).thenReturn(Map.of(topic, Collections.emptySet()));

        NavigableSet<RouteMatcher> routeMatchers = Sets.newTreeSet(Comparators.RouteMatcherComparator);
        routeMatchers.add(TopicUtil.from("#"));
        cache.refresh(routeMatchers);

        await().until(() -> cache.getMatch(topic, FULL_BOUNDARY).join().isEmpty());
    }

    @Test
    void testDestroy() {
        TenantRouteCache cache = new TenantRouteCache(tenantId, mockMatcher, expiryDuration, directExecutor());
        assertGauge(tenantId, TenantMetric.MqttRouteCacheSize);
        cache.destroy();
        assertNoGauge(tenantId, TenantMetric.MqttRouteCacheSize);
    }
}