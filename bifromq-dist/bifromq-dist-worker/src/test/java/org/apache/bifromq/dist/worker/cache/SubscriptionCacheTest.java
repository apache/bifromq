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

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.github.benmanes.caffeine.cache.Ticker;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import org.apache.bifromq.dist.worker.Comparators;
import org.apache.bifromq.dist.worker.cache.task.AddRoutesTask;
import org.apache.bifromq.dist.worker.cache.task.RefreshEntriesTask;
import org.apache.bifromq.dist.worker.schema.Matching;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.util.TopicUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SubscriptionCacheTest {

    private SubscriptionCache cache;
    private ITenantRouteCacheFactory tenantRouteCacheFactoryMock;
    private ITenantRouteCache tenantRouteCacheMock;
    private Supplier<IKVCloseableReader> readerSupplierMock;
    private KVRangeId kvRangeIdMock;
    private Executor matchExecutor;
    private Ticker tickerMock;

    @BeforeMethod
    public void setUp() {
        tenantRouteCacheFactoryMock = mock(ITenantRouteCacheFactory.class);
        tenantRouteCacheMock = mock(ITenantRouteCache.class);
        readerSupplierMock = mock(Supplier.class);
        kvRangeIdMock = mock(KVRangeId.class);
        matchExecutor = Executors.newSingleThreadExecutor();
        tickerMock = mock(Ticker.class);

        when(tenantRouteCacheFactoryMock.create(anyString())).thenReturn(tenantRouteCacheMock);
        when(tenantRouteCacheFactoryMock.expiry()).thenReturn(Duration.ofMinutes(10));

        cache = new SubscriptionCache(kvRangeIdMock, tenantRouteCacheFactoryMock, tickerMock);
        cache.reset(FULL_BOUNDARY);
    }

    @Test
    public void get() {
        String tenantId = "tenant1";
        String topic = "home/sensor/temperature";

        Set<Matching> mockMatchings = new HashSet<>();
        when(tenantRouteCacheMock.getMatch(eq(topic), any(Boundary.class))).thenReturn(
            CompletableFuture.completedFuture(mockMatchings));

        Set<Matching> resultSet = cache.get(tenantId, topic).join();
        assertEquals(mockMatchings, resultSet);
        verify(tenantRouteCacheMock).getMatch(eq(topic), any(Boundary.class));
    }

    @Test
    public void isCached() {
        String tenantId = "tenant1";
        String topic = "home/sensor/temperature";
        List<String> filterLevels = TopicUtil.from(topic).getFilterLevelList();
        assertFalse(cache.isCached(tenantId, filterLevels));

        Set<Matching> mockMatchings = new HashSet<>();
        when(tenantRouteCacheMock.getMatch(eq(topic), any(Boundary.class))).thenReturn(
            CompletableFuture.completedFuture(mockMatchings));
        when(tenantRouteCacheFactoryMock.create(eq(tenantId))).thenReturn(tenantRouteCacheMock);
        // load cache
        cache.get(tenantId, topic).join();

        assertFalse(cache.isCached(tenantId, TopicUtil.from(topic).getFilterLevelList()));

        when(tenantRouteCacheMock.isCached(eq(filterLevels))).thenReturn(true);
        assertTrue(cache.isCached(tenantId, filterLevels));
    }

    @Test
    public void refresh() {
        String tenantId = "tenant1";
        NavigableMap<RouteMatcher, Set<Matching>> routeMatchers = new TreeMap<>(Comparators.RouteMatcherComparator);
        RefreshEntriesTask refreshEntriesTask = AddRoutesTask.of(routeMatchers);
        Map<String, RefreshEntriesTask> matchesByTenant = new HashMap<>();
        matchesByTenant.put(tenantId, refreshEntriesTask);

        when(tenantRouteCacheFactoryMock.create(tenantId)).thenReturn(tenantRouteCacheMock);
        cache.refresh(matchesByTenant);

        verify(tenantRouteCacheMock, never()).refresh(refreshEntriesTask);
    }

    @Test
    public void cacheExpiry() {
        String tenantId = "tenant1";
        String topic = "home/sensor/temperature";

        Set<Matching> mockMatchings = new HashSet<>();
        when(tenantRouteCacheMock.getMatch(eq(topic), any(Boundary.class))).thenReturn(
            CompletableFuture.completedFuture(mockMatchings));

        cache.get(tenantId, topic);

        long expiryNanos = Duration.ofMinutes(20).toNanos();
        when(tickerMock.read()).thenReturn(0L).thenReturn(expiryNanos);

        CompletableFuture<Set<Matching>> result = cache.get(tenantId, topic);
        assertNotNull(result);
        assertTrue(result.isDone());
        verify(tenantRouteCacheMock, times(2)).getMatch(eq(topic), any(Boundary.class));
    }

    @Test
    public void resetBoundary() {
        String tenantId = "tenant1";
        String topic = "home/sensor/temperature";

        Set<Matching> mockMatchings = new HashSet<>();
        when(tenantRouteCacheMock.getMatch(eq(topic), any(Boundary.class))).thenReturn(
            CompletableFuture.completedFuture(mockMatchings));
        cache.get(tenantId, topic);

        Boundary boundary = Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("z")).build();
        cache.reset(boundary);

        CompletableFuture<Set<Matching>> result = cache.get("tenant1", "home/sensor/temperature");
        assertNotNull(result);
        verify(tenantRouteCacheMock, times(2)).getMatch(eq(topic), any(Boundary.class));
    }

    @Test
    public void close() {
        cache.close();
        verify(tenantRouteCacheFactoryMock).close();
    }
}