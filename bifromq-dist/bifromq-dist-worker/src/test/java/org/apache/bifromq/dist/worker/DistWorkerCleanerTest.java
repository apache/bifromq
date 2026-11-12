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

package org.apache.bifromq.dist.worker;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.proto.KVRangeROReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.dist.rpc.proto.DistServiceROCoProcOutput;
import org.apache.bifromq.dist.rpc.proto.GCReply;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DistWorkerCleanerTest {
    private static final String STORE = "gc-test-store";
    private IBaseKVStoreClient client;
    private ScheduledExecutorService scheduler;
    private DistWorkerCleaner cleaner;
    private SimpleMeterRegistry registry;
    private Deque<Wakeup> wakeups;
    private NavigableMap<Boundary, KVRangeSetting> routes;
    private KVRangeId rangeId;

    @BeforeMethod
    public void setup() {
        client = mock(IBaseKVStoreClient.class);
        scheduler = mock(ScheduledExecutorService.class);
        registry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(registry);
        wakeups = new ArrayDeque<>();
        routes = new TreeMap<>(BoundaryUtil::compare);
        rangeId = KVRangeIdUtil.generate();
        routes.put(FULL_BOUNDARY, setting(rangeId, 1, FULL_BOUNDARY, STORE));
        when(client.latestEffectiveRouter()).thenAnswer(inv -> routes);
        when(client.query(eq(STORE), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(reply(512, 0, false, null)));
        doAnswer(inv -> {
            inv.getArgument(0, Runnable.class).run();
            return null;
        }).when(scheduler).execute(any(Runnable.class));
        when(scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenAnswer(inv -> {
            ScheduledFuture<?> future = mock(ScheduledFuture.class);
            wakeups.add(new Wakeup(inv.getArgument(0),
                inv.getArgument(2, TimeUnit.class).toNanos(inv.getArgument(1)), future));
            return future;
        });
        cleaner = new DistWorkerCleaner(client, Duration.ZERO, Duration.ofSeconds(32), scheduler);
    }

    @AfterMethod
    public void teardown() {
        cleaner.stop().join();
        Metrics.globalRegistry.remove(registry);
        registry.close();
    }

    @Test
    public void startsOnceWithoutFirstSweepSprint() {
        cleaner.start(STORE);
        cleaner.start(STORE);
        assertEquals(wakeups.size(), 1);
        assertEquals(wakeups.peek().delayNanos, TimeUnit.SECONDS.toNanos(32));
        runNext();
        KVRangeRORequest request = requests(1).getValue();
        assertEquals(request.getRoCoProc().getDistService().getGc().getStepHint(), 1);
        assertEquals(request.getRoCoProc().getDistService().getGc().getScanQuota(), 2048);
        assertFalse(request.getRoCoProc().getDistService().getGc().hasStartKey());
        assertEquals(wakeups.peek().delayNanos, TimeUnit.SECONDS.toNanos(8));
    }

    @DataProvider
    public Object[][] resultsAndWaits() {
        return new Object[][] {
            {512, 0, 8_000_000_000L}, {512, 1, 8_000_000_000L},
            {512, 8, 8_000_000_000L}, {512, 16, 5_333_333_334L},
            {1024, 0, 16_000_000_000L},
            {2048, 8, 32_000_000_000L}, {2048, 64, 21_333_333_334L},
            {1, 16, 5_333_333_334L}, {0, 0, 32_000_000_000L}
        };
    }

    @Test(dataProvider = "resultsAndWaits")
    public void waitsForActualWorkAndDeletionYield(int inspected, int removed, long expectedWait) {
        when(client.query(eq(STORE), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(reply(inspected, removed, false, null)));
        cleaner.start(STORE);
        runNext();
        assertEquals(wakeups.peek().delayNanos, expectedWait);
    }

    @Test
    public void highYieldDoesNotPermanentlyRaiseTheRate() {
        when(client.query(eq(STORE), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(reply(512, 32, false, null)))
            .thenReturn(CompletableFuture.completedFuture(reply(512, 8, false, null)))
            .thenReturn(CompletableFuture.completedFuture(reply(512, 1, false, null)));
        cleaner.start(STORE);
        runNext();
        assertEquals(wakeups.peek().delayNanos, 5_333_333_334L);
        runNext();
        assertEquals(wakeups.peek().delayNanos, 5_333_333_334L);
        runNext();
        assertEquals(wakeups.peek().delayNanos, 8_000_000_000L);
    }

    @Test
    public void sustainedGarbageSpeedsUpAndCleanBatchesRestoreBaseRate() {
        // A backlog spans several batches, then the cursor reaches clean routes.
        when(client.query(eq(STORE), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(reply(512, 32, false, null)));
        cleaner.start(STORE);
        long[] highYieldWaits = {
            5_333_333_334L, 3_555_555_556L, 2_370_370_371L,
            1_580_246_914L, 1_333_333_334L, 1_333_333_334L
        };
        for (long expectedWait : highYieldWaits) {
            runNext();
            assertEquals(wakeups.peek().delayNanos, expectedWait);
        }
        when(client.query(eq(STORE), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(reply(512, 0, false, null)));
        long[] lowYieldWaits = {
            2_000_000_000L, 3_000_000_000L, 4_500_000_000L,
            6_750_000_000L, 8_000_000_000L, 8_000_000_000L
        };
        for (long expectedWait : lowYieldWaits) {
            runNext();
            assertEquals(wakeups.peek().delayNanos, expectedWait);
        }
    }

    @Test
    public void respectsExplicitIntervalBounds() {
        cleaner = new DistWorkerCleaner(client, Duration.ofSeconds(10), Duration.ofSeconds(20), scheduler);
        cleaner.start(STORE);
        assertEquals(wakeups.peek().delayNanos, 20_000_000_000L);
        runNext();
        assertEquals(wakeups.peek().delayNanos, 10_000_000_000L);
        when(client.query(eq(STORE), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(reply(2048, 0, false, null)));
        runNext();
        assertEquals(wakeups.peek().delayNanos, 20_000_000_000L);
    }

    @Test
    public void validatesIntervalsBeforeBuildingWorkerResources() {
        assertThrows(IllegalArgumentException.class,
            () -> new DistWorkerBuilder().minGCInterval(Duration.ofSeconds(-1)).build());
        assertThrows(IllegalArgumentException.class,
            () -> new DistWorkerBuilder().maxGCInterval(Duration.ZERO).build());
        assertThrows(IllegalArgumentException.class,
            () -> new DistWorkerBuilder().minGCInterval(Duration.ofSeconds(33)).build());
        DistWorkerBuilder defaults = new DistWorkerBuilder();
        assertEquals(defaults.minGCInterval, Duration.ZERO);
        assertEquals(defaults.maxGCInterval, Duration.ofSeconds(32));
    }

    @Test
    public void preservesProgressAfterQueryFailureAndRejection() {
        ByteString cursor = ByteString.copyFromUtf8("cursor");
        when(client.query(eq(STORE), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(reply(512, 32, false, cursor)))
            .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("query failed")))
            .thenReturn(CompletableFuture.completedFuture(KVRangeROReply.newBuilder()
                .setCode(ReplyCode.BadVersion).build()))
            .thenReturn(CompletableFuture.completedFuture(reply(512, 0, false, null)));
        cleaner.start(STORE);
        runNext();
        runNext();
        assertEquals(wakeups.peek().delayNanos, 32_000_000_000L);
        runNext();
        assertEquals(wakeups.peek().delayNanos, 32_000_000_000L);
        runNext();
        for (KVRangeRORequest request : requests(4).getAllValues().subList(1, 4)) {
            assertEquals(request.getRoCoProc().getDistService().getGc().getStartKey(), cursor);
        }
    }

    @Test
    public void wrappedLegacyReplyRestartsWithoutItsCursor() {
        when(client.query(eq(STORE), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(reply(512, 32, true, ByteString.copyFromUtf8("legacy"))));
        cleaner.start(STORE);
        runNext();
        assertEquals(wakeups.peek().delayNanos, 5_333_333_334L);
        runNext();
        assertFalse(requests(2).getAllValues().get(1).getRoCoProc().getDistService().getGc().hasStartKey());
    }

    @Test
    public void rotatesRangesWithoutOverlappingQueries() {
        ByteString split = ByteString.copyFromUtf8("m");
        Boundary left = Boundary.newBuilder().setEndKey(split).build();
        Boundary right = Boundary.newBuilder().setStartKey(split).build();
        routes.clear();
        routes.put(left, setting(rangeId, 1, left, STORE));
        routes.put(right, setting(KVRangeIdUtil.generate(), 1, right, STORE));
        CompletableFuture<KVRangeROReply> pending = new CompletableFuture<>();
        when(client.query(eq(STORE), any(KVRangeRORequest.class)))
            .thenReturn(pending)
            .thenReturn(CompletableFuture.completedFuture(reply(512, 0, false, null)));
        cleaner.start(STORE);
        runNext();
        assertTrue(wakeups.isEmpty());
        verify(client, times(1)).query(eq(STORE), any(KVRangeRORequest.class));
        pending.complete(reply(512, 0, false, null));
        runNext();
        runNext();
        var issued = requests(3).getAllValues();
        assertNotEquals(issued.get(0).getKvRangeId(), issued.get(1).getKvRangeId());
        assertEquals(issued.get(0).getKvRangeId(), issued.get(2).getKvRangeId());
    }

    @Test
    public void ignoresReplyFromPreviousRangeVersion() {
        ByteString split = ByteString.copyFromUtf8("m");
        Boundary nextBoundary = Boundary.newBuilder().setStartKey(split).build();
        CompletableFuture<KVRangeROReply> pending = new CompletableFuture<>();
        when(client.query(eq(STORE), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(reply(512, 0, false, ByteString.copyFromUtf8("a"))))
            .thenReturn(pending)
            .thenReturn(CompletableFuture.completedFuture(reply(512, 0, false, null)));
        cleaner.start(STORE);
        runNext();
        runNext();
        routes.clear();
        Boundary left = Boundary.newBuilder().setEndKey(split).build();
        routes.put(left, setting(KVRangeIdUtil.generate(), 1, left, "other-store"));
        routes.put(nextBoundary, setting(rangeId, 2, nextBoundary, STORE));
        pending.complete(reply(512, 32, false, ByteString.copyFromUtf8("z")));
        assertEquals(wakeups.peek().delayNanos, 32_000_000_000L);
        runNext();
        KVRangeRORequest request = requests(3).getAllValues().get(2);
        assertEquals(request.getVer(), 2L);
        assertEquals(request.getRoCoProc().getDistService().getGc().getStartKey(), split);
    }

    @Test
    public void removesMetricsWhenLeadershipIsLostDuringQuery() {
        CompletableFuture<KVRangeROReply> pending = new CompletableFuture<>();
        when(client.query(eq(STORE), any(KVRangeRORequest.class))).thenReturn(pending);
        cleaner.start(STORE);
        runNext();
        assertEquals(registry.get("dist.gc.step").gauge().value(), 1.0);
        routes.clear();
        pending.complete(reply(512, 32, false, ByteString.copyFromUtf8("old")));
        assertNull(registry.find("dist.gc.step").gauge());
        assertNull(registry.find("dist.gc.interval.ms").gauge());
        runNext();
        verify(client, times(1)).query(eq(STORE), any(KVRangeRORequest.class));
        assertEquals(wakeups.peek().delayNanos, 32_000_000_000L);
    }

    @Test
    public void onlyCleansLocalLeaders() {
        routes.put(FULL_BOUNDARY, setting(rangeId, 1, FULL_BOUNDARY, "other-store"));
        cleaner.start(STORE);
        runNext();
        verify(client, never()).query(any(), any());
        assertEquals(wakeups.peek().delayNanos, 32_000_000_000L);
    }

    @Test
    public void stopCancelsWakeupAndDiscardsPendingReplies() {
        CompletableFuture<KVRangeROReply> pending = new CompletableFuture<>();
        when(client.query(eq(STORE), any(KVRangeRORequest.class))).thenReturn(pending);
        cleaner.start(STORE);
        Wakeup first = wakeups.peek();
        runNext();
        cleaner.stop().join();
        cleaner.stop().join();
        verify(first.future).cancel(false);
        clearInvocations(scheduler);
        pending.complete(reply(512, 32, false, null));
        verify(scheduler, never()).execute(any(Runnable.class));
        assertTrue(wakeups.isEmpty());
        assertNull(registry.find("dist.gc.step").gauge());
        verify(scheduler, never()).shutdown();
    }

    @Test
    public void stopBeforeFirstWakeupDoesNotQuery() {
        cleaner.start(STORE);
        cleaner.stop().join();
        runNext();
        verify(client, never()).query(any(), any());
        assertTrue(wakeups.isEmpty());
    }

    @Test
    public void stopWithPendingQueryOnMultithreadScheduler() throws InterruptedException {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        CompletableFuture<KVRangeROReply> pending = new CompletableFuture<>();
        CountDownLatch queried = new CountDownLatch(1);
        when(client.query(eq(STORE), any(KVRangeRORequest.class))).thenAnswer(inv -> {
            queried.countDown();
            return pending;
        });
        cleaner = new DistWorkerCleaner(client, Duration.ZERO, Duration.ofMillis(1), executor);
        try {
            cleaner.start(STORE);
            assertTrue(queried.await(5, TimeUnit.SECONDS));
            cleaner.stop().join();
            pending.complete(reply(512, 32, false, null));
            executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
            verify(client, times(1)).query(eq(STORE), any(KVRangeRORequest.class));
            assertNull(registry.find("dist.gc.step").gauge());
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new AssertionError(e);
        } finally {
            cleaner.stop().join();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private void runNext() {
        wakeups.remove().task.run();
    }

    private ArgumentCaptor<KVRangeRORequest> requests(int count) {
        ArgumentCaptor<KVRangeRORequest> captor = ArgumentCaptor.forClass(KVRangeRORequest.class);
        verify(client, times(count)).query(eq(STORE), captor.capture());
        return captor;
    }

    private KVRangeSetting setting(KVRangeId id, long ver, Boundary boundary, String leader) {
        return new KVRangeSetting("dist.worker", leader, Map.of(leader,
            KVRangeDescriptor.newBuilder().setId(id).setVer(ver).setBoundary(boundary).build()));
    }

    private KVRangeROReply reply(int inspected, int removed, boolean wrapped, ByteString cursor) {
        GCReply.Builder gc = GCReply.newBuilder().setInspectedCount(inspected)
            .setRemoveSuccess(removed).setWrapped(wrapped);
        if (cursor != null) {
            gc.setNextStartKey(cursor);
        }
        return KVRangeROReply.newBuilder().setCode(ReplyCode.Ok)
            .setRoCoProcResult(ROCoProcOutput.newBuilder()
                .setDistService(DistServiceROCoProcOutput.newBuilder().setGc(gc)))
            .build();
    }

    private record Wakeup(Runnable task, long delayNanos, ScheduledFuture<?> future) {
    }
}
