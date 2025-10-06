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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.store.proto.KVRangeROReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.dist.rpc.proto.DistServiceROCoProcOutput;
import org.apache.bifromq.dist.rpc.proto.GCReply;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DistWorkerCleanerTest {
    @Mock
    private IBaseKVStoreClient distWorkerClient;
    @Mock
    private ScheduledExecutorService jobScheduler;
    private DistWorkerCleaner cleaner;
    private AutoCloseable openMocks;
    private ManualClock manualClock;

    @BeforeMethod
    public void setup() {
        openMocks = MockitoAnnotations.openMocks(this);
        manualClock = new ManualClock(java.time.Instant.EPOCH);
        cleaner = new DistWorkerCleaner(distWorkerClient, Duration.ofMillis(100), Duration.ofMillis(200), jobScheduler,
            manualClock);
        doAnswer(invocation -> {
            Runnable task = invocation.getArgument(0);
            task.run();
            return null;
        }).when(jobScheduler).execute(any(Runnable.class));
    }

    @AfterMethod
    public void tearDown() throws Exception {
        openMocks.close();
    }

    @Test
    public void testStartSchedulesRecurringTask() {
        // Mock scheduler to capture the scheduled task
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenReturn(mockFuture);

        // Start the cleaner
        cleaner.start("store1");

        // Verify initial scheduling
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler).schedule(taskCaptor.capture(), anyLong(), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testGCOperationExecuted() {
        // Mock scheduler to capture the task
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenReturn(mockFuture);

        // Setup KVRange data
        KVRangeSetting leaderRange = new KVRangeSetting("dist.worker", "store1", new HashMap<>() {
            {
                put("store1", KVRangeDescriptor.newBuilder()
                    .setId(KVRangeIdUtil.generate())
                    .setVer(1)
                    .build());
            }
        });
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        router.put(FULL_BOUNDARY, leaderRange);
        when(distWorkerClient.latestEffectiveRouter()).thenReturn(router);

        // Mock query response
        when(distWorkerClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(KVRangeROReply.newBuilder().build()));

        // Start and trigger the task
        cleaner.start("store1");
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));

        // Execute the captured task (simulate timer trigger)
        taskCaptor.getValue().run();

        // Verify GC executed for leader range
        verify(distWorkerClient).query(eq("store1"), any(KVRangeRORequest.class));

        // Verify rescheduling after execution
        verify(jobScheduler, times(2)).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testStopPreventsFurtherExecution() {
        // Mock scheduler and future
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
            .thenReturn(mockFuture);

        // Start and stop
        cleaner.start("store1");
        cleaner.stop().join();

        // Verify cancellation
        verify(mockFuture).cancel(true);

        // Ensure no more scheduling after stop
        verify(jobScheduler, times(1)).schedule(any(Runnable.class), anyLong(), any());
    }

    @Test
    public void testHandleGCFailure() {
        // Setup
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
            .thenReturn(mockFuture);

        KVRangeSetting leaderRange = new KVRangeSetting("dist.worker", "store1", new HashMap<>() {
            {
                put("store1", KVRangeDescriptor.newBuilder()
                    .setId(KVRangeIdUtil.generate())
                    .setVer(1)
                    .build());
            }
        });

        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        router.put(FULL_BOUNDARY, leaderRange);
        when(distWorkerClient.latestEffectiveRouter()).thenReturn(router);

        // Simulate query failure
        when(distWorkerClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Failed")));

        // Trigger execution
        cleaner.start("store1");
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        taskCaptor.getValue().run();

        // Verify rescheduled despite failure
        verify(jobScheduler, times(2)).schedule(any(Runnable.class), anyLong(), any());
    }

    @Test
    public void testNoGcForNonLeaderRanges() {
        // Setup non-leader range
        KVRangeSetting nonLeaderRange = new KVRangeSetting("dist.worker", "store2", new HashMap<>() {
            {
                put("store2", KVRangeDescriptor.newBuilder()
                    .setId(KVRangeIdUtil.generate())
                    .setVer(1)
                    .build());
            }
        });

        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        router.put(FULL_BOUNDARY, nonLeaderRange);
        when(distWorkerClient.latestEffectiveRouter()).thenReturn(router);

        // Trigger execution
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
            .thenReturn(mockFuture);
        cleaner.start("store1");
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        taskCaptor.getValue().run();

        // Verify no GC requests sent
        verify(distWorkerClient, never()).query(any(), any());
    }

    @Test
    public void testIdempotentStart() {
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenReturn(mockFuture);

        cleaner.start("store1");
        cleaner.start("store1"); // Duplicate call

        // Verify only one scheduling
        verify(jobScheduler, times(1)).schedule(any(Runnable.class), anyLong(), any());
    }

    @Test
    public void testShrinkOnHighSuccessRatio() {
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenReturn(mockFuture);

        KVRangeSetting leaderRange = new KVRangeSetting("dist.worker", "store1", new HashMap<>() {
            {
                put("store1", KVRangeDescriptor.newBuilder()
                    .setId(KVRangeIdUtil.generate())
                    .setVer(1)
                    .build());
            }
        });
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        router.put(FULL_BOUNDARY, leaderRange);
        when(distWorkerClient.latestEffectiveRouter()).thenReturn(router);

        AtomicInteger callCount = new AtomicInteger();
        when(distWorkerClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenAnswer(inv -> {
                int c = callCount.incrementAndGet();
                GCReply gcReply;
                if (c == 1) {
                    // high removal success ratio: removeSuccess ~= inspected
                    gcReply = GCReply.newBuilder()
                        .setInspectedCount(100)
                        .setRemoveSuccess(100)
                        .setWrapped(false)
                        .build();
                } else {
                    // default low work
                    gcReply = GCReply.getDefaultInstance();
                }
                return CompletableFuture.completedFuture(
                    KVRangeROReply.newBuilder()
                        .setCode(ReplyCode.Ok)
                        .setRoCoProcResult(ROCoProcOutput.newBuilder()
                            .setDistService(DistServiceROCoProcOutput.newBuilder()
                                .setGc(gcReply)
                                .build())
                            .build())
                        .build());
            });

        cleaner.start("store1");
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        // 1st run: trigger shrink
        taskCaptor.getValue().run();
        verify(jobScheduler, times(2)).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        InOrder inOrder = inOrder(distWorkerClient);
        ArgumentCaptor<KVRangeRORequest> reqCaptor1 = ArgumentCaptor.forClass(KVRangeRORequest.class);
        inOrder.verify(distWorkerClient).query(eq("store1"), reqCaptor1.capture());
        // Advance time to allow next GC and run second task
        manualClock.advance(Duration.ofHours(1));
        taskCaptor.getAllValues().get(1).run();
        ArgumentCaptor<KVRangeRORequest> reqCaptor2 = ArgumentCaptor.forClass(KVRangeRORequest.class);
        inOrder.verify(distWorkerClient).query(eq("store1"), reqCaptor2.capture());

        int stepHintFirst = reqCaptor1.getValue().getRoCoProc().getDistService().getGc().getStepHint();
        int stepHintSecond = reqCaptor2.getValue().getRoCoProc().getDistService().getGc().getStepHint();
        assertEquals(10, stepHintFirst);
        assertEquals(5, stepHintSecond);
    }

    @Test
    public void testLowHitSmallRangeGrowIntervalWithoutStep() {
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenReturn(mockFuture);

        KVRangeSetting leaderRange = new KVRangeSetting("dist.worker", "store1", new HashMap<>() {
            {
                put("store1", KVRangeDescriptor.newBuilder()
                    .setId(KVRangeIdUtil.generate())
                    .setVer(1)
                    .build());
            }
        });
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        router.put(FULL_BOUNDARY, leaderRange);
        when(distWorkerClient.latestEffectiveRouter()).thenReturn(router);

        // Sequence: 1) shrink by high candidate 2/3/4) low-hit with wrapped=true and small inspected
        AtomicInteger callCount = new AtomicInteger();
        when(distWorkerClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenAnswer(inv -> {
                int c = callCount.incrementAndGet();
                GCReply gcReply;
                if (c == 1) {
                    // shrink by high success ratio
                    gcReply = GCReply.newBuilder()
                        .setInspectedCount(100)
                        .setRemoveSuccess(100)
                        .setWrapped(false)
                        .build();
                } else {
                    // low candidate, small inspected, but wrapped=true ensures coverageOK
                    gcReply = GCReply.newBuilder()
                        .setInspectedCount(10)
                        .setRemoveSuccess(0)
                        .setWrapped(true)
                        .build();
                }
                return CompletableFuture.completedFuture(
                    KVRangeROReply.newBuilder()
                        .setCode(ReplyCode.Ok)
                        .setRoCoProcResult(ROCoProcOutput.newBuilder()
                            .setDistService(DistServiceROCoProcOutput.newBuilder()
                                .setGc(gcReply)
                                .build())
                            .build())
                        .build());
            });

        cleaner.start("store1");

        // capture scheduled tasks and run 5 cycles (1 shrink + 3 low-hit + 1 observe)
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        List<Runnable> scheduled = new ArrayList<>();
        verify(jobScheduler).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        scheduled.add(taskCaptor.getValue());
        scheduled.get(0).run(); // 1st: shrink to step 5
        verify(jobScheduler, times(2)).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        scheduled.add(taskCaptor.getValue());
        manualClock.advance(Duration.ofHours(1));
        scheduled.get(1).run(); // 2nd: low-hit
        verify(jobScheduler, times(3)).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        scheduled.add(taskCaptor.getValue());
        manualClock.advance(Duration.ofHours(1));
        scheduled.get(2).run(); // 3rd: low-hit
        verify(jobScheduler, times(4)).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        scheduled.add(taskCaptor.getValue());
        manualClock.advance(Duration.ofHours(1));
        scheduled.get(3).run(); // 4th: low-hit triggers growth (interval) without step growth
        // 5th scheduling for observation
        verify(jobScheduler, times(5)).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        scheduled.add(taskCaptor.getValue());

        ArgumentCaptor<KVRangeRORequest> reqCaptor = ArgumentCaptor.forClass(KVRangeRORequest.class);
        verify(distWorkerClient, times(4)).query(eq("store1"), reqCaptor.capture());
        List<KVRangeRORequest> reqs = reqCaptor.getAllValues();
        int stepAfterShrink = reqs.get(1).getRoCoProc().getDistService().getGc().getStepHint();
        int stepAfterGrowth = reqs.get(3).getRoCoProc().getDistService().getGc().getStepHint();
        // Step should remain unchanged (no growth) for small-range low-hit
        assertEquals(5, stepAfterShrink);
        assertEquals(5, stepAfterGrowth);
    }

    static class ManualClock extends Clock {
        private final ZoneId zone = ZoneOffset.UTC;
        private Instant now;

        ManualClock(Instant start) {
            this.now = start;
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }

        void advance(Duration d) {
            now = now.plus(d);
        }
    }
}
