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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.store.proto.KVRangeROReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import java.time.Duration;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.mockito.ArgumentCaptor;
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

    @BeforeMethod
    public void setup() {
        openMocks = MockitoAnnotations.openMocks(this);
        cleaner = new DistWorkerCleaner(distWorkerClient, Duration.ofMillis(100), jobScheduler);
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
        verify(jobScheduler).schedule(taskCaptor.capture(), eq(100L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testGCOperationExecuted() {
        // Mock scheduler to capture the task
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenReturn(mockFuture);

        // Setup KVRange data
        KVRangeSetting leaderRange = new KVRangeSetting("dist.worker", "store1",
            KVRangeDescriptor.newBuilder()
                .setId(KVRangeIdUtil.generate())
                .setVer(1)
                .build());
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
        verify(jobScheduler, times(2)).schedule(any(Runnable.class), eq(100L), eq(TimeUnit.MILLISECONDS));
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

        KVRangeSetting leaderRange = new KVRangeSetting("dist.worker", "store1",
            KVRangeDescriptor.newBuilder()
                .setId(KVRangeIdUtil.generate())
                .setVer(1)
                .build());

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
        KVRangeSetting nonLeaderRange = new KVRangeSetting("dist.worker", "store2",
            KVRangeDescriptor.newBuilder()
                .setId(KVRangeIdUtil.generate())
                .setVer(1)
                .build());

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
}