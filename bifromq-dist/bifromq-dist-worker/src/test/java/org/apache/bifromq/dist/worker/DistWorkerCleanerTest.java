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
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
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
    private SimpleMeterRegistry meterRegistry;

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
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
        Metrics.globalRegistry.remove(meterRegistry);
        openMocks.close();
    }

    @Test
    public void testStartSchedulesRecurringTask() {
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenReturn(mockFuture);

        cleaner.start("store1");

        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler).schedule(taskCaptor.capture(), anyLong(), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testGCOperationExecuted() {
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

        when(distWorkerClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(KVRangeROReply.newBuilder().build()));

        cleaner.start("store1");
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));

        taskCaptor.getValue().run();

        verify(distWorkerClient).query(eq("store1"), any(KVRangeRORequest.class));

        verify(jobScheduler, times(2)).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testStopPreventsFurtherExecution() {
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
            .thenReturn(mockFuture);

        cleaner.start("store1");
        cleaner.stop().join();

        verify(mockFuture).cancel(true);

        verify(jobScheduler, times(1)).schedule(any(Runnable.class), anyLong(), any());
    }

    @Test
    public void testHandleGCFailure() {
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

        when(distWorkerClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Failed")));

        cleaner.start("store1");
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        taskCaptor.getValue().run();

        verify(jobScheduler, times(2)).schedule(any(Runnable.class), anyLong(), any());
    }

    @Test
    public void testNoGcForNonLeaderRanges() {
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

        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
            .thenReturn(mockFuture);
        cleaner.start("store1");
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        taskCaptor.getValue().run();

        verify(distWorkerClient, never()).query(any(), any());
    }

    @Test
    public void testIdempotentStart() {
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenReturn(mockFuture);

        cleaner.start("store1");
        cleaner.start("store1"); // Duplicate call

        verify(jobScheduler, times(1)).schedule(any(Runnable.class), anyLong(), any());
    }

    @Test
    public void testFirstSweepQuotaAndStepHint() {
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

        when(distWorkerClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                KVRangeROReply.newBuilder()
                    .setCode(ReplyCode.Ok)
                    .setRoCoProcResult(ROCoProcOutput.newBuilder()
                        .setDistService(DistServiceROCoProcOutput.newBuilder()
                            .setGc(GCReply.newBuilder().setInspectedCount(10).setRemoveSuccess(0).setWrapped(false).build())
                            .build())
                        .build())
                    .build()));

        cleaner.start("store1");
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        taskCaptor.getValue().run();

        ArgumentCaptor<KVRangeRORequest> reqCaptor = ArgumentCaptor.forClass(KVRangeRORequest.class);
        verify(distWorkerClient).query(eq("store1"), reqCaptor.capture());
        int stepHint = reqCaptor.getValue().getRoCoProc().getDistService().getGc().getStepHint();
        int quota = reqCaptor.getValue().getRoCoProc().getDistService().getGc().getScanQuota();
        assertEquals(1, stepHint);
        assertEquals(50_000, quota);
    }

    @Test
    public void testSwitchToAdaptiveAfterFirstWrap() {
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

        AtomicInteger call = new AtomicInteger();
        when(distWorkerClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenAnswer(inv -> {
                GCReply reply = call.getAndIncrement() == 0
                    ? GCReply.newBuilder().setInspectedCount(10).setRemoveSuccess(0).setWrapped(true).build()
                    : GCReply.getDefaultInstance();
                return CompletableFuture.completedFuture(
                    KVRangeROReply.newBuilder()
                        .setCode(ReplyCode.Ok)
                        .setRoCoProcResult(ROCoProcOutput.newBuilder()
                            .setDistService(DistServiceROCoProcOutput.newBuilder().setGc(reply).build())
                            .build())
                        .build());
            });

        cleaner.start("store1");
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        taskCaptor.getValue().run();
        verify(jobScheduler, times(2)).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        manualClock.advance(Duration.ofMinutes(10));
        taskCaptor.getAllValues().get(1).run();

        ArgumentCaptor<KVRangeRORequest> reqCaptor = ArgumentCaptor.forClass(KVRangeRORequest.class);
        verify(distWorkerClient, times(2)).query(eq("store1"), reqCaptor.capture());
        KVRangeRORequest secondReq = reqCaptor.getAllValues().get(1);
        int stepHint2 = secondReq.getRoCoProc().getDistService().getGc().getStepHint();
        int quota2 = secondReq.getRoCoProc().getDistService().getGc().getScanQuota();
        assertEquals(1, stepHint2);
        assertEquals(512, quota2);
    }

    @Test
    public void testFirstSweepFailureKeepsSprint() {
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

        AtomicInteger call = new AtomicInteger();
        when(distWorkerClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenAnswer(inv -> {
                if (call.getAndIncrement() == 0) {
                    return CompletableFuture.failedFuture(new RuntimeException("network"));
                }
                return CompletableFuture.completedFuture(
                    KVRangeROReply.newBuilder()
                        .setCode(ReplyCode.Ok)
                        .setRoCoProcResult(ROCoProcOutput.newBuilder()
                            .setDistService(DistServiceROCoProcOutput.newBuilder()
                                .setGc(GCReply.newBuilder().setWrapped(false).build())
                                .build())
                            .build())
                        .build());
            });

        cleaner.start("store1");
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        taskCaptor.getValue().run();
        verify(jobScheduler, times(2)).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        manualClock.advance(Duration.ofSeconds(5));
        taskCaptor.getAllValues().get(1).run();

        ArgumentCaptor<KVRangeRORequest> reqCaptor = ArgumentCaptor.forClass(KVRangeRORequest.class);
        verify(distWorkerClient, times(2)).query(eq("store1"), reqCaptor.capture());
        int stepHint = reqCaptor.getAllValues().get(1).getRoCoProc().getDistService().getGc().getStepHint();
        int quota = reqCaptor.getAllValues().get(1).getRoCoProc().getDistService().getGc().getScanQuota();
        assertEquals(1, stepHint);
        assertEquals(50_000, quota);
    }

    @Test
    public void testMetricsRegisteredAndUpdated() {
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

        when(distWorkerClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                KVRangeROReply.newBuilder()
                    .setCode(ReplyCode.Ok)
                    .setRoCoProcResult(ROCoProcOutput.newBuilder()
                        .setDistService(DistServiceROCoProcOutput.newBuilder()
                            .setGc(GCReply.newBuilder().setInspectedCount(10).setRemoveSuccess(0).setWrapped(true).build())
                            .build())
                        .build())
                    .build()));

        cleaner.start("store1");
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        taskCaptor.getValue().run();

        String rangeIdStr = KVRangeIdUtil.toString(leaderRange.id());
        Gauge stepGauge = Metrics.globalRegistry.find("dist.gc.step")
            .tag("storeId", "store1").tag("rangeId", rangeIdStr).gauge();
        Gauge intervalGauge = Metrics.globalRegistry.find("dist.gc.interval.ms")
            .tag("storeId", "store1").tag("rangeId", rangeIdStr).gauge();

        assertEquals(1.0, stepGauge.value(), 0.0);
        assertEquals(100.0, intervalGauge.value(), 0.0);
    }

    @Test
    public void testMetricsRemovedOnLeadershipLost() {
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
        NavigableMap<Boundary, KVRangeSetting> router1 = new TreeMap<>(BoundaryUtil::compare);
        router1.put(FULL_BOUNDARY, leaderRange);
        NavigableMap<Boundary, KVRangeSetting> router2 = new TreeMap<>(BoundaryUtil::compare); // empty -> lose leadership
        when(distWorkerClient.latestEffectiveRouter()).thenReturn(router1, router2);

        when(distWorkerClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                KVRangeROReply.newBuilder()
                    .setCode(ReplyCode.Ok)
                    .setRoCoProcResult(ROCoProcOutput.newBuilder()
                        .setDistService(DistServiceROCoProcOutput.newBuilder()
                            .setGc(GCReply.getDefaultInstance())
                            .build())
                        .build())
                    .build()));

        cleaner.start("store1");
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        taskCaptor.getValue().run();
        String rangeIdStr = KVRangeIdUtil.toString(leaderRange.id());
        Gauge stepGauge = Metrics.globalRegistry.find("dist.gc.step")
            .tag("storeId", "store1").tag("rangeId", rangeIdStr).gauge();
        Gauge intervalGauge = Metrics.globalRegistry.find("dist.gc.interval.ms")
            .tag("storeId", "store1").tag("rangeId", rangeIdStr).gauge();
        assertEquals(1.0, stepGauge.value(), 0.0);

        verify(jobScheduler, times(2)).schedule(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
        manualClock.advance(Duration.ofMinutes(10));
        taskCaptor.getAllValues().get(1).run();

        Gauge stepGaugeAfter = Metrics.globalRegistry.find("dist.gc.step")
            .tag("storeId", "store1").tag("rangeId", rangeIdStr).gauge();
        Gauge intervalGaugeAfter = Metrics.globalRegistry.find("dist.gc.interval.ms")
            .tag("storeId", "store1").tag("rangeId", rangeIdStr).gauge();
        assertNull(stepGaugeAfter);
        assertNull(intervalGaugeAfter);
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
