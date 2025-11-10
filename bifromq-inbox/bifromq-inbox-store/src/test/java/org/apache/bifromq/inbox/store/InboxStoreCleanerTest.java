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

package org.apache.bifromq.inbox.store;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
import org.apache.bifromq.inbox.storage.proto.GCReply;
import org.apache.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InboxStoreCleanerTest {
    @Mock
    private IBaseKVStoreClient inboxStoreClient;
    @Mock
    private ScheduledExecutorService jobScheduler;
    private InboxStoreCleaner cleaner;
    private AutoCloseable openMocks;
    private ManualClock manualClock;
    private SimpleMeterRegistry meterRegistry;

    @BeforeMethod
    public void setup() {
        openMocks = MockitoAnnotations.openMocks(this);
        manualClock = new ManualClock(Instant.EPOCH);
        cleaner = new InboxStoreCleaner(inboxStoreClient, Duration.ofMillis(100), Duration.ofHours(24), jobScheduler,
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
        // cleanup metrics registry
        Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
        Metrics.globalRegistry.remove(meterRegistry);
        openMocks.close();
    }

    @Test
    public void testStartSchedulesRecurringTask() {
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any())).thenReturn(mockFuture);

        cleaner.start("store1");

        verify(jobScheduler, times(1)).schedule(any(Runnable.class), anyLong(), any());
    }

    @Test
    public void testGCOperationExecuted() {
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any())).thenAnswer(inv -> mockFuture);

        KVRangeSetting leaderRange = new KVRangeSetting("inbox.store", "store1", new HashMap<>() {
            {
                put("store1", KVRangeDescriptor.newBuilder().setId(KVRangeIdUtil.generate()).setVer(1).build());
            }
        });
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        router.put(FULL_BOUNDARY, leaderRange);
        when(inboxStoreClient.latestEffectiveRouter()).thenReturn(router);

        when(inboxStoreClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(KVRangeROReply.newBuilder()
                .setCode(ReplyCode.Ok)
                .setRoCoProcResult(ROCoProcOutput.newBuilder()
                    .setInboxService(InboxServiceROCoProcOutput.newBuilder().setGc(GCReply.getDefaultInstance())
                        .build())
                    .build())
                .build()));

        cleaner.start("store1");
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler, times(1)).schedule(taskCaptor.capture(), anyLong(), any());
        taskCaptor.getValue().run();

        verify(inboxStoreClient).query(eq("store1"), any(KVRangeRORequest.class));
        verify(jobScheduler, times(2)).schedule(any(Runnable.class), anyLong(), any());
    }

    @Test
    public void testStopPreventsFurtherExecution() {
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any())).thenReturn(mockFuture);

        cleaner.start("store1");
        cleaner.stop().join();

        verify(mockFuture).cancel(true);
        verify(jobScheduler, times(1)).schedule(any(Runnable.class), anyLong(), any());
    }

    @Test
    public void testNoGcForNonLeaderRanges() {
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any())).thenReturn(mockFuture);

        KVRangeSetting nonLeaderRange = new KVRangeSetting("inbox.store", "store2", new HashMap<>() {
            {
                put("store2", KVRangeDescriptor.newBuilder().setId(KVRangeIdUtil.generate()).setVer(1).build());
            }
        });
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        router.put(FULL_BOUNDARY, nonLeaderRange);
        when(inboxStoreClient.latestEffectiveRouter()).thenReturn(router);

        cleaner.start("store1");
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler, times(1)).schedule(taskCaptor.capture(), anyLong(), any());
        taskCaptor.getValue().run();

        verify(inboxStoreClient, never()).query(any(), any());
    }

    @Test
    public void testLowHitGrowthThenShrink() {
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any())).thenAnswer(inv -> mockFuture);

        KVRangeSetting leaderRange = new KVRangeSetting("inbox.store", "store1", new HashMap<>() {
            {
                put("store1", KVRangeDescriptor.newBuilder().setId(KVRangeIdUtil.generate()).setVer(1).build());
            }
        });
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        router.put(FULL_BOUNDARY, leaderRange);
        when(inboxStoreClient.latestEffectiveRouter()).thenReturn(router);

        AtomicInteger callCount = new AtomicInteger();
        when(inboxStoreClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenAnswer(inv -> {
                int c = callCount.incrementAndGet();
                GCReply reply;
                if (c <= 3) {
                    reply = GCReply.newBuilder().setInspectedCount(512).setRemoveSuccess(0).setWrapped(true).build();
                } else if (c == 4) {
                    reply = GCReply.newBuilder().setInspectedCount(512).setRemoveSuccess(512).setWrapped(false)
                        .build();
                } else {
                    reply = GCReply.newBuilder().setInspectedCount(1).setRemoveSuccess(0).setWrapped(false).build();
                }
                return CompletableFuture.completedFuture(KVRangeROReply.newBuilder()
                    .setCode(ReplyCode.Ok)
                    .setRoCoProcResult(ROCoProcOutput.newBuilder()
                        .setInboxService(InboxServiceROCoProcOutput.newBuilder().setGc(reply).build())
                        .build())
                    .build());
            });

        cleaner.start("store1");
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler, times(1)).schedule(taskCaptor.capture(), anyLong(), any());
        taskCaptor.getValue().run();
        manualClock.advance(Duration.ofHours(1));
        verify(jobScheduler, times(2)).schedule(taskCaptor.capture(), anyLong(), any());
        taskCaptor.getAllValues().get(1).run();
        manualClock.advance(Duration.ofHours(1));
        verify(jobScheduler, times(3)).schedule(taskCaptor.capture(), anyLong(), any());
        taskCaptor.getAllValues().get(2).run();
        manualClock.advance(Duration.ofHours(1));

        ArgumentCaptor<KVRangeRORequest> reqCaptor = ArgumentCaptor.forClass(KVRangeRORequest.class);
        verify(inboxStoreClient, times(3)).query(eq("store1"), any(KVRangeRORequest.class));

        verify(jobScheduler, times(4)).schedule(taskCaptor.capture(), anyLong(), any());
        taskCaptor.getAllValues().get(3).run();
        manualClock.advance(Duration.ofHours(1));
        verify(inboxStoreClient, times(4)).query(eq("store1"), reqCaptor.capture());
        List<KVRangeRORequest> reqs = reqCaptor.getAllValues();
        ArgumentCaptor<KVRangeRORequest> lastReqCaptor = ArgumentCaptor.forClass(KVRangeRORequest.class);
        verify(inboxStoreClient, times(4)).query(eq("store1"), lastReqCaptor.capture());

        verify(jobScheduler, times(5)).schedule(taskCaptor.capture(), anyLong(), any());
        taskCaptor.getAllValues().get(4).run();

        ArgumentCaptor<KVRangeRORequest> reqCaptor5 = ArgumentCaptor.forClass(KVRangeRORequest.class);
        verify(inboxStoreClient, times(5)).query(eq("store1"), reqCaptor5.capture());
    }

    @Test
    public void testNextStartKeyPropagation() {
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        when(jobScheduler.schedule(any(Runnable.class), anyLong(), any())).thenReturn(mockFuture);

        KVRangeSetting leaderRange = new KVRangeSetting("inbox.store", "store1", new HashMap<>() {
            {
                put("store1", KVRangeDescriptor.newBuilder().setId(KVRangeIdUtil.generate()).setVer(1).build());
            }
        });
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        router.put(FULL_BOUNDARY, leaderRange);
        when(inboxStoreClient.latestEffectiveRouter()).thenReturn(router);

        AtomicInteger count = new AtomicInteger();
        ByteString nextKey = ByteString.copyFromUtf8("next");
        when(inboxStoreClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenAnswer(inv -> {
                int c = count.incrementAndGet();
                GCReply reply;
                if (c == 1) {
                    reply = GCReply.newBuilder().setInspectedCount(1).setRemoveSuccess(0).setWrapped(false)
                        .setNextStartKey(nextKey).build();
                } else {
                    reply = GCReply.getDefaultInstance();
                }
                return CompletableFuture.completedFuture(KVRangeROReply.newBuilder().setCode(ReplyCode.Ok)
                    .setRoCoProcResult(ROCoProcOutput.newBuilder()
                        .setInboxService(InboxServiceROCoProcOutput.newBuilder().setGc(reply).build()).build())
                    .build());
            });

        cleaner.start("store1");
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(jobScheduler, times(1)).schedule(taskCaptor.capture(), anyLong(), any());
        taskCaptor.getValue().run();
        manualClock.advance(Duration.ofHours(1));
        verify(jobScheduler, times(2)).schedule(taskCaptor.capture(), anyLong(), any());
        taskCaptor.getAllValues().get(1).run();

        ArgumentCaptor<KVRangeRORequest> reqCaptor = ArgumentCaptor.forClass(KVRangeRORequest.class);
        verify(inboxStoreClient, times(2)).query(eq("store1"), reqCaptor.capture());
        KVRangeRORequest secondReq = reqCaptor.getAllValues().get(1);
        ByteString startKey = secondReq.getRoCoProc().getInboxService().getGc().getStartKey();
        assertEquals(nextKey, startKey);
    }

    @Test
    public void testRangeGaugesRegisteredAndRemovedOnLeadershipChange() {
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        when(jobScheduler.schedule(taskCaptor.capture(), anyLong(), any())).thenReturn(mockFuture);

        var rangeId = KVRangeIdUtil.generate();
        String rangeIdStr = KVRangeIdUtil.toString(rangeId);

        KVRangeSetting leaderRange = new KVRangeSetting("inbox.store", "store1", new HashMap<>() {
            {
                put("store1", KVRangeDescriptor.newBuilder().setId(rangeId).setVer(1).build());
            }
        });
        NavigableMap<Boundary, KVRangeSetting> router1 = new TreeMap<>(BoundaryUtil::compare);
        router1.put(FULL_BOUNDARY, leaderRange);

        KVRangeSetting otherLeaderRange = new KVRangeSetting("inbox.store", "store2", new HashMap<>() {
            {
                put("store2", KVRangeDescriptor.newBuilder().setId(rangeId).setVer(1).build());
            }
        });
        NavigableMap<Boundary, KVRangeSetting> router2 = new TreeMap<>(BoundaryUtil::compare);
        router2.put(FULL_BOUNDARY, otherLeaderRange);

        when(inboxStoreClient.latestEffectiveRouter()).thenReturn(router1, router2);

        // reply ok to avoid exceptions
        when(inboxStoreClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(KVRangeROReply.newBuilder()
                .setCode(ReplyCode.Ok)
                .setRoCoProcResult(ROCoProcOutput.newBuilder()
                    .setInboxService(InboxServiceROCoProcOutput.newBuilder()
                        .setGc(GCReply.getDefaultInstance()).build())
                    .build())
                .build()));

        cleaner.start("store1");

        assertEquals(taskCaptor.getAllValues().size(), 1);
        taskCaptor.getAllValues().get(0).run();

        assertNotNull(meterRegistry.find("inbox.gc.interval.ms")
            .tag("storeId", "store1")
            .tag("rangeId", rangeIdStr)
            .gauge());
        assertNotNull(meterRegistry.find("inbox.gc.step")
            .tag("storeId", "store1")
            .tag("rangeId", rangeIdStr)
            .gauge());

        manualClock.advance(Duration.ofHours(1));
        verify(jobScheduler, times(2)).schedule(any(Runnable.class), anyLong(), any());
        taskCaptor.getAllValues().get(1).run();

        assertNull(meterRegistry.find("inbox.gc.interval.ms")
            .tag("storeId", "store1")
            .tag("rangeId", rangeIdStr)
            .gauge());
        assertNull(meterRegistry.find("inbox.gc.step")
            .tag("storeId", "store1")
            .tag("rangeId", rangeIdStr)
            .gauge());
    }

    @Test
    public void testGaugesReflectIntervalAndStepChanges() {
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        when(jobScheduler.schedule(taskCaptor.capture(), anyLong(), any())).thenReturn(mockFuture);

        var rangeId = KVRangeIdUtil.generate();
        String rangeIdStr = KVRangeIdUtil.toString(rangeId);
        KVRangeSetting leaderRange = new KVRangeSetting("inbox.store", "store1", new HashMap<>() {
            {
                put("store1", KVRangeDescriptor.newBuilder().setId(rangeId).setVer(1).build());
            }
        });
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        router.put(FULL_BOUNDARY, leaderRange);
        when(inboxStoreClient.latestEffectiveRouter()).thenReturn(router);

        AtomicInteger callCount = new AtomicInteger();
        when(inboxStoreClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenAnswer(inv -> {
                int c = callCount.incrementAndGet();
                GCReply reply;
                if (c == 1) {
                    reply = GCReply.newBuilder().setInspectedCount(100).setRemoveSuccess(0).setWrapped(true).build();
                } else if (c == 2) {
                    reply = GCReply.newBuilder().setInspectedCount(512).setRemoveSuccess(0).setWrapped(false).build();
                } else if (c == 3) {
                    reply = GCReply.newBuilder().setInspectedCount(512).setRemoveSuccess(0).setWrapped(true).build();
                } else {
                    reply = GCReply.newBuilder().setInspectedCount(512).setRemoveSuccess(0).setWrapped(false).build();
                }
                return CompletableFuture.completedFuture(KVRangeROReply.newBuilder()
                    .setCode(ReplyCode.Ok)
                    .setRoCoProcResult(ROCoProcOutput.newBuilder()
                        .setInboxService(InboxServiceROCoProcOutput.newBuilder().setGc(reply).build())
                        .build())
                    .build());
            });

        cleaner.start("store1");

        taskCaptor.getAllValues().get(0).run();

        assertNotNull(meterRegistry.find("inbox.gc.interval.ms").tag("storeId", "store1").tag("rangeId", rangeIdStr)
            .gauge());
        assertEquals(meterRegistry.find("inbox.gc.interval.ms")
                .tag("storeId", "store1").tag("rangeId", rangeIdStr)
                .gauge().value(),
            100.0, 0.01);
        assertEquals(meterRegistry.find("inbox.gc.step").tag("storeId", "store1").tag("rangeId", rangeIdStr)
            .gauge().value(), 1.0, 0.01);

        manualClock.advance(Duration.ofHours(1));
        verify(jobScheduler, times(2)).schedule(any(Runnable.class), anyLong(), any());
        taskCaptor.getAllValues().get(1).run();

        manualClock.advance(Duration.ofHours(1));
        verify(jobScheduler, times(3)).schedule(any(Runnable.class), anyLong(), any());
        taskCaptor.getAllValues().get(2).run();

        manualClock.advance(Duration.ofHours(1));
        verify(jobScheduler, times(4)).schedule(any(Runnable.class), anyLong(), any());
        taskCaptor.getAllValues().get(3).run();

        assertEquals(meterRegistry.find("inbox.gc.step")
                .tag("storeId", "store1").tag("rangeId", rangeIdStr)
                .gauge().value(),
            2.0, 0.01);
        assertEquals(meterRegistry.find("inbox.gc.interval.ms")
                .tag("storeId", "store1").tag("rangeId", rangeIdStr)
                .gauge().value(),
            150.0, 0.01);
    }

    @Test
    public void testFirstSweepSprintQuotaAndIntervalThenSwitchOnWrapped() {
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        when(jobScheduler.schedule(taskCaptor.capture(), anyLong(), any())).thenReturn(mockFuture);

        KVRangeSetting leaderRange = new KVRangeSetting("inbox.store", "store1", new HashMap<>() {
            {
                put("store1", KVRangeDescriptor.newBuilder().setId(KVRangeIdUtil.generate()).setVer(1).build());
            }
        });
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        router.put(FULL_BOUNDARY, leaderRange);
        when(inboxStoreClient.latestEffectiveRouter()).thenReturn(router);

        AtomicInteger callCount = new AtomicInteger();
        when(inboxStoreClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenAnswer(inv -> {
                int c = callCount.incrementAndGet();
                GCReply reply;
                if (c == 1) {
                    reply = GCReply.newBuilder().setInspectedCount(100).setRemoveSuccess(0).setWrapped(false).build();
                } else if (c == 2) {
                    reply = GCReply.newBuilder().setInspectedCount(100).setRemoveSuccess(0).setWrapped(true).build();
                } else {
                    reply = GCReply.newBuilder().setInspectedCount(1).setRemoveSuccess(0).setWrapped(false).build();
                }
                return CompletableFuture.completedFuture(KVRangeROReply.newBuilder()
                    .setCode(ReplyCode.Ok)
                    .setRoCoProcResult(ROCoProcOutput.newBuilder()
                        .setInboxService(InboxServiceROCoProcOutput.newBuilder().setGc(reply).build())
                        .build())
                    .build());
            });

        cleaner.start("store1");
        verify(jobScheduler, times(1)).schedule(any(Runnable.class), anyLong(), any());
        taskCaptor.getAllValues().get(0).run();

        ArgumentCaptor<KVRangeRORequest> reqCaptor = ArgumentCaptor.forClass(KVRangeRORequest.class);
        verify(inboxStoreClient, times(1)).query(eq("store1"), reqCaptor.capture());
        KVRangeRORequest firstReq = reqCaptor.getValue();
        assertEquals(firstReq.getRoCoProc().getInboxService().getGc().getScanQuota(), 50_000);

        verify(jobScheduler, times(2)).schedule(any(Runnable.class), anyLong(), any());
        manualClock.advance(Duration.ofSeconds(5));
        taskCaptor.getAllValues().get(1).run();

        verify(inboxStoreClient, times(2)).query(eq("store1"), reqCaptor.capture());
        KVRangeRORequest secondReq = reqCaptor.getAllValues().get(1);
        assertEquals(secondReq.getRoCoProc().getInboxService().getGc().getScanQuota(), 50_000);

        final int[] runIdx = {2};
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            if (taskCaptor.getAllValues().size() > runIdx[0]) {
                manualClock.advance(Duration.ofMillis(100));
                taskCaptor.getAllValues().get(runIdx[0]).run();
                runIdx[0]++;
            }
            verify(inboxStoreClient, atLeast(3)).query(eq("store1"), reqCaptor.capture());
            KVRangeRORequest lastReq = reqCaptor.getAllValues().get(reqCaptor.getAllValues().size() - 1);
            assertEquals(lastReq.getRoCoProc().getInboxService().getGc().getScanQuota(), 512);
        });
    }

    @Test
    public void testSprintSurvivesFailureAndKeepsLargeQuota() {
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        when(jobScheduler.schedule(taskCaptor.capture(), anyLong(), any())).thenReturn(mockFuture);

        KVRangeSetting leaderRange = new KVRangeSetting("inbox.store", "store1", new HashMap<>() {
            {
                put("store1", KVRangeDescriptor.newBuilder().setId(KVRangeIdUtil.generate()).setVer(1).build());
            }
        });
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        router.put(FULL_BOUNDARY, leaderRange);
        when(inboxStoreClient.latestEffectiveRouter()).thenReturn(router);

        when(inboxStoreClient.query(eq("store1"), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("io error")))
            .thenReturn(CompletableFuture.completedFuture(KVRangeROReply.newBuilder()
                .setCode(ReplyCode.Ok)
                .setRoCoProcResult(ROCoProcOutput.newBuilder()
                    .setInboxService(InboxServiceROCoProcOutput.newBuilder()
                        .setGc(GCReply.newBuilder().setInspectedCount(1).setRemoveSuccess(0).setWrapped(false).build())
                        .build())
                    .build())
                .build()));

        cleaner.start("store1");
        verify(jobScheduler, times(1)).schedule(any(Runnable.class), anyLong(), any());
        taskCaptor.getAllValues().get(0).run();

        verify(jobScheduler, times(2)).schedule(any(Runnable.class), anyLong(), any());
        manualClock.advance(Duration.ofSeconds(5));
        taskCaptor.getAllValues().get(1).run();

        ArgumentCaptor<KVRangeRORequest> reqCaptor = ArgumentCaptor.forClass(KVRangeRORequest.class);
        verify(inboxStoreClient, times(2)).query(eq("store1"), reqCaptor.capture());
        KVRangeRORequest secondReq = reqCaptor.getAllValues().get(1);
        assertEquals(secondReq.getRoCoProc().getInboxService().getGc().getScanQuota(), 50_000);
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
