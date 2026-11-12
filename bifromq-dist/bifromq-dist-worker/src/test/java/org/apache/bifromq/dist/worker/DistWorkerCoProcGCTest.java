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

import static com.google.protobuf.ByteString.unsignedLexicographicalComparator;
import static org.apache.bifromq.basekv.store.util.KVUtil.toByteString;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.base.Ticker;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVRangeReader;
import org.apache.bifromq.basekv.store.api.IKVRangeRefreshableReader;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import org.apache.bifromq.dist.rpc.proto.GCReply;
import org.apache.bifromq.dist.rpc.proto.GCRequest;
import org.apache.bifromq.dist.rpc.proto.RouteGroup;
import org.apache.bifromq.dist.worker.cache.ISubscriptionCache;
import org.apache.bifromq.dist.worker.schema.KVSchemaUtil;
import org.apache.bifromq.plugin.subbroker.CheckRequest;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.util.TopicUtil;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DistWorkerCoProcGCTest {
    private ISubscriptionCache routeCache;
    private ISubscriptionCleaner subscriptionChecker;
    private IKVRangeReader reader;
    private DistWorkerCoProc coProc;
    private List<KV> data;
    private List<MatchInfo> checked;
    private AtomicLong elapsed;
    private long keyCostNanos;
    private long seekCostNanos;
    private MockedStatic<Ticker> tickerMock;
    private FakeIterator iterator;

    @BeforeMethod
    public void setUp() {
        data = new ArrayList<>();
        checked = new ArrayList<>();
        elapsed = new AtomicLong();
        keyCostNanos = 0;
        seekCostNanos = 0;
        Ticker ticker = new Ticker() {
            @Override
            public long read() {
                return elapsed.get();
            }
        };
        tickerMock = mockStatic(Ticker.class);
        tickerMock.when(Ticker::systemTicker).thenReturn(ticker);
        routeCache = mock(ISubscriptionCache.class);
        subscriptionChecker = mock(ISubscriptionCleaner.class);
        reader = mock(IKVRangeReader.class);
        when(reader.boundary()).thenReturn(FULL_BOUNDARY);
        when(reader.iterator()).thenAnswer(inv -> {
            iterator = new FakeIterator(data);
            return iterator;
        });
        IKVRangeRefreshableReader resetReader = mock(IKVRangeRefreshableReader.class);
        when(resetReader.iterator()).thenAnswer(inv -> new FakeIterator(List.of()));
        when(subscriptionChecker.sweep(anyInt(), any(CheckRequest.class))).thenAnswer(inv -> {
            CheckRequest request = inv.getArgument(1);
            checked.addAll(request.getMatchInfoList());
            return CompletableFuture.completedFuture(new ISubscriptionCleaner.GCStats(0, 0));
        });
        coProc = new DistWorkerCoProc(KVRangeId.newBuilder().setId(1).setEpoch(1).build(),
            () -> resetReader, routeCache, mock(ITenantsStats.class), mock(IDeliverExecutorGroup.class),
            subscriptionChecker);
        coProc.reset(FULL_BOUNDARY);
    }

    @AfterMethod
    public void tearDown() {
        tickerMock.close();
        coProc.close();
    }

    @Test
    public void gcEmptyRangeReturnsZeroStats() {
        GCReply reply = gc(null, 1, 2048).join();
        assertEquals(reply.getReqId(), 1001L);
        assertEquals(reply.getInspectedCount(), 0);
        assertEquals(reply.getRemoveSuccess(), 0);
        assertTrue(reply.getWrapped());
        assertFalse(reply.hasNextStartKey());
        assertTrue(checked.isEmpty());
    }

    @Test
    public void gcChecksEveryKeyDespiteLegacyStepHint() {
        addNormalRoutes(4);
        GCReply reply = gc(null, 3, 100).join();
        assertEquals(reply.getInspectedCount(), 4);
        assertEquals(checked.size(), 4);
        assertTrue(reply.getWrapped());
        assertFalse(reply.hasNextStartKey());
    }

    @DataProvider
    public Object[][] tableSizes() {
        return new Object[][] {{1}, {511}, {512}, {513}, {2047}, {2048}, {2049}, {4095}, {4096}, {4097}};
    }

    // A route table can end exactly on a batch boundary or between two batches.
    @Test(dataProvider = "tableSizes")
    public void gcVisitsEveryKeyOnceAcrossBatches(int size) {
        addNormalRoutes(size);
        ByteString cursor = null;
        int inspected = 0;
        do {
            GCReply reply = gc(cursor, 2, 2048).join();
            int batchSize = Math.min(2048, size - inspected);
            assertEquals(reply.getInspectedCount(), batchSize);
            inspected += batchSize;
            assertEquals(reply.getWrapped(), inspected == size);
            assertEquals(reply.hasNextStartKey(), inspected < size);
            if (inspected < size) {
                assertEquals(reply.getNextStartKey(), data.get(inspected).key());
                cursor = reply.getNextStartKey();
            }
        } while (inspected < size);
        assertEquals(checked.size(), size);
        assertEquals(new HashSet<>(checked).size(), size);
    }

    @DataProvider
    public Object[][] localCosts() {
        return new Object[][] {
            {0L, 0L, 2048}, {2_000L, 0L, 2048}, {10_000L, 0L, 1000},
            {30_000L, 0L, 512}, {0L, 20_000_000L, 512}
        };
    }

    // Cold reads and expensive Group records can exhaust the soft budget before the minimum batch.
    @Test(dataProvider = "localCosts")
    public void gcAdaptsToLocalWorkAfterMinimumBatch(long keyCost, long seekCost, int expected) {
        addNormalRoutes(4097);
        keyCostNanos = keyCost;
        seekCostNanos = seekCost;
        GCReply reply = gc(null, 1, 2048).join();
        assertEquals(reply.getInspectedCount(), expected);
        assertEquals(checked.size(), expected);
        assertEquals(reply.getNextStartKey(), data.get(expected).key());
        assertFalse(reply.getWrapped());
    }

    @DataProvider
    public Object[][] legacyQuotas() {
        return new Object[][] {{0, 2048}, {1, 1}, {128, 128}, {512, 512}, {50_000, 2048}, {-1, 2048}};
    }

    @Test(dataProvider = "legacyQuotas")
    public void gcHonorsUnsignedLegacyQuota(int quota, int expected) {
        addNormalRoutes(4097);
        GCReply reply = gc(null, -1, quota).join();
        assertEquals(reply.getInspectedCount(), expected);
        assertEquals(reply.getNextStartKey(), data.get(expected).key());
    }

    @Test
    public void gcRespectsSmallQuotaEvenWhenSeekUsesBudget() {
        addNormalRoutes(1024);
        seekCostNanos = 20_000_000L;
        assertEquals(gc(null, 1, 128).join().getInspectedCount(), 128);
    }

    @Test
    public void gcContinuesAfterBatchSizeChangesAndCursorDeletion() {
        addNormalRoutes(2049);
        GCReply first = gc(null, 1, 512).join();
        ByteString cursor = first.getNextStartKey();
        // Unsubscribes remove already visited routes and the next cursor before the next query.
        data.subList(0, 513).clear();
        GCReply second = gc(cursor, 1, 2048).join();
        assertEquals(second.getInspectedCount(), 1536);
        assertTrue(second.getWrapped());
        assertFalse(second.hasNextStartKey());
        assertEquals(checked.size(), 2048);
        assertEquals(new HashSet<>(checked).size(), 2048);
    }

    @Test
    public void gcDoesNotWrapWithinTheSameRequest() {
        addNormalRoutes(4);
        GCReply tail = gc(data.get(3).key(), 1, 2048).join();
        assertEquals(tail.getInspectedCount(), 1);
        assertTrue(tail.getWrapped());
        assertFalse(tail.hasNextStartKey());
        ByteString deletedTail = data.remove(3).key();
        GCReply emptyTail = gc(deletedTail, 1, 2048).join();
        assertEquals(emptyTail.getInspectedCount(), 0);
        assertTrue(emptyTail.getWrapped());
    }

    @Test
    public void gcClampsAnOldCursorAfterRangeChanges() {
        addNormalRoutes(4);
        ByteString beforeStart = data.get(0).key();
        ByteString start = data.get(1).key();
        ByteString end = data.get(3).key();
        Boundary boundary = Boundary.newBuilder().setStartKey(start).setEndKey(end).build();
        coProc.reset(boundary);
        data = new ArrayList<>(data.subList(1, 3));
        for (ByteString oldCursor : List.of(beforeStart, end)) {
            GCReply reply = gc(oldCursor, 1, 1).join();
            assertEquals(reply.getInspectedCount(), 1);
            assertEquals(reply.getNextStartKey(), data.get(1).key());
        }
    }

    // Active topics need no extra Check; they must still consume quota and advance the cursor.
    @Test
    public void gcSkipsCachedNormalRoutesButAdvancesCursor() {
        addNormalRoutes(513);
        when(routeCache.isCached(anyString(), anyList())).thenReturn(true);
        GCReply first = gc(null, 1, 512).join();
        assertEquals(first.getInspectedCount(), 512);
        assertEquals(first.getRemoveSuccess(), 0);
        assertFalse(first.getWrapped());
        assertEquals(first.getNextStartKey(), data.get(512).key());
        verifyNoInteractions(subscriptionChecker);

        // The next route has no cached topic, so the next batch must check it.
        when(routeCache.isCached("t", TopicUtil.from("q/00512").getFilterLevelList())).thenReturn(false);
        GCReply last = gc(first.getNextStartKey(), 1, 512).join();
        assertEquals(last.getInspectedCount(), 1);
        assertTrue(last.getWrapped());
        assertFalse(last.hasNextStartKey());
        assertEquals(checked.size(), 1);
        assertEquals(checked.get(0).getIncarnation(), 7L);

        // When traffic stops and the cache expires, the next pass checks those routes too.
        when(routeCache.isCached(anyString(), anyList())).thenReturn(false);
        checked.clear();
        assertEquals(gc(null, 1, 2048).join().getInspectedCount(), 513);
        assertEquals(checked.size(), 513);
    }

    @DataProvider
    public Object[][] cacheStates() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "cacheStates")
    public void gcGroupKeyCountsOnceAndOnlyUncachedGroupIsChecked(boolean cached) {
        when(routeCache.isCached(anyString(), anyList())).thenReturn(cached);
        ByteString key = KVSchemaUtil.toGroupRouteKey("t", TopicUtil.from("$share/group/test/x"));
        RouteGroup members = RouteGroup.newBuilder()
            .putMembers("1\0ix\0d1", 1L).putMembers("2\0iy\0d2", 2L).build();
        data.add(new KV(key, members.toByteString()));
        when(subscriptionChecker.sweep(anyInt(), any(CheckRequest.class))).thenAnswer(inv -> {
            CheckRequest request = inv.getArgument(1);
            return CompletableFuture.completedFuture(
                new ISubscriptionCleaner.GCStats(request.getMatchInfoCount(), request.getMatchInfoCount()));
        });
        GCReply reply = gc(null, 1, 2048).join();
        assertEquals(reply.getInspectedCount(), 1);
        assertTrue(reply.getWrapped());
        assertFalse(reply.hasNextStartKey());
        if (cached) {
            assertEquals(reply.getRemoveSuccess(), 0);
            verifyNoInteractions(subscriptionChecker);
            return;
        }
        assertEquals(reply.getRemoveSuccess(), 2);
        ArgumentCaptor<Integer> brokers = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<CheckRequest> requests = ArgumentCaptor.forClass(CheckRequest.class);
        verify(subscriptionChecker, times(2)).sweep(brokers.capture(), requests.capture());
        for (int i = 0; i < 2; i++) {
            int broker = brokers.getAllValues().get(i);
            CheckRequest request = requests.getAllValues().get(i);
            assertEquals(request.getTenantId(), "t");
            assertEquals(request.getDelivererKey(), "d" + broker);
            assertEquals(request.getMatchInfoCount(), 1);
            assertEquals(request.getMatchInfo(0).getIncarnation(), (long) broker);
        }
    }

    // Check/delete RPCs may finish after the iterator closes; their latency must not reduce the next batch.
    @Test
    public void gcSnapshotsProgressBeforeWaitingForSweep() {
        addNormalRoutes(4097);
        CompletableFuture<ISubscriptionCleaner.GCStats> sweep = new CompletableFuture<>();
        when(subscriptionChecker.sweep(anyInt(), any(CheckRequest.class))).thenReturn(sweep);
        CompletableFuture<GCReply> pending = gc(null, 1, 2048);
        assertFalse(pending.isDone());
        assertTrue(iterator.closed);
        elapsed.addAndGet(15_000_000L);
        ByteString cursor = data.get(2048).key();
        data.remove(2048);
        sweep.complete(new ISubscriptionCleaner.GCStats(10, 7));
        GCReply reply = pending.join();
        assertEquals(reply.getInspectedCount(), 2048);
        assertEquals(reply.getRemoveSuccess(), 7);
        assertEquals(reply.getNextStartKey(), cursor);
        assertEquals(gc(cursor, 1, 2048).join().getInspectedCount(), 2048);
    }

    private CompletableFuture<GCReply> gc(ByteString cursor, int stepHint, int quota) {
        GCRequest.Builder request = GCRequest.newBuilder().setReqId(1001).setStepHint(stepHint).setScanQuota(quota);
        if (cursor != null) {
            request.setStartKey(cursor);
        }
        return coProc.query(ROCoProcInput.newBuilder()
            .setDistService(DistServiceROCoProcInput.newBuilder().setGc(request)).build(), reader)
            .thenApply(output -> output.getDistService().getGc());
    }

    private void addNormalRoutes(int size) {
        for (int i = 0; i < size; i++) {
            String suffix = String.format("%05d", i);
            data.add(new KV(KVSchemaUtil.toNormalRouteKey("t", TopicUtil.from("q/" + suffix),
                "1\0i" + suffix + "\0d1"), toByteString(7L)));
        }
        data.sort(Comparator.comparing(KV::key, unsignedLexicographicalComparator()));
    }

    private record KV(ByteString key, ByteString val) {
    }

    private class FakeIterator implements IKVIterator {
        private final List<KV> snapshot;
        private int index = -1;
        private boolean closed;

        FakeIterator(List<KV> records) {
            snapshot = new ArrayList<>(records);
            snapshot.sort(Comparator.comparing(KV::key, unsignedLexicographicalComparator()));
        }

        @Override
        public ByteString key() {
            assertFalse(closed);
            return snapshot.get(index).key();
        }

        @Override
        public ByteString value() {
            assertFalse(closed);
            return snapshot.get(index).val();
        }

        @Override
        public boolean isValid() {
            assertFalse(closed);
            return index >= 0 && index < snapshot.size();
        }

        @Override
        public void next() {
            elapsed.addAndGet(keyCostNanos);
            index++;
        }

        @Override
        public void prev() {
            index--;
        }

        @Override
        public void seekToFirst() {
            elapsed.addAndGet(seekCostNanos);
            index = 0;
        }

        @Override
        public void seekToLast() {
            index = snapshot.size() - 1;
        }

        @Override
        public void seek(ByteString key) {
            elapsed.addAndGet(seekCostNanos);
            index = 0;
            while (isValid() && unsignedLexicographicalComparator().compare(key(), key) < 0) {
                index++;
            }
        }

        @Override
        public void seekForPrev(ByteString key) {
            index = snapshot.size() - 1;
            while (isValid() && unsignedLexicographicalComparator().compare(key(), key) > 0) {
                index--;
            }
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
