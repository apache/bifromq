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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import org.apache.bifromq.dist.rpc.proto.GCReply;
import org.apache.bifromq.dist.rpc.proto.GCRequest;
import org.apache.bifromq.dist.rpc.proto.RouteGroup;
import org.apache.bifromq.dist.worker.cache.ISubscriptionCache;
import org.apache.bifromq.dist.worker.schema.KVSchemaUtil;
import org.apache.bifromq.plugin.subbroker.CheckRequest;
import org.apache.bifromq.util.TopicUtil;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DistWorkerCoProcGCTest {
    private ISubscriptionCache routeCache;
    private ITenantsStats tenantsState;
    private IDeliverExecutorGroup deliverExecutorGroup;
    private ISubscriptionCleaner subscriptionChecker;
    private Supplier<IKVCloseableReader> readerProvider;
    private IKVCloseableReader reader;
    private DistWorkerCoProc coProc;
    private List<KV> currentData = new ArrayList<>();

    private static ByteString normalKey(String tenantId, String topic, String receiverUrl) {
        return KVSchemaUtil.toNormalRouteKey(tenantId, TopicUtil.from(topic), receiverUrl);
    }

    private static ByteString groupKey(String tenantId, String sharedTopic) {
        return KVSchemaUtil.toGroupRouteKey(tenantId, TopicUtil.from(sharedTopic));
    }

    @BeforeMethod
    public void setUp() {
        routeCache = mock(ISubscriptionCache.class);
        tenantsState = mock(ITenantsStats.class);
        deliverExecutorGroup = mock(IDeliverExecutorGroup.class);
        subscriptionChecker = mock(ISubscriptionCleaner.class);
        readerProvider = mock(Supplier.class);
        reader = mock(IKVCloseableReader.class);
        when(reader.boundary()).thenReturn(FULL_BOUNDARY);
        when(readerProvider.get()).thenReturn(reader);
        when(reader.iterator()).thenReturn(new FakeIterator(java.util.List.of()));
        when(routeCache.isCached(anyString(), anyList())).thenReturn(false);
        coProc = new DistWorkerCoProc(KVRangeId.newBuilder().setId(1).setEpoch(1).build(),
            readerProvider, routeCache, tenantsState, deliverExecutorGroup, subscriptionChecker);
        coProc.reset(FULL_BOUNDARY);
        coProc.onLeader(true);
    }

    private ROCoProcOutput gc(long reqId, Integer stepHint, Integer scanQuota) {
        when(reader.iterator()).thenReturn(new FakeIterator(currentData));
        GCRequest.Builder req = GCRequest.newBuilder().setReqId(reqId);
        if (stepHint != null) {
            req.setStepHint(stepHint);
        }
        if (scanQuota != null) {
            req.setScanQuota(scanQuota);
        }
        ROCoProcInput in = ROCoProcInput.newBuilder()
            .setDistService(DistServiceROCoProcInput.newBuilder().setGc(req.build()).build()).build();
        CompletableFuture<ROCoProcOutput> f = coProc.query(in, reader);
        return f.join();
    }

    @Test
    public void gcEmptyRangeReturnsZeroStats() {
        currentData.clear();
        when(subscriptionChecker.sweep(any(Integer.class), any(CheckRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(new ISubscriptionCleaner.GCStats(0, 0)));

        ROCoProcOutput out = gc(1001L, 1, 10);
        GCReply r = out.getDistService().getGc();
        assertEquals(r.getReqId(), 1001L);
        assertEquals(r.getInspectedCount(), 0);
        assertEquals(r.getRemoveSuccess(), 0);
        assertFalse(r.getWrapped());
        assertFalse(r.hasNextStartKey());
    }

    @Test
    public void gcStartFromFirstWhenNoStartKey() {
        currentData.clear();
        // two normal keys
        currentData.add(new KV(normalKey("t1", "a/b", "1\0inbox1\0d1"), toByteString(1L)));
        currentData.add(new KV(normalKey("t2", "c/d", "2\0inbox2\0d2"), toByteString(1L)));
        when(subscriptionChecker.sweep(any(Integer.class), any(CheckRequest.class)))
            .thenAnswer(inv -> {
                CheckRequest req = inv.getArgument(1);
                return CompletableFuture.completedFuture(new ISubscriptionCleaner.GCStats(req.getMatchInfoCount(), 0));
            });

        ROCoProcOutput out = gc(1002L, 1, 2);
        GCReply r = out.getDistService().getGc();
        assertEquals(r.getInspectedCount(), 2);
        assertFalse(r.getWrapped());
        // nextStartKey set only when iterator still valid; with two keys and quota=2, it's invalid
        assertFalse(r.hasNextStartKey());
    }

    @Test
    public void gcRespectStepHintSkipping() {
        currentData.clear();
        currentData.add(new KV(normalKey("t", "k/1", "1\0i1\0d1"), toByteString(1L)));
        currentData.add(new KV(normalKey("t", "k/2", "1\0i2\0d1"), toByteString(1L)));
        currentData.add(new KV(normalKey("t", "k/3", "1\0i3\0d1"), toByteString(1L)));
        currentData.add(new KV(normalKey("t", "k/4", "1\0i4\0d1"), toByteString(1L)));
        when(subscriptionChecker.sweep(any(Integer.class), any(CheckRequest.class)))
            .thenAnswer(inv -> {
                CheckRequest req = inv.getArgument(1);
                return CompletableFuture.completedFuture(new ISubscriptionCleaner.GCStats(req.getMatchInfoCount(), 0));
            });

        ROCoProcOutput out = gc(1003L, 3, 100);
        GCReply r = out.getDistService().getGc();
        // 4 keys with step 3 => inspected 2
        assertEquals(r.getInspectedCount(), 2);
    }

    @Test
    public void gcStopsOnQuota() {
        currentData.clear();
        for (int i = 0; i < 6; i++) {
            currentData.add(new KV(normalKey("t", "q/" + i, "1\0i" + i + "\0d1"), toByteString(1L)));
        }
        when(subscriptionChecker.sweep(any(Integer.class), any(CheckRequest.class)))
            .thenAnswer(inv -> {
                CheckRequest req = inv.getArgument(1);
                return CompletableFuture.completedFuture(new ISubscriptionCleaner.GCStats(req.getMatchInfoCount(), 0));
            });

        ROCoProcOutput out = gc(1004L, 1, 3);
        GCReply r = out.getDistService().getGc();
        assertEquals(r.getInspectedCount(), 3);
    }

    @Test
    public void gcWrapAndStopOnSessionStartKey() {
        currentData.clear();
        currentData.add(new KV(normalKey("t", "w/1", "1\0ia\0d1"), toByteString(1L)));
        currentData.add(new KV(normalKey("t", "w/2", "1\0ib\0d1"), toByteString(1L)));
        currentData.add(new KV(normalKey("t", "w/3", "1\0ic\0d1"), toByteString(1L)));
        when(subscriptionChecker.sweep(any(Integer.class), any(CheckRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(new ISubscriptionCleaner.GCStats(1, 0)));

        ROCoProcOutput out = gc(1005L, 2, 100);
        GCReply r = out.getDistService().getGc();
        assertTrue(r.getWrapped());
        // inspected should be >=1 and <= data size
        assertTrue(r.getInspectedCount() >= 1);
    }

    @Test
    public void gcGroupKeyCountsOnceAndSweepExpandsReceivers() {
        currentData.clear();
        String sharedTopic = "$share/group/test/x";
        ByteString gk = groupKey("t", sharedTopic);
        RouteGroup members = RouteGroup.newBuilder()
            .putMembers("1\0ix\0d1", 1L)
            .putMembers("2\0iy\0d2", 1L)
            .build();
        currentData.add(new KV(gk, members.toByteString()));
        when(subscriptionChecker.sweep(any(Integer.class), any(CheckRequest.class)))
            .thenAnswer(inv -> {
                CheckRequest req = inv.getArgument(1);
                return CompletableFuture.completedFuture(new ISubscriptionCleaner.GCStats(req.getMatchInfoCount(), 0));
            });

        ROCoProcOutput out = gc(1006L, 1, 10);
        GCReply r = out.getDistService().getGc();

        // verify two sweeps were issued in total across subBrokers
        ArgumentCaptor<CheckRequest> reqCaptor = ArgumentCaptor.forClass(CheckRequest.class);
        verify(subscriptionChecker, atLeast(1)).sweep(any(Integer.class), reqCaptor.capture());
        // sum attempts across captured requests equals total receivers
        int attempts = reqCaptor.getAllValues().stream().mapToInt(CheckRequest::getMatchInfoCount).sum();
        assertEquals(attempts, 2);
    }

    private record KV(ByteString key, ByteString val) {}

    private static class FakeIterator implements IKVIterator {
        private final List<KV> data;
        private int idx = -1;

        FakeIterator(List<KV> list) {
            this.data = new ArrayList<>(list);
            this.data.sort(Comparator.comparing(KV::key, unsignedLexicographicalComparator()));
        }

        @Override
        public ByteString key() {
            return isValid() ? data.get(idx).key() : null;
        }

        @Override
        public ByteString value() {
            return isValid() ? data.get(idx).val() : null;
        }

        @Override
        public boolean isValid() {
            return idx >= 0 && idx < data.size();
        }

        @Override
        public void next() {
            idx++;
        }

        @Override
        public void prev() {
            idx--;
        }

        @Override
        public void seekToFirst() {
            idx = data.isEmpty() ? -1 : 0;
        }

        @Override
        public void seekToLast() {
            idx = data.isEmpty() ? -1 : data.size() - 1;
        }

        @Override
        public void seek(ByteString key) {
            // find first >= key
            int i = 0;
            while (i < data.size()
                && unsignedLexicographicalComparator().compare(data.get(i).key(), key) < 0) {
                i++;
            }
            idx = (i < data.size()) ? i : -1;
        }

        @Override
        public void seekForPrev(ByteString key) {
            // find last <= key
            int i = data.size() - 1;
            while (i >= 0 && unsignedLexicographicalComparator().compare(data.get(i).key(), key) > 0) {
                i--;
            }
            idx = (i >= 0) ? i : -1;
        }
    }
}
