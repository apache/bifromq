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
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toReceiverUrl;
import static org.apache.bifromq.util.BSUtil.toByteString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVRangeCoProc;
import org.apache.bifromq.basekv.store.api.IKVWriter;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.dist.rpc.proto.BatchDistReply;
import org.apache.bifromq.dist.rpc.proto.BatchDistRequest;
import org.apache.bifromq.dist.rpc.proto.BatchMatchReply;
import org.apache.bifromq.dist.rpc.proto.BatchMatchRequest;
import org.apache.bifromq.dist.rpc.proto.BatchUnmatchReply;
import org.apache.bifromq.dist.rpc.proto.BatchUnmatchRequest;
import org.apache.bifromq.dist.rpc.proto.DistPack;
import org.apache.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import org.apache.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import org.apache.bifromq.dist.rpc.proto.Fact;
import org.apache.bifromq.dist.rpc.proto.MatchRoute;
import org.apache.bifromq.dist.rpc.proto.TenantOption;
import org.apache.bifromq.dist.worker.cache.ISubscriptionCache;
import org.apache.bifromq.dist.worker.cache.task.AddRoutesTask;
import org.apache.bifromq.dist.worker.cache.task.RefreshEntriesTask;
import org.apache.bifromq.dist.worker.cache.task.RemoveRoutesTask;
import org.apache.bifromq.dist.worker.schema.KVSchemaUtil;
import org.apache.bifromq.dist.worker.schema.cache.Matching;
import org.apache.bifromq.type.TopicMessagePack;
import org.apache.bifromq.util.BSUtil;
import org.apache.bifromq.util.TopicUtil;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DistWorkerCoProcTest {

    private ISubscriptionCache routeCache;
    private ITenantsStats tenantsState;
    private IDeliverExecutorGroup deliverExecutorGroup;
    private ISubscriptionCleaner subscriptionChecker;
    private Supplier<IKVCloseableReader> readerProvider;
    private IKVCloseableReader reader;
    private IKVWriter writer;
    private IKVIterator iterator;
    private KVRangeId rangeId;
    private DistWorkerCoProc distWorkerCoProc;

    @BeforeMethod
    public void setUp() {
        routeCache = mock(ISubscriptionCache.class);
        tenantsState = mock(ITenantsStats.class);
        deliverExecutorGroup = mock(IDeliverExecutorGroup.class);
        subscriptionChecker = mock(ISubscriptionCleaner.class);
        readerProvider = mock(Supplier.class);
        reader = mock(IKVCloseableReader.class);
        iterator = mock(IKVIterator.class);
        writer = mock(IKVWriter.class);
        rangeId = KVRangeId.newBuilder().setId(1).setEpoch(1).build();
        when(readerProvider.get()).thenReturn(reader);
        when(reader.boundary()).thenReturn(FULL_BOUNDARY);
        when(reader.iterator()).thenReturn(iterator);
        when(iterator.isValid()).thenReturn(false);
        distWorkerCoProc = new DistWorkerCoProc(rangeId, readerProvider, routeCache, tenantsState, deliverExecutorGroup,
            subscriptionChecker);
        distWorkerCoProc.reset(FULL_BOUNDARY);
        distWorkerCoProc.onLeader(true);
    }

    @Test
    public void testMutateBatchAddRoute() {
        RWCoProcInput rwCoProcInput = RWCoProcInput.newBuilder().setDistService(DistServiceRWCoProcInput.newBuilder()
            .setBatchMatch(BatchMatchRequest.newBuilder().setReqId(123).putRequests("tenant1",
                    BatchMatchRequest.TenantBatch.newBuilder()
                        .setOption(TenantOption.newBuilder().setMaxReceiversPerSharedSubGroup(10).build()).addRoute(
                            MatchRoute.newBuilder().setMatcher(TopicUtil.from("topicFilter1")).setBrokerId(1)
                                .setReceiverId("inbox1").setDelivererKey("deliverer1").setIncarnation(1L).build()).build())
                .putRequests("tenant2", BatchMatchRequest.TenantBatch.newBuilder()
                    .setOption(TenantOption.newBuilder().setMaxReceiversPerSharedSubGroup(5).build()).addRoute(
                        MatchRoute.newBuilder().setMatcher(TopicUtil.from("topicFilter2")).setBrokerId(1)
                            .setReceiverId("inbox2").setDelivererKey("deliverer2").setIncarnation(1L).build()).build())
                .build()).build()).build();

        when(reader.exist(any(ByteString.class))).thenReturn(false);

        // Simulate mutation
        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer,
            true);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        verify(writer, times(2)).put(any(), eq(toByteString(1L)));
        // Verify that matches are added to the cache (may refresh added/removed maps)
        verify(routeCache, atLeast(1)).refresh(any());

        // Verify that tenant state is updated for both tenants
        verify(tenantsState, times(1)).incNormalRoutes(eq("tenant1"), eq(1));
        verify(tenantsState, times(1)).incNormalRoutes(eq("tenant2"), eq(1));

        // Check the result output
        BatchMatchReply reply = result.output().getDistService().getBatchMatch();
        assertEquals(reply.getReqId(), 123);
        assertEquals(reply.getResultsOrThrow("tenant1").getCode(0), BatchMatchReply.TenantBatch.Code.OK);
        assertEquals(reply.getResultsOrThrow("tenant2").getCode(0), BatchMatchReply.TenantBatch.Code.OK);
    }

    @Test
    public void testMutateBatchRemoveRoute() {
        long incarnation = 1;
        RWCoProcInput rwCoProcInput = RWCoProcInput.newBuilder().setDistService(DistServiceRWCoProcInput.newBuilder()
            .setBatchUnmatch(BatchUnmatchRequest.newBuilder().setReqId(456).putRequests("tenant1",
                BatchUnmatchRequest.TenantBatch.newBuilder().addRoute(
                        MatchRoute.newBuilder().setMatcher(TopicUtil.from("topicFilter1")).setBrokerId(1)
                            .setReceiverId("inbox1").setDelivererKey("deliverer1").setIncarnation(incarnation).build())
                    .build()).build()).build()).build();

        // Simulate match exists in the reader
        when(reader.get(any(ByteString.class))).thenReturn(Optional.of(toByteString(1L)));

        // Simulate mutation
        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer,
            true);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        // Verify that matches are removed from the cache
        verify(routeCache, times(1)).refresh(
            argThat(m -> m.containsKey("tenant1") && m.get("tenant1").routes.keySet()
                .contains(TopicUtil.from("topicFilter1"))));

        // Verify that tenant state is updated
        verify(tenantsState, times(1)).decNormalRoutes(eq("tenant1"), eq(1));

        // Check the result output
        BatchUnmatchReply reply = result.output().getDistService().getBatchUnmatch();
        assertEquals(reply.getReqId(), 456);
        assertEquals(reply.getResultsOrThrow("tenant1").getCode(0), BatchUnmatchReply.TenantBatch.Code.OK);
    }

    @Test
    public void testMutateBatchAddRouteReturnsCorrectFact() {
        ByteString keyFirst = KVSchemaUtil.toNormalRouteKey("tenantA", TopicUtil.from("topicA"), "receiver1");
        ByteString keyLast = KVSchemaUtil.toNormalRouteKey("tenantB", TopicUtil.from("topicB"), "receiver2");

        when(iterator.isValid()).thenReturn(true, true, false);
        when(iterator.key()).thenReturn(keyFirst, keyLast);

        RWCoProcInput rwCoProcInput = RWCoProcInput.newBuilder().setDistService(DistServiceRWCoProcInput.newBuilder()
            .setBatchMatch(BatchMatchRequest.newBuilder().setReqId(1001).putRequests("tenant1",
                    BatchMatchRequest.TenantBatch.newBuilder()
                        .setOption(TenantOption.newBuilder().setMaxReceiversPerSharedSubGroup(10).build()).addRoute(
                            MatchRoute.newBuilder().setMatcher(TopicUtil.from("topicFilter1")).setBrokerId(1)
                                .setReceiverId("inbox1").setDelivererKey("deliverer1").setIncarnation(1L).build()).build())
                .putRequests("tenant2", BatchMatchRequest.TenantBatch.newBuilder()
                    .setOption(TenantOption.newBuilder().setMaxReceiversPerSharedSubGroup(5).build()).addRoute(
                        MatchRoute.newBuilder().setMatcher(TopicUtil.from("topicFilter2")).setBrokerId(1)
                            .setReceiverId("inbox2").setDelivererKey("deliverer2").setIncarnation(1L).build()).build())
                .build()).build()).build();

        when(reader.get(any(ByteString.class))).thenReturn(Optional.empty());

        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer,
            true);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        verify(writer, times(2)).put(any(), eq(BSUtil.toByteString(1L)));
        verify(routeCache, atLeast(1)).refresh(any());
        verify(tenantsState, times(1)).incNormalRoutes(eq("tenant1"), eq(1));
        verify(tenantsState, times(1)).incNormalRoutes(eq("tenant2"), eq(1));

        try {
            Fact fact = result.fact().get().unpack(Fact.class);
            assertTrue(fact.hasFirstGlobalFilterLevels());
            assertTrue(fact.hasLastGlobalFilterLevels());
            assertEquals(fact.getFirstGlobalFilterLevels().getFilterLevel(0), "tenantA");
            assertEquals(fact.getLastGlobalFilterLevels().getFilterLevel(0), "tenantB");
        } catch (InvalidProtocolBufferException e) {
            fail("Failed to unpack Fact", e);
        }
    }

    @Test
    public void testMutateBatchRemoveRouteReturnsCorrectFact() {
        ByteString keyFirst = KVSchemaUtil.toNormalRouteKey("tenantA", TopicUtil.from("topicA"), "receiver1");
        ByteString keyLast = KVSchemaUtil.toNormalRouteKey("tenantB", TopicUtil.from("topicB"), "receiver2");

        when(iterator.isValid()).thenReturn(true, true, false);
        when(iterator.key()).thenReturn(keyFirst, keyLast);

        long incarnation = 1;
        RWCoProcInput rwCoProcInput = RWCoProcInput.newBuilder().setDistService(DistServiceRWCoProcInput.newBuilder()
            .setBatchUnmatch(BatchUnmatchRequest.newBuilder().setReqId(1002).putRequests("tenant1",
                BatchUnmatchRequest.TenantBatch.newBuilder().addRoute(
                        MatchRoute.newBuilder().setMatcher(TopicUtil.from("topicFilter1")).setBrokerId(1)
                            .setReceiverId("inbox1").setDelivererKey("deliverer1").setIncarnation(incarnation).build())
                    .build()).build()).build()).build();

        when(reader.get(any(ByteString.class))).thenReturn(Optional.of(BSUtil.toByteString(incarnation)));

        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer,
            true);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        verify(writer, times(1)).delete(any(ByteString.class));
        verify(routeCache, times(1)).refresh(
            argThat(m -> m.containsKey("tenant1") && m.get("tenant1").routes.keySet()
                .contains(TopicUtil.from("topicFilter1"))));
        verify(tenantsState, times(1)).decNormalRoutes(eq("tenant1"), eq(1));

        try {
            Fact fact = result.fact().get().unpack(Fact.class);
            assertTrue(fact.hasFirstGlobalFilterLevels());
            assertTrue(fact.hasLastGlobalFilterLevels());
            assertEquals(fact.getFirstGlobalFilterLevels().getFilterLevel(0), "tenantA");
            assertEquals(fact.getLastGlobalFilterLevels().getFilterLevel(0), "tenantB");
        } catch (InvalidProtocolBufferException e) {
            fail("Failed to unpack Fact", e);
        }
    }

    @SneakyThrows
    @Test
    public void testAddRouteExceedFirstRouteTriggersRefresh() {
        ByteString keyFirst = KVSchemaUtil.toNormalRouteKey("tenantA", TopicUtil.from("topicA"), "receiverA");
        ByteString keyLast = KVSchemaUtil.toNormalRouteKey("tenantB", TopicUtil.from("topicB"), "receiverB");
        when(iterator.isValid()).thenReturn(true, true, false);
        when(iterator.key()).thenReturn(keyFirst, keyLast);
        Fact initialFact = distWorkerCoProc.reset(FULL_BOUNDARY).unpack(Fact.class);
        assertEquals(initialFact.getFirstGlobalFilterLevels().getFilterLevelList(), List.of("tenantA", "topicA"));
        assertEquals(initialFact.getLastGlobalFilterLevels().getFilterLevelList(), List.of("tenantB", "topicB"));

        RWCoProcInput rwCoProcInput = RWCoProcInput.newBuilder()
            .setDistService(DistServiceRWCoProcInput.newBuilder()
                .setBatchMatch(BatchMatchRequest.newBuilder()
                    .setReqId(2001)
                    .putRequests("tenant0", BatchMatchRequest.TenantBatch.newBuilder()
                        .setOption(TenantOption.newBuilder().setMaxReceiversPerSharedSubGroup(10).build())
                        .addRoute(MatchRoute.newBuilder()
                            .setMatcher(TopicUtil.from("topicFilter0"))
                            .setBrokerId(1)
                            .setReceiverId("inbox0")
                            .setDelivererKey("deliverer0")
                            .setIncarnation(1L)
                            .build())
                        .build())
                    .build())
                .build())
            .build();

        when(reader.get(any(ByteString.class))).thenReturn(Optional.empty());

        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer,
            true);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        verify(writer, times(1)).put(any(), eq(BSUtil.toByteString(1L)));
        // at least once (added map refresh); there may also be an empty removed map refresh
        verify(routeCache, atLeast(1)).refresh(any());
        verify(tenantsState, times(1)).incNormalRoutes(eq("tenant0"), eq(1));

        verify(reader, times(1)).refresh();
    }

    @SneakyThrows
    @Test
    public void testAddRouteExceedLastRouteTriggersRefresh() {
        ByteString keyFirst = KVSchemaUtil.toNormalRouteKey("tenantA", TopicUtil.from("topicA"), "receiverA");
        ByteString keyLast = KVSchemaUtil.toNormalRouteKey("tenantB", TopicUtil.from("topicB"), "receiverB");
        when(iterator.isValid()).thenReturn(true, true, false);
        when(iterator.key()).thenReturn(keyFirst, keyLast);
        Fact initialFact = distWorkerCoProc.reset(FULL_BOUNDARY).unpack(Fact.class);
        distWorkerCoProc.onLeader(true);
        assertEquals(initialFact.getFirstGlobalFilterLevels().getFilterLevelList(), List.of("tenantA", "topicA"));
        assertEquals(initialFact.getLastGlobalFilterLevels().getFilterLevelList(), List.of("tenantB", "topicB"));

        RWCoProcInput rwCoProcInput = RWCoProcInput.newBuilder()
            .setDistService(DistServiceRWCoProcInput.newBuilder()
                .setBatchMatch(BatchMatchRequest.newBuilder()
                    .setReqId(2001)
                    .putRequests("tenantC", BatchMatchRequest.TenantBatch.newBuilder()
                        .setOption(TenantOption.newBuilder().setMaxReceiversPerSharedSubGroup(10).build())
                        .addRoute(MatchRoute.newBuilder()
                            .setMatcher(TopicUtil.from("topicFilter0"))
                            .setBrokerId(1)
                            .setReceiverId("inbox0")
                            .setDelivererKey("deliverer0")
                            .setIncarnation(1L)
                            .build())
                        .build())
                    .build())
                .build())
            .build();

        when(reader.get(any(ByteString.class))).thenReturn(Optional.empty());

        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer,
            true);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        verify(writer, times(1)).put(any(), eq(BSUtil.toByteString(1L)));
        verify(routeCache, atLeast(1)).refresh(any());
        verify(tenantsState, times(1)).incNormalRoutes(eq("tenantC"), eq(1));

        verify(reader, times(1)).refresh();
    }

    @SneakyThrows
    @Test
    public void testRemoveRouteAffectFirstRouteTriggersRefresh() {
        ByteString keyFirst = KVSchemaUtil.toNormalRouteKey("tenantA", TopicUtil.from("topicA"), "receiverA");
        ByteString keyLast = KVSchemaUtil.toNormalRouteKey("tenantB", TopicUtil.from("topicB"), "receiverB");
        when(iterator.isValid()).thenReturn(true, true, false);
        when(iterator.key()).thenReturn(keyFirst, keyLast);
        Fact initialFact = distWorkerCoProc.reset(FULL_BOUNDARY).unpack(Fact.class);
        distWorkerCoProc.onLeader(true);
        assertEquals(initialFact.getFirstGlobalFilterLevels().getFilterLevel(0), "tenantA");
        assertEquals(initialFact.getLastGlobalFilterLevels().getFilterLevel(0), "tenantB");

        long incarnation = 1;
        RWCoProcInput rwCoProcInput = RWCoProcInput.newBuilder()
            .setDistService(DistServiceRWCoProcInput.newBuilder()
                .setBatchUnmatch(BatchUnmatchRequest.newBuilder()
                    .setReqId(2002)
                    .putRequests("tenantA", BatchUnmatchRequest.TenantBatch.newBuilder()
                        .addRoute(MatchRoute.newBuilder()
                            .setMatcher(TopicUtil.from("topicA"))
                            .setBrokerId(1)
                            .setReceiverId("inboxA")
                            .setDelivererKey("delivererA")
                            .setIncarnation(incarnation)
                            .build())
                        .build())
                    .build())
                .build())
            .build();

        when(reader.get(any(ByteString.class))).thenReturn(Optional.of(BSUtil.toByteString(incarnation)));
        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer,
            true);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        verify(writer, times(1)).delete(any(ByteString.class));
        verify(routeCache, times(1)).refresh(argThat(m -> m.containsKey("tenantA")
            && m.get("tenantA").routes.keySet()
            .contains(TopicUtil.from("topicA"))));
        verify(tenantsState, times(1)).decNormalRoutes(eq("tenantA"), eq(1));
        verify(reader, times(1)).refresh();
    }

    @SneakyThrows
    @Test
    public void testRemoveRouteAffectLastRouteTriggersRefresh() {
        ByteString keyFirst = KVSchemaUtil.toNormalRouteKey("tenantA", TopicUtil.from("topicA"), "receiverA");
        ByteString keyLast = KVSchemaUtil.toNormalRouteKey("tenantB", TopicUtil.from("topicB"), "receiverB");
        when(iterator.isValid()).thenReturn(true, true, false);
        when(iterator.key()).thenReturn(keyFirst, keyLast);
        Fact initialFact = distWorkerCoProc.reset(FULL_BOUNDARY).unpack(Fact.class);
        distWorkerCoProc.onLeader(true);
        assertEquals(initialFact.getFirstGlobalFilterLevels().getFilterLevel(0), "tenantA");
        assertEquals(initialFact.getLastGlobalFilterLevels().getFilterLevel(0), "tenantB");

        long incarnation = 1;
        RWCoProcInput rwCoProcInput = RWCoProcInput.newBuilder()
            .setDistService(DistServiceRWCoProcInput.newBuilder()
                .setBatchUnmatch(BatchUnmatchRequest.newBuilder()
                    .setReqId(2002)
                    .putRequests("tenantB", BatchUnmatchRequest.TenantBatch.newBuilder()
                        .addRoute(MatchRoute.newBuilder()
                            .setMatcher(TopicUtil.from("topicB"))
                            .setBrokerId(1)
                            .setReceiverId("inboxA")
                            .setDelivererKey("delivererA")
                            .setIncarnation(incarnation)
                            .build())
                        .build())
                    .build())
                .build())
            .build();

        when(reader.get(any(ByteString.class))).thenReturn(Optional.of(BSUtil.toByteString(incarnation)));
        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer,
            true);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        verify(writer, times(1)).delete(any(ByteString.class));
        verify(routeCache, times(1)).refresh(argThat(m -> m.containsKey("tenantB")
            && m.get("tenantB").routes.keySet().contains(TopicUtil.from("topicB"))));
        verify(tenantsState, times(1)).decNormalRoutes(eq("tenantB"), eq(1));
        verify(reader, times(1)).refresh();
    }

    @Test
    public void testQueryBatchDist() {
        ROCoProcInput roCoProcInput = ROCoProcInput.newBuilder().setDistService(DistServiceROCoProcInput.newBuilder()
                .setBatchDist(BatchDistRequest.newBuilder().setReqId(789).addDistPack(
                    DistPack.newBuilder().setTenantId("tenant1")
                        .addMsgPack(TopicMessagePack.newBuilder().setTopic("topic1").build()).build()).build()).build())
            .build();

        // Simulate routes in cache
        CompletableFuture<Set<Matching>> futureRoutes = CompletableFuture.completedFuture(Set.of(
            createMatching("tenant1",
                MatchRoute.newBuilder().setMatcher(TopicUtil.from("topic1")).setBrokerId(1).setReceiverId("inbox1")
                    .setDelivererKey("deliverer1").setIncarnation(1L).build())));
        when(routeCache.get(eq("tenant1"), eq("topic1"))).thenReturn(futureRoutes);

        // Simulate query
        CompletableFuture<ROCoProcOutput> resultFuture = distWorkerCoProc.query(roCoProcInput, reader);
        ROCoProcOutput result = resultFuture.join();

        // Verify the submission to executor group
        verify(deliverExecutorGroup, times(1)).submit(eq("tenant1"), anySet(), any(TopicMessagePack.class));

        // Check the result output
        BatchDistReply reply = result.getDistService().getBatchDist();
        assertEquals(reply.getReqId(), 789);
    }

    @SneakyThrows
    @Test
    public void testReset() {
        Boundary boundary = Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("tenantA"))
            .setEndKey(ByteString.copyFromUtf8("tenantB")).build();
        when(reader.boundary()).thenReturn(boundary);
        when(reader.iterator()).thenReturn(iterator);
        when(iterator.isValid()).thenReturn(true, true, false);
        when(iterator.key()).thenReturn(
            KVSchemaUtil.toNormalRouteKey("tenantA", TopicUtil.from("topic1"), "receiver1"));
        Fact fact = distWorkerCoProc.reset(boundary).unpack(Fact.class);
        distWorkerCoProc.onLeader(true);
        assertTrue(fact.hasFirstGlobalFilterLevels() && fact.hasLastGlobalFilterLevels());
        // Verify that tenant state and route cache are reset
        // reset may be invoked twice due to test setup creating a DistWorkerCoProc and calling reset again
        verify(tenantsState, atLeast(1)).reset();
        verify(routeCache, times(1)).reset(eq(boundary));
    }

    @Test
    public void testClose() {
        distWorkerCoProc.close();

        // Verify that tenant state, route cache, and deliver executor group are closed
        verify(tenantsState, times(1)).close();
        verify(routeCache, times(1)).close();
        verify(deliverExecutorGroup, times(1)).shutdown();
    }

    @Test
    public void testAddNormalRouteNewRouteWritesRefreshAndInc() {
        long inc = 1L;
        String tenantId = "tenantX";
        String topicFilter = "t/1";

        // Build add route request with one normal route
        RWCoProcInput rwCoProcInput = RWCoProcInput.newBuilder().setDistService(
                DistServiceRWCoProcInput.newBuilder()
                    .setBatchMatch(BatchMatchRequest.newBuilder()
                        .setReqId(3001)
                        .putRequests(tenantId,
                            BatchMatchRequest.TenantBatch.newBuilder()
                                .setOption(TenantOption.newBuilder().setMaxReceiversPerSharedSubGroup(10).build())
                                .addRoute(MatchRoute.newBuilder()
                                    .setMatcher(TopicUtil.from(topicFilter))
                                    .setBrokerId(1)
                                    .setReceiverId("inboxX")
                                    .setDelivererKey("delivererX")
                                    .setIncarnation(inc)
                                    .build())
                                .build())
                        .build())
                    .build())
            .build();

        // route not existed
        when(reader.get(any(ByteString.class))).thenReturn(Optional.empty());

        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer,
            false);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        // put once, refresh at least once (added map), inc once
        verify(writer, times(1)).put(any(ByteString.class), eq(BSUtil.toByteString(inc)));
        verify(routeCache, atLeast(1)).refresh(any());
        verify(tenantsState, times(1)).incNormalRoutes(eq(tenantId), eq(1));

        // reply codes should be OK
        BatchMatchReply reply = result.output().getDistService().getBatchMatch();
        assertEquals(reply.getReqId(), 3001);
        assertEquals(reply.getResultsOrThrow(tenantId).getCode(0), BatchMatchReply.TenantBatch.Code.OK);
    }

    @Test
    public void testAddNormalRouteDuplicatedInOneBatchOnlyCountOnceAndRefresh() {
        long inc = 1L;
        String tenantId = "tenantD";
        String topicFilter = "dup/topic";

        // Build add route request with duplicated normal route entries
        BatchMatchRequest.TenantBatch tenantBatch = BatchMatchRequest.TenantBatch.newBuilder()
            .setOption(TenantOption.newBuilder().setMaxReceiversPerSharedSubGroup(10).build())
            .addRoute(MatchRoute.newBuilder()
                .setMatcher(TopicUtil.from(topicFilter))
                .setBrokerId(1)
                .setReceiverId("inboxD")
                .setDelivererKey("delivererD")
                .setIncarnation(inc)
                .build())
            .addRoute(MatchRoute.newBuilder()
                .setMatcher(TopicUtil.from(topicFilter))
                .setBrokerId(1)
                .setReceiverId("inboxD")
                .setDelivererKey("delivererD")
                .setIncarnation(inc)
                .build())
            .build();

        RWCoProcInput rwCoProcInput = RWCoProcInput.newBuilder().setDistService(
                DistServiceRWCoProcInput.newBuilder()
                    .setBatchMatch(BatchMatchRequest.newBuilder()
                        .setReqId(3002)
                        .putRequests(tenantId, tenantBatch)
                        .build())
                    .build())
            .build();

        // route not existed for both entries
        when(reader.get(any(ByteString.class))).thenReturn(Optional.empty());

        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer,
            false);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        // writer.put is called twice for duplicated entries
        verify(writer, times(2)).put(any(ByteString.class), eq(BSUtil.toByteString(inc)));
        // but we only inc once; refresh called at least once (added map)
        verify(routeCache, atLeast(1)).refresh(any());
        verify(tenantsState, times(1)).incNormalRoutes(eq(tenantId), eq(1));

        BatchMatchReply reply = result.output().getDistService().getBatchMatch();
        assertEquals(reply.getReqId(), 3002);
        assertEquals(reply.getResultsOrThrow(tenantId).getCode(0), BatchMatchReply.TenantBatch.Code.OK);
        assertEquals(reply.getResultsOrThrow(tenantId).getCode(1), BatchMatchReply.TenantBatch.Code.OK);
    }

    @Test
    public void testAddNormalRouteUpgradeIncNoIncButRefreshCalled() {
        long oldInc = 1L;
        long newInc = 2L;
        String tenantId = "tenantU";
        String topicFilter = "u/1";

        RWCoProcInput rwCoProcInput = RWCoProcInput.newBuilder().setDistService(
                DistServiceRWCoProcInput.newBuilder()
                    .setBatchMatch(BatchMatchRequest.newBuilder()
                        .setReqId(3003)
                        .putRequests(tenantId, BatchMatchRequest.TenantBatch.newBuilder()
                            .setOption(TenantOption.newBuilder().setMaxReceiversPerSharedSubGroup(10).build())
                            .addRoute(MatchRoute.newBuilder()
                                .setMatcher(TopicUtil.from(topicFilter))
                                .setBrokerId(1)
                                .setReceiverId("inboxU")
                                .setDelivererKey("delivererU")
                                .setIncarnation(newInc)
                                .build())
                            .build())
                        .build())
                    .build())
            .build();

        // existing route with smaller inc
        when(reader.get(any(ByteString.class))).thenReturn(Optional.of(BSUtil.toByteString(oldInc)));

        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer,
            false);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        // KV updated, but no inc; refresh called twice (added & removed)
        verify(writer, times(1)).put(any(ByteString.class), eq(BSUtil.toByteString(newInc)));
        verify(routeCache, times(2)).refresh(any());
        verify(tenantsState, times(0)).incNormalRoutes(anyString(), anyInt());

        BatchMatchReply reply = result.output().getDistService().getBatchMatch();
        assertEquals(reply.getReqId(), 3003);
        assertEquals(reply.getResultsOrThrow(tenantId).getCode(0), BatchMatchReply.TenantBatch.Code.OK);
    }

    @Test
    public void testUpgradeNormalRouteRefreshesAddAndRemoveTasks() {
        long oldInc = 1L;
        long newInc = 3L;
        String tenantId = "tenantZ";
        String topicFilter = "z/topic";

        RWCoProcInput rwCoProcInput = RWCoProcInput.newBuilder().setDistService(
                DistServiceRWCoProcInput.newBuilder()
                    .setBatchMatch(BatchMatchRequest.newBuilder()
                        .setReqId(3005)
                        .putRequests(tenantId, BatchMatchRequest.TenantBatch.newBuilder()
                            .setOption(TenantOption.newBuilder()
                                .setMaxReceiversPerSharedSubGroup(10)
                                .build())
                            .addRoute(MatchRoute.newBuilder()
                                .setMatcher(TopicUtil.from(topicFilter))
                                .setBrokerId(1)
                                .setReceiverId("inboxZ")
                                .setDelivererKey("delivererZ")
                                .setIncarnation(newInc)
                                .build())
                            .build())
                        .build())
                    .build())
            .build();

        // existing with smaller inc
        when(reader.get(any(ByteString.class))).thenReturn(Optional.of(BSUtil.toByteString(oldInc)));

        Supplier<IKVRangeCoProc.MutationResult> resultSupplier =
            distWorkerCoProc.mutate(rwCoProcInput, reader, writer, false);
        resultSupplier.get();

        // capture both refresh calls and assert one AddRoutesTask and one RemoveRoutesTask
        ArgumentCaptor<java.util.Map<String, RefreshEntriesTask>> captor = ArgumentCaptor.forClass(java.util.Map.class);
        verify(routeCache, times(2)).refresh(captor.capture());

        boolean sawAdd = false, sawRemove = false;
        for (java.util.Map<String, RefreshEntriesTask> m : captor.getAllValues()) {
            assertTrue(m.containsKey(tenantId));
            RefreshEntriesTask task = m.get(tenantId);
            if (task instanceof AddRoutesTask) {
                sawAdd = true;
            }
            if (task instanceof RemoveRoutesTask) {
                sawRemove = true;
            }
        }
        assertTrue(sawAdd && sawRemove);

        verify(writer, times(1)).put(any(ByteString.class), eq(BSUtil.toByteString(newInc)));
        verify(tenantsState, times(0)).incNormalRoutes(anyString(), anyInt());
        // toggle metering invoked with leader=false in this test
        verify(tenantsState, times(1)).toggleMetering(eq(false));
    }

    @Test
    public void testAddNormalRouteNoOpWhenIncEqualOrLowerNoIncButRefreshCalled() {
        long stored = 2L;
        long reqInc = 2L; // equal
        String tenantId = "tenantEQ";
        String topicFilter = "eq/topic";

        RWCoProcInput rwCoProcInput = RWCoProcInput.newBuilder().setDistService(
                DistServiceRWCoProcInput.newBuilder()
                    .setBatchMatch(BatchMatchRequest.newBuilder()
                        .setReqId(3004)
                        .putRequests(tenantId, BatchMatchRequest.TenantBatch.newBuilder()
                            .setOption(TenantOption.newBuilder()
                                .setMaxReceiversPerSharedSubGroup(10)
                                .build())
                            .addRoute(MatchRoute.newBuilder()
                                .setMatcher(TopicUtil.from(topicFilter))
                                .setBrokerId(1)
                                .setReceiverId("inboxEQ")
                                .setDelivererKey("delivererEQ")
                                .setIncarnation(reqInc)
                                .build())
                            .build())
                        .build())
                    .build())
            .build();

        when(reader.get(any(ByteString.class))).thenReturn(Optional.of(BSUtil.toByteString(stored)));

        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer,
            false);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        // No kv write, no inc; refresh will be called with empty maps
        verify(writer, times(0)).put(any(ByteString.class), any(ByteString.class));
        verify(routeCache, never()).refresh(any());
        verify(tenantsState, times(0)).incNormalRoutes(anyString(), anyInt());

        BatchMatchReply reply = result.output().getDistService().getBatchMatch();
        assertEquals(reply.getReqId(), 3004);
        assertEquals(reply.getResultsOrThrow(tenantId).getCode(0), BatchMatchReply.TenantBatch.Code.OK);
    }

    @Test
    public void testRemoveNormalRouteNotExistedWhenRequestIncOlder() {
        String tenantId = "tenantR";
        String topicFilter = "r/t";
        long storedInc = 5L;
        long reqInc = 3L; // older than stored

        RWCoProcInput rwCoProcInput = RWCoProcInput.newBuilder().setDistService(
                DistServiceRWCoProcInput.newBuilder()
                    .setBatchUnmatch(BatchUnmatchRequest.newBuilder()
                        .setReqId(4001)
                        .putRequests(tenantId, BatchUnmatchRequest.TenantBatch.newBuilder()
                            .addRoute(MatchRoute.newBuilder()
                                .setMatcher(TopicUtil.from(topicFilter))
                                .setBrokerId(1)
                                .setReceiverId("inboxR")
                                .setDelivererKey("delivererR")
                                .setIncarnation(reqInc)
                                .build())
                            .build())
                        .build())
                    .build())
            .build();

        when(reader.get(any(ByteString.class))).thenReturn(Optional.of(BSUtil.toByteString(storedInc)));

        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer,
            false);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        // Should not delete nor dec; refresh will be called with empty maps twice
        verify(writer, times(0)).delete(any(ByteString.class));
        verify(routeCache, never()).refresh(any());
        verify(tenantsState, times(0)).decNormalRoutes(anyString(), anyInt());

        BatchUnmatchReply reply = result.output().getDistService().getBatchUnmatch();
        assertEquals(reply.getReqId(), 4001);
        assertEquals(reply.getResultsOrThrow(tenantId).getCode(0), BatchUnmatchReply.TenantBatch.Code.NOT_EXISTED);
    }

    private Matching createMatching(String tenantId, MatchRoute route) {
        // Sample data for creating a Matching object

        // Construct a ByteString for normal match record key
        ByteString normalRouteKey = KVSchemaUtil.toNormalRouteKey(tenantId, route.getMatcher(), toReceiverUrl(route));

        // Construct the match record value (for example, an empty value for a normal match)
        ByteString matchRecordValue = toByteString(1L);

        // Use EntityUtil to parse the key and value into a Matching object
        return KVSchemaUtil.buildMatchRoute(normalRouteKey, matchRecordValue);
    }
}
