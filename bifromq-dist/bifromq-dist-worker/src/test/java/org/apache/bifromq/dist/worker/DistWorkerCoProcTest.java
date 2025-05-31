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

package org.apache.bifromq.dist.worker;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.parseRouteDetail;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toReceiverUrl;
import static org.apache.bifromq.util.BSUtil.toByteString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

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
import org.apache.bifromq.dist.rpc.proto.GCReply;
import org.apache.bifromq.dist.rpc.proto.GCRequest;
import org.apache.bifromq.dist.rpc.proto.MatchRoute;
import org.apache.bifromq.dist.rpc.proto.RouteGroup;
import org.apache.bifromq.dist.rpc.proto.TenantOption;
import org.apache.bifromq.dist.worker.cache.ISubscriptionCache;
import org.apache.bifromq.dist.worker.schema.KVSchemaUtil;
import org.apache.bifromq.dist.worker.schema.Matching;
import org.apache.bifromq.plugin.subbroker.CheckRequest;
import org.apache.bifromq.type.TopicMessagePack;
import org.apache.bifromq.util.BSUtil;
import org.apache.bifromq.util.TopicUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DistWorkerCoProcTest {

    private ISubscriptionCache routeCache;
    private ITenantsState tenantsState;
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
        tenantsState = mock(ITenantsState.class);
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
        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        verify(writer, times(2)).put(any(), eq(toByteString(1L)));
        // Verify that matches are added to the cache
        verify(routeCache, times(1)).refresh(any());

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
        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        // Verify that matches are removed from the cache
        verify(routeCache, times(1)).refresh(
            argThat(m -> m.containsKey("tenant1") && m.get("tenant1").contains(TopicUtil.from("topicFilter1"))));

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

        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        verify(writer, times(2)).put(any(), eq(BSUtil.toByteString(1L)));
        verify(routeCache, times(1)).refresh(any());
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

        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        verify(writer, times(1)).delete(any(ByteString.class));
        verify(routeCache, times(1)).refresh(
            argThat(m -> m.containsKey("tenant1") && m.get("tenant1").contains(TopicUtil.from("topicFilter1"))));
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

        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        verify(writer, times(1)).put(any(), eq(BSUtil.toByteString(1L)));
        verify(routeCache, times(1)).refresh(any());
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

        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        verify(writer, times(1)).put(any(), eq(BSUtil.toByteString(1L)));
        verify(routeCache, times(1)).refresh(any());
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
        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        verify(writer, times(1)).delete(any(ByteString.class));
        verify(routeCache, times(1)).refresh(argThat(m -> m.containsKey("tenantA")
            && m.get("tenantA").contains(TopicUtil.from("topicA"))));
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
        Supplier<IKVRangeCoProc.MutationResult> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer);
        IKVRangeCoProc.MutationResult result = resultSupplier.get();

        verify(writer, times(1)).delete(any(ByteString.class));
        verify(routeCache, times(1)).refresh(argThat(m -> m.containsKey("tenantB")
            && m.get("tenantB").contains(TopicUtil.from("topicB"))));
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

    @Test
    public void testGC() {
        String tenant1 = "tenant1";
        String tenant2 = "tenant2";
        String topic1 = "topic1";
        String topic2 = "topic2";
        String receiverUrl1 = toReceiverUrl(1, "inbox1", "deliverer1");
        String receiverUrl2 = toReceiverUrl(2, "inbox2", "deliverer2");

        ByteString normalMatchKey1 =
            KVSchemaUtil.toNormalRouteKey(tenant1, TopicUtil.from(topic1), toReceiverUrl(1, "inbox1", "deliverer1"));
        ByteString normalMatchKey2 =
            KVSchemaUtil.toNormalRouteKey(tenant2, TopicUtil.from(topic2), toReceiverUrl(2, "inbox2", "deliverer2"));

        String sharedTopic = "$share/group/topic3";
        ByteString groupMatchKey = KVSchemaUtil.toGroupRouteKey(tenant1, TopicUtil.from(sharedTopic));
        RouteGroup groupMembers =
            RouteGroup.newBuilder().putMembers(receiverUrl1, 1L).putMembers(receiverUrl2, 1L).build();

        when(iterator.isValid()).thenReturn(true, true, true, false);
        when(iterator.key()).thenReturn(normalMatchKey1, groupMatchKey, normalMatchKey2);
        when(iterator.value()).thenReturn(toByteString(1L), groupMembers.toByteString(), toByteString(1L));

        when(routeCache.isCached(eq(tenant1), eq(TopicUtil.from(topic1).getFilterLevelList()))).thenReturn(false);
        when(routeCache.isCached(eq(tenant1),
            eq(TopicUtil.from(parseRouteDetail(groupMatchKey).matcher().getMqttTopicFilter())
                .getFilterLevelList()))).thenReturn(false);
        when(routeCache.isCached(eq(tenant2), eq(TopicUtil.from(topic2).getFilterLevelList()))).thenReturn(false);

        when(subscriptionChecker.sweep(anyInt(), any(CheckRequest.class))).thenReturn(
            CompletableFuture.completedFuture(null));

        ROCoProcInput roCoProcInput = ROCoProcInput.newBuilder().setDistService(
            DistServiceROCoProcInput.newBuilder().setGc(GCRequest.newBuilder().setReqId(999).build()).build()).build();

        CompletableFuture<ROCoProcOutput> resultFuture = distWorkerCoProc.query(roCoProcInput, reader);
        ROCoProcOutput result = resultFuture.join();

        GCReply reply = result.getDistService().getGc();
        assertEquals(reply.getReqId(), 999);

        verify(subscriptionChecker, times(1)).sweep(eq(1),
            argThat(req -> req.getTenantId().equals(tenant1) && req.getMatchInfoCount() == 2));
        verify(subscriptionChecker, times(1)).sweep(eq(2), argThat(
            req -> req.getTenantId().equals(tenant2) && req.getMatchInfoCount() == 1
                && req.getMatchInfoList().get(0).getMatcher().getMqttTopicFilter().equals(topic2)));
        verify(subscriptionChecker, times(1)).sweep(eq(2), argThat(
            req -> req.getTenantId().equals(tenant1) && req.getMatchInfoCount() == 1
                && req.getMatchInfoList().get(0).getMatcher().getMqttTopicFilter().equals(sharedTopic)));
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
        assertTrue(fact.hasFirstGlobalFilterLevels() && fact.hasLastGlobalFilterLevels());
        // Verify that tenant state and route cache are reset
        verify(tenantsState, times(2)).reset();
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