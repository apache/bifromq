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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.protobuf.ByteString;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVRangeRefreshableReader;
import org.apache.bifromq.basekv.store.api.IKVWriter;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.dist.rpc.proto.BatchMatchRequest;
import org.apache.bifromq.dist.rpc.proto.BatchUnmatchRequest;
import org.apache.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import org.apache.bifromq.dist.rpc.proto.Fact;
import org.apache.bifromq.dist.rpc.proto.MatchRoute;
import org.apache.bifromq.dist.rpc.proto.TenantOption;
import org.apache.bifromq.dist.worker.cache.ISubscriptionCache;
import org.apache.bifromq.util.TopicUtil;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DistWorkerFactOrderTest {
    private Path path;
    private Options options;
    private RocksDB db;
    private IKVRangeRefreshableReader reader;
    private DistWorkerCoProc coProc;
    private ISubscriptionCache routeCache;
    private IDeliverExecutorGroup deliverExecutorGroup;

    @BeforeMethod
    public void setup() throws Exception {
        RocksDB.loadLibrary();
        path = Files.createTempDirectory("dist-fact-order-");
        options = new Options().setCreateIfMissing(true);
        db = RocksDB.open(options, path.toString());
        reader = mock(IKVRangeRefreshableReader.class);
        when(reader.boundary()).thenReturn(FULL_BOUNDARY);
        when(reader.get(any())).thenAnswer(call -> {
            byte[] value = db.get(((ByteString) call.getArgument(0)).toByteArray());
            return value == null ? Optional.empty() : Optional.of(ByteString.copyFrom(value));
        });
        when(reader.iterator()).thenAnswer(call -> {
            RocksIterator rocks = db.newIterator();
            IKVIterator iterator = mock(IKVIterator.class);
            doAnswer(invocation -> { rocks.seekToFirst(); return null; }).when(iterator).seekToFirst();
            doAnswer(invocation -> { rocks.seekToLast(); return null; }).when(iterator).seekToLast();
            when(iterator.isValid()).thenAnswer(invocation -> rocks.isValid());
            when(iterator.key()).thenAnswer(invocation -> ByteString.copyFrom(rocks.key()));
            doAnswer(invocation -> { rocks.close(); return null; }).when(iterator).close();
            return iterator;
        });
        routeCache = mock(ISubscriptionCache.class);
        deliverExecutorGroup = mock(IDeliverExecutorGroup.class);
        coProc = new DistWorkerCoProc(KVRangeId.getDefaultInstance(), () -> reader,
            routeCache, mock(ITenantsStats.class), deliverExecutorGroup, mock(ISubscriptionCleaner.class));
        coProc.reset(FULL_BOUNDARY);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        if (coProc != null) {
            coProc.close();
        }
        if (db != null) {
            db.close();
        }
        if (options != null) {
            RocksDB.destroyDB(path.toString(), options);
            options.close();
            Files.deleteIfExists(path);
        }
    }

    @Test
    public void addLongerTenantUpdatesLastFact() throws Exception {
        apply(true, routes("a", "zz"));
        Fact fact = apply(true, routes("bbb"));
        assertEquals(fact.getLastGlobalFilterLevels().getFilterLevelList(), List.of("bbb", "sensor", "#"));
    }

    @Test
    public void batchAddUsesEncodedExtremes() throws Exception {
        apply(true, routes("a", "zz"));
        Fact fact = apply(true, routes("bbb", "t"));
        assertEquals(fact.getLastGlobalFilterLevels().getFilterLevel(0), "bbb");
    }

    @Test
    public void batchDeleteUsesEncodedExtremes() throws Exception {
        apply(true, routes("t", "aa", "zz", "bbb"));
        Fact fact = apply(false, routes("t", "aa"));
        assertEquals(fact.getFirstGlobalFilterLevels().getFilterLevel(0), "zz");
    }

    @Test
    public void tenantSortKeysStayInsideMutationTracking() throws Exception {
        for (String tenantId : List.of("", "#", "+", "租户", "\uD83D\uDE00")) {
            var routes = Map.of(tenantId, List.of(route("$oshare/g/a", "r0")));
            for (boolean add : List.of(true, false)) {
                clearInvocations(routeCache, deliverExecutorGroup);
                Fact fact = apply(add, routes);
                verify(routeCache).refresh(argThat(tasks -> tasks.keySet().equals(routes.keySet())));
                verify(deliverExecutorGroup).refreshOrderedShareSubRoutes(tenantId, TopicUtil.from("$oshare/g/a"));
                if (add) {
                    assertEquals(fact.getFirstGlobalFilterLevels().getFilterLevel(0), tenantId);
                }
            }
        }
    }

    @Test
    public void boundariesAndRemainingReceiversMatchStoredRecords() throws Exception {
        apply(true, routes("aa", "zz"));
        apply(true, routes("t"));
        apply(false, routes("zz"));
        MatchRoute second = route("sensor/#", "r1");
        apply(true, Map.of("t", List.of(second)));
        apply(false, routes("t"));
        apply(false, Map.of("t", List.of(second)));
        assertEquals(apply(false, routes("aa")), Fact.getDefaultInstance());
        apply(true, routes("bbb"));
    }

    @Test
    public void unicodeFilterExtremesMatchStoredRecords() throws Exception {
        apply(true, Map.of("t", List.of(route("a", "r0"), route("\uE000", "r0"))));
        apply(true, Map.of("t", List.of(route("\uD800\uDC00", "r0"), route("z", "r0"))));
        apply(false, Map.of("t", List.of(route("\uE000", "r0"), route("\uD800\uDC00", "r0"))));
    }

    @Test
    public void emptyLevelsKeepFactExtremesAcrossRestartAndRemoval() throws Exception {
        Fact fact = apply(true, Map.of("t", List.of(route("a", "r1"), route("a//x", "r2"), route("a", "r0"))));
        assertEquals(fact.getFirstGlobalFilterLevels().getFilterLevelList(), List.of("t", "a"));
        assertEquals(fact.getLastGlobalFilterLevels().getFilterLevelList(), List.of("t", "a", "", "x"));
        db.close();
        db = RocksDB.open(options, path.toString());
        assertEquals(coProc.reset(FULL_BOUNDARY).unpack(Fact.class), fact);
        fact = apply(false, Map.of("t", List.of(route("a//x", "r2"))));
        assertEquals(fact.getLastGlobalFilterLevels().getFilterLevelList(), List.of("t", "a"));
    }

    private Map<String, List<MatchRoute>> routes(String... tenants) {
        Map<String, List<MatchRoute>> routes = new java.util.HashMap<>();
        for (String tenant : tenants) {
            routes.put(tenant, List.of(route("sensor/#", "r0")));
        }
        return routes;
    }

    private MatchRoute route(String filter, String receiver) {
        return MatchRoute.newBuilder().setMatcher(TopicUtil.from(filter)).setBrokerId(0)
            .setReceiverId(receiver).setDelivererKey("d").setIncarnation(1).build();
    }

    private Fact apply(boolean add, Map<String, List<MatchRoute>> routes) throws Exception {
        DistServiceRWCoProcInput.Builder request = DistServiceRWCoProcInput.newBuilder();
        if (add) {
            BatchMatchRequest.Builder batch = BatchMatchRequest.newBuilder();
            routes.forEach((tenant, values) -> batch.putRequests(tenant, BatchMatchRequest.TenantBatch.newBuilder()
                .setOption(TenantOption.newBuilder().setMaxReceiversPerSharedSubGroup(10))
                .addAllRoute(values).build()));
            request.setBatchMatch(batch);
        } else {
            BatchUnmatchRequest.Builder batch = BatchUnmatchRequest.newBuilder();
            routes.forEach((tenant, values) -> batch.putRequests(tenant,
                BatchUnmatchRequest.TenantBatch.newBuilder().addAllRoute(values).build()));
            request.setBatchUnmatch(batch);
        }
        try (WriteBatch batch = new WriteBatch(); WriteOptions writeOptions = new WriteOptions()) {
            IKVWriter writer = mock(IKVWriter.class);
            doAnswer(call -> {
                batch.put(((ByteString) call.getArgument(0)).toByteArray(),
                    ((ByteString) call.getArgument(1)).toByteArray());
                return null;
            }).when(writer).put(any(), any());
            doAnswer(call -> {
                batch.delete(((ByteString) call.getArgument(0)).toByteArray());
                return null;
            }).when(writer).delete(any());
            var mutation = coProc.mutate(RWCoProcInput.newBuilder().setDistService(request).build(), reader, writer, true);
            db.write(writeOptions, batch);
            Fact fact = mutation.get().fact().orElseThrow().unpack(Fact.class);
            assertEquals(fact, coProc.reset(FULL_BOUNDARY).unpack(Fact.class));
            return fact;
        }
    }
}
