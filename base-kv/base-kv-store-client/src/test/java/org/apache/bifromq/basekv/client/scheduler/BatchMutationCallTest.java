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

package org.apache.bifromq.basekv.client.scheduler;

import static org.apache.bifromq.basekv.client.scheduler.Fixtures.setting;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.IMutationPipeline;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.proto.KVRangeRWReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRWRequest;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.collections.Sets;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BatchMutationCallTest {
    private KVRangeId id;
    @Mock
    private IBaseKVStoreClient storeClient;
    @Mock
    private IMutationPipeline mutationPipeline1;
    @Mock
    private IMutationPipeline mutationPipeline2;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        id = KVRangeIdUtil.generate();
    }

    @SneakyThrows
    @AfterMethod
    public void teardown() {
        closeable.close();
    }

    @Test
    public void addToSameBatch() {
        when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {
            {
                put(FULL_BOUNDARY, setting(id, "V1", 0));
            }
        });

        when(storeClient.createMutationPipeline("V1")).thenReturn(mutationPipeline1);
        when(mutationPipeline1.execute(any()))
            .thenReturn(CompletableFuture.supplyAsync(() -> KVRangeRWReply.newBuilder().build(),
                CompletableFuture.delayedExecutor(1000, TimeUnit.MILLISECONDS)));

        TestMutationCallScheduler scheduler = new TestMutationCallScheduler(storeClient, Duration.ofMillis(1000));
        List<Integer> reqList = new ArrayList<>();
        List<Integer> respList = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            int req = ThreadLocalRandom.current().nextInt(100);
            reqList.add(req);
            futures.add(scheduler.schedule(ByteString.copyFromUtf8(Integer.toString(req)))
                .thenAccept((v) -> respList.add(Integer.parseInt(v.toStringUtf8()))));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        ArgumentCaptor<KVRangeRWRequest> rwRequestCaptor = ArgumentCaptor.forClass(KVRangeRWRequest.class);
        verify(mutationPipeline1, atMost(1000)).execute(rwRequestCaptor.capture());
        for (KVRangeRWRequest request : rwRequestCaptor.getAllValues()) {
            String[] keys = request.getRwCoProc().getRaw().toStringUtf8().split("_");
            assertEquals(keys.length, Sets.newSet(keys).size());
        }
        Collections.sort(reqList);
        Collections.sort(respList);
        assertEquals(reqList, respList);
    }

    @Test
    public void addToDifferentBatch() {
        when(storeClient.createMutationPipeline("V1")).thenReturn(mutationPipeline1);
        when(storeClient.createMutationPipeline("V2")).thenReturn(mutationPipeline2);
        when(mutationPipeline1.execute(any()))
            .thenReturn(CompletableFuture.supplyAsync(() -> KVRangeRWReply.newBuilder().build()));
        when(mutationPipeline2.execute(any()))
            .thenReturn(CompletableFuture.supplyAsync(() -> KVRangeRWReply.newBuilder().build()));

        TestMutationCallScheduler scheduler = new TestMutationCallScheduler(storeClient, Duration.ofMillis(1000));
        List<Integer> reqList = new ArrayList<>();
        List<Integer> respList = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            int req = ThreadLocalRandom.current().nextInt(1, 1001);
            reqList.add(req);
            if (req < 500) {
                when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {
                    {
                        put(FULL_BOUNDARY, setting(id, "V1", 0));
                    }
                });
            } else {
                when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {
                    {
                        put(FULL_BOUNDARY, setting(id, "V2", 0));
                    }
                });
            }
            futures.add(scheduler.schedule(ByteString.copyFromUtf8(Integer.toString(req)))
                .thenAccept((v) -> respList.add(Integer.parseInt(v.toStringUtf8()))));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        Collections.sort(reqList);
        Collections.sort(respList);
        assertEquals(reqList, respList);
    }

    @Test
    public void executeManySmallBatchesNoRecursion() {
        when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {
            {
                put(FULL_BOUNDARY, setting(id, "V1", 0));
            }
        });

        when(storeClient.createMutationPipeline("V1")).thenReturn(mutationPipeline1);
        AtomicInteger execCount = new AtomicInteger();
        when(mutationPipeline1.execute(any())).thenAnswer(invocation -> {
            execCount.incrementAndGet();
            return CompletableFuture.supplyAsync(KVRangeRWReply::newBuilder)
                .thenApply(KVRangeRWReply.Builder::build);
        });

        TestMutationCallScheduler scheduler = new TestMutationCallScheduler(storeClient, Duration.ofMillis(1000));
        int n = 5000;
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            futures.add(scheduler.schedule(ByteString.copyFromUtf8("k"))
                .thenAccept(v -> {}));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        assertEquals(execCount.get(), n);
    }

    @Test
    public void reScanWhenHitNonBatchable() {
        when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {
            {
                put(FULL_BOUNDARY, setting(id, "V1", 0));
            }
        });
        when(storeClient.createMutationPipeline("V1")).thenReturn(mutationPipeline1);
        when(mutationPipeline1.execute(any()))
            .thenReturn(CompletableFuture.supplyAsync(() -> KVRangeRWReply.newBuilder().build()));

        MutationCallScheduler<ByteString, ByteString, TestBatchMutationCall> scheduler =
            new MutationCallScheduler<>(NonBatchableBatchCall::new, Duration.ofMillis(1000).toNanos(), storeClient) {
                @Override
                protected ByteString rangeKey(ByteString call) {
                    return call;
                }
            };
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<ByteString> reqs = List.of(
            ByteString.copyFromUtf8("k1"),
            ByteString.copyFromUtf8("k_dup"), // will mark non-batchable in first batch
            ByteString.copyFromUtf8("k2"));
        List<ByteString> resps = new CopyOnWriteArrayList<>();
        reqs.forEach(req -> futures.add(scheduler.schedule(req).thenAccept(resps::add)));
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        List<String> reqSorted = reqs.stream().map(ByteString::toStringUtf8).sorted().toList();
        List<String> respSorted = resps.stream().map(ByteString::toStringUtf8).sorted().toList();
        assertEquals(reqSorted, respSorted);
    }

    @Test
    public void mixDifferentVersions() {
        when(storeClient.createMutationPipeline("V1")).thenReturn(mutationPipeline1);
        when(storeClient.createMutationPipeline("V2")).thenReturn(mutationPipeline2);
        when(mutationPipeline1.execute(any()))
            .thenReturn(CompletableFuture.supplyAsync(() -> KVRangeRWReply.newBuilder().build()));
        when(mutationPipeline2.execute(any()))
            .thenReturn(CompletableFuture.supplyAsync(() -> KVRangeRWReply.newBuilder().build()));
        TestMutationCallScheduler scheduler = new TestMutationCallScheduler(storeClient, Duration.ofMillis(1000));
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<ByteString> reqs = new ArrayList<>();
        List<ByteString> resps = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 20; i++) {
            ByteString req = ByteString.copyFromUtf8("k" + i);
            reqs.add(req);
            if (i % 2 == 0) {
                when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {
                    {
                        put(FULL_BOUNDARY, setting(id, "V1", 0));
                    }
                });
            } else {
                when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {
                    {
                        put(FULL_BOUNDARY, setting(id, "V2", 1));
                    }
                });
            }
            futures.add(scheduler.schedule(req).thenAccept(resps::add));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        List<String> reqSorted = reqs.stream().map(ByteString::toStringUtf8).sorted().toList();
        List<String> respSorted = resps.stream().map(ByteString::toStringUtf8).sorted().toList();
        assertEquals(reqSorted, respSorted);
    }

    private static class NonBatchableBatchCall extends TestBatchMutationCall {
        protected NonBatchableBatchCall(IMutationPipeline pipeline, MutationCallBatcherKey batcherKey) {
            super(pipeline, batcherKey);
        }

        @Override
        protected NonBatchableFirstBatch newBatch(long ver) {
            return new NonBatchableFirstBatch(ver);
        }
    }
}
