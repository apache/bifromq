/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.basekv.client.scheduler;

import static com.baidu.bifromq.basekv.client.scheduler.Fixtures.setting;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.IMutationPipeline;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.collections.Sets;
import org.mockito.stubbing.Answer;
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
        when(storeClient.findByKey(any())).thenReturn(Optional.of(setting(id, "V1", 0)));
        when(storeClient.createMutationPipeline("V1")).thenReturn(mutationPipeline1);
        when(mutationPipeline1.execute(any()))
            .thenReturn(CompletableFuture.supplyAsync(() -> KVRangeRWReply.newBuilder().build(),
                CompletableFuture.delayedExecutor(1000, TimeUnit.MILLISECONDS)));

        TestMutationCallScheduler scheduler =
            new TestMutationCallScheduler("test_call_scheduler", storeClient, Duration.ofMillis(100),
                Duration.ofMillis(1000), Duration.ofMinutes(5));
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
        // the resp order preserved
        assertEquals(reqList, respList);
    }

    @Test
    public void addToDifferentBatch() {
        when(storeClient.findByKey(any())).thenAnswer((Answer<Optional<KVRangeSetting>>) invocation -> {
            int req = Integer.parseInt(((ByteString) invocation.getArgument(0)).toStringUtf8());
            return Optional.of(req < 500 ?
                setting(id, "V1", 0) :
                setting(id, "V2", 0));
        });
        when(storeClient.createMutationPipeline("V1")).thenReturn(mutationPipeline1);
        when(storeClient.createMutationPipeline("V2")).thenReturn(mutationPipeline2);
        when(mutationPipeline1.execute(any()))
            .thenReturn(CompletableFuture.supplyAsync(() -> KVRangeRWReply.newBuilder().build()));
        when(mutationPipeline2.execute(any()))
            .thenReturn(CompletableFuture.supplyAsync(() -> KVRangeRWReply.newBuilder().build()));

        TestMutationCallScheduler scheduler =
            new TestMutationCallScheduler("test_call_scheduler", storeClient, Duration.ofMillis(100),
                Duration.ofMillis(1000), Duration.ofMinutes(5));
        List<Integer> reqList = new ArrayList<>();
        List<Integer> respList = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            int req = ThreadLocalRandom.current().nextInt(1, 1001);
            reqList.add(req);
            futures.add(scheduler.schedule(ByteString.copyFromUtf8(Integer.toString(req)))
                .thenAccept((v) -> respList.add(Integer.parseInt(v.toStringUtf8()))));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        // the resp order preserved
        assertEquals(reqList, respList);
    }

    @Test
    public void pipelineExpiry() {
        when(storeClient.findByKey(any())).thenReturn(Optional.of(setting(id, "V1", 0)));
        when(storeClient.createMutationPipeline("V1")).thenReturn(mutationPipeline1);
        when(mutationPipeline1.execute(any()))
            .thenReturn(CompletableFuture.supplyAsync(() -> KVRangeRWReply.newBuilder().build()));

        TestMutationCallScheduler scheduler =
            new TestMutationCallScheduler("test_call_scheduler", storeClient, Duration.ofMillis(100),
                Duration.ofMillis(1000), Duration.ofMillis(100), Duration.ofMillis(100));
        List<CompletableFuture<ByteString>> futures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            int req = ThreadLocalRandom.current().nextInt();
            futures.add(scheduler.schedule(ByteString.copyFromUtf8(Integer.toString(req))));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        verify(mutationPipeline1, timeout(Long.MAX_VALUE).times(1)).close();
    }
}