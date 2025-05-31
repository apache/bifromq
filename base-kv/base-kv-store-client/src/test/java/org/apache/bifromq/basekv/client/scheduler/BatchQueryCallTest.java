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

package org.apache.bifromq.basekv.client.scheduler;

import static org.apache.bifromq.basekv.client.scheduler.Fixtures.setting;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.IQueryPipeline;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.proto.KVRangeROReply;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BatchQueryCallTest {
    private KVRangeId id;
    @Mock
    private IBaseKVStoreClient storeClient;
    @Mock
    private IQueryPipeline queryPipeline1;
    @Mock
    private IQueryPipeline queryPipeline2;
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
        ExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {{
            put(FULL_BOUNDARY, setting(id, "V1", 0));
        }});
        when(storeClient.createLinearizedQueryPipeline("V1")).thenReturn(queryPipeline1);
        when(queryPipeline1.query(any()))
            .thenReturn(CompletableFuture.supplyAsync(() -> KVRangeROReply.newBuilder().build(), executor));

        TestQueryCallScheduler scheduler = new TestQueryCallScheduler(storeClient, Duration.ofMinutes(5), true);
        List<Integer> reqList = new ArrayList<>();
        List<Integer> respList = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            int req = ThreadLocalRandom.current().nextInt();
            reqList.add(req);
            futures.add(scheduler.schedule(ByteString.copyFromUtf8(Integer.toString(req)))
                .thenAccept((v) -> respList.add(Integer.parseInt(v.toStringUtf8()))));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        // the resp order preserved
        assertEquals(reqList, respList);
        executor.shutdown();
    }

    @Test
    public void addToDifferentBatch() {
        when(storeClient.createLinearizedQueryPipeline("V1")).thenReturn(queryPipeline1);
        when(storeClient.createLinearizedQueryPipeline("V2")).thenReturn(queryPipeline2);
        ExecutorService executor1 = Executors.newSingleThreadScheduledExecutor();
        ExecutorService executor2 = Executors.newSingleThreadScheduledExecutor();
        when(queryPipeline1.query(any()))
            .thenReturn(CompletableFuture.supplyAsync(() -> KVRangeROReply.newBuilder().build(), executor1));
        when(queryPipeline2.query(any()))
            .thenReturn(CompletableFuture.supplyAsync(() -> KVRangeROReply.newBuilder().build(), executor2));
        TestQueryCallScheduler scheduler = new TestQueryCallScheduler(storeClient, Duration.ofMinutes(5), true);
        List<Integer> reqList1 = new ArrayList<>();
        List<Integer> reqList2 = new ArrayList<>();
        List<Integer> respList1 = new CopyOnWriteArrayList<>();
        List<Integer> respList2 = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            int req = ThreadLocalRandom.current().nextInt(1, 1001);
            if (req < 500) {
                reqList1.add(req);
                when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {{
                    put(FULL_BOUNDARY, setting(id, "V1", 0));
                }});
                futures.add(scheduler.schedule(ByteString.copyFromUtf8(Integer.toString(req)))
                    .thenAccept((v) -> respList1.add(Integer.parseInt(v.toStringUtf8()))));
            } else {
                reqList2.add(req);
                when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {{
                    put(FULL_BOUNDARY, setting(id, "V2", 0));
                }});
                futures.add(scheduler.schedule(ByteString.copyFromUtf8(Integer.toString(req)))
                    .thenAccept((v) -> respList2.add(Integer.parseInt(v.toStringUtf8()))));
            }
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        // the resp order preserved
        assertEquals(reqList1, respList1);
        assertEquals(reqList2, respList2);
        executor1.shutdown();
        executor2.shutdown();
    }
}