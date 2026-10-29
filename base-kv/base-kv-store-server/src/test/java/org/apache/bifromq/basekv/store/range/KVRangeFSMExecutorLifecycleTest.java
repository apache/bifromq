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

package org.apache.bifromq.basekv.store.range;

import static org.apache.bifromq.basekv.localengine.StructUtil.toValue;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_ROOT_DIR;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.micrometer.core.instrument.Metrics;
import io.reactivex.rxjava3.core.Observable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.basekv.TestCoProcFactory;
import org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import org.apache.bifromq.basekv.store.option.KVRangeOptions;
import org.apache.bifromq.basekv.store.wal.KVRangeWALStorageEngine;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.testng.annotations.Test;

/**
 * Checks that range destruction releases executor meters.
 */
public class KVRangeFSMExecutorLifecycleTest extends AbstractKVRangeTest {
    @Test
    public void destroyReleasesMetersBeforeCompletion() throws Exception {
        KVRangeWALStorageEngine walEngine = new KVRangeWALStorageEngine("fsm-test", null, "rocksdb",
            RocksDBDefaultConfigs.WAL.toBuilder()
                .putFields(DB_ROOT_DIR, toValue(dbRootDir.resolve("wal").toString())).build());
        walEngine.start();
        ExecutorService auxiliary = Executors.newSingleThreadExecutor();
        CompletableFuture<Void> destroying = new CompletableFuture<>();
        CompletableFuture<Void> release = new CompletableFuture<>();
        KVRangeId id = KVRangeIdUtil.generate();
        ClusterConfig config = ClusterConfig.newBuilder().addVoters(walEngine.id()).build();
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder().setId(id).setVer(0).setLastAppliedIndex(0)
            .setBoundary(FULL_BOUNDARY).setState(State.newBuilder().setType(State.StateType.Normal))
            .setClusterConfig(config).build();
        IKVRange range = spy(new KVRange(id, kvEngine.createIfMissing(KVRangeIdUtil.toString(id)), snapshot));
        doAnswer(invocation -> {
            destroying.complete(null);
            release.join();
            return invocation.callRealMethod();
        }).when(range).destroy();
        KVRangeFSM fsm = fsm(range, snapshot, walEngine, auxiliary);
        IKVRangeMessenger messenger = mock(IKVRangeMessenger.class);
        when(messenger.receive()).thenReturn(Observable.never());
        try {
            fsm.open(messenger).get(5, TimeUnit.SECONDS);
            assertEquals(executorMeters(id), 18L);
            CompletableFuture<Void> destroyed = fsm.destroy();
            destroying.get(5, TimeUnit.SECONDS);
            CompletableFuture<Long> metersAtCompletion = destroyed.thenApply(v -> executorMeters(id));
            release.complete(null);
            assertEquals(metersAtCompletion.get(5, TimeUnit.SECONDS), Long.valueOf(0));
            IKVRange replacementRange = new KVRange(id,
                kvEngine.createIfMissing(KVRangeIdUtil.toString(id)), snapshot);
            KVRangeFSM replacement = fsm(replacementRange, snapshot, walEngine, auxiliary);
            try {
                replacement.open(messenger).get(5, TimeUnit.SECONDS);
                assertEquals(executorMeters(id), 18L);
            } finally {
                replacement.close().get(5, TimeUnit.SECONDS);
            }
            assertEquals(executorMeters(id), 0L);
        } finally {
            release.complete(null);
            fsm.close().get(5, TimeUnit.SECONDS);
            walEngine.stop();
            auxiliary.shutdown();
            assertTrue(auxiliary.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private KVRangeFSM fsm(IKVRange range, KVRangeSnapshot snapshot, KVRangeWALStorageEngine walEngine,
                           ExecutorService auxiliary) {
        Snapshot walSnapshot = Snapshot.newBuilder().setClusterConfig(snapshot.getClusterConfig())
            .setData(snapshot.toByteString()).build();
        return new KVRangeFSM("fsm-test", walEngine.id(), snapshot.getId(), new TestCoProcFactory(), range,
            walEngine.create(snapshot.getId(), walSnapshot), auxiliary, auxiliary, new KVRangeOptions(),
            List.of(), (closed, reset) -> { }, "clusterId", "fsm-test", "storeId", walEngine.id(),
            "rangeId", KVRangeIdUtil.toString(snapshot.getId()));
    }

    private long executorMeters(KVRangeId id) {
        return Metrics.globalRegistry.getMeters().stream()
            .filter(meter -> meter.getId().getName().startsWith("basekv.range.executor")
                && KVRangeIdUtil.toString(id).equals(meter.getId().getTag("rangeId"))).count();
    }
}
