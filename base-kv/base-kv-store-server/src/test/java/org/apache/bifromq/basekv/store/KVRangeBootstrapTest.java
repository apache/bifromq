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

package org.apache.bifromq.basekv.store;

import static java.util.Collections.emptyMap;
import static org.apache.bifromq.basekv.proto.State.StateType.Normal;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.Any;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.basekv.MockableTest;
import org.apache.bifromq.basekv.TestCoProcFactory;
import org.apache.bifromq.basekv.localengine.rocksdb.RocksDBCPableKVEngineConfigurator;
import org.apache.bifromq.basekv.localengine.rocksdb.RocksDBWALableKVEngineConfigurator;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeMessage;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.proto.StoreMessage;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.raft.proto.RaftNodeSyncState;
import org.apache.bifromq.basekv.store.option.KVRangeStoreOptions;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeBootstrapTest extends MockableTest {
    private final String DB_NAME = "testDB";
    private final String DB_CHECKPOINT_DIR_NAME = "testDB_cp";
    private final String DB_WAL_NAME = "testWAL";
    private final String DB_WAL_CHECKPOINT_DIR = "testWAL_cp";
    private final KVRangeStoreOptions options = new KVRangeStoreOptions();
    private final PublishSubject<StoreMessage> incomingStoreMessage = PublishSubject.create();
    private final int tickerThreads = 2;
    public Path dbRootDir;
    private IKVRangeStore rangeStore;
    private ExecutorService queryExecutor;
    private ScheduledExecutorService bgTaskExecutor;

    @SneakyThrows
    protected void doSetup(Method method) {
        options.getKvRangeOptions().getWalRaftConfig().setAsyncAppend(false);
        options.getKvRangeOptions().getWalRaftConfig().setInstallSnapshotTimeoutTick(10);

        queryExecutor = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
            EnvProvider.INSTANCE.newThreadFactory("query-executor"));
        bgTaskExecutor = new ScheduledThreadPoolExecutor(1,
            EnvProvider.INSTANCE.newThreadFactory("bg-task-executor"));

        dbRootDir = Files.createTempDirectory("");
        (((RocksDBCPableKVEngineConfigurator) options.getDataEngineConfigurator()))
            .dbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR_NAME)
                .toString())
            .dbRootDir(Paths.get(dbRootDir.toString(), DB_NAME).toString());
        ((RocksDBWALableKVEngineConfigurator) options.getWalEngineConfigurator())
            .dbRootDir(Paths.get(dbRootDir.toString(), DB_WAL_NAME).toString());

        rangeStore =
            new KVRangeStore("testCluster",
                options,
                new TestCoProcFactory(),
                queryExecutor,
                tickerThreads,
                bgTaskExecutor,
                emptyMap());
        IStoreMessenger messenger = new IStoreMessenger() {
            @Override
            public void send(StoreMessage message) {
                KVRangeMessage payload = message.getPayload();
                if (!payload.hasHostStoreId()) {
                    incomingStoreMessage.onNext(message.toBuilder()
                        .setPayload(payload.toBuilder()
                            .setHostStoreId(rangeStore.id())
                            .build())
                        .build());
                    return;
                }
                if (payload.getHostStoreId().equals(rangeStore.id())) {
                    incomingStoreMessage.onNext(message);
                }
            }

            @Override
            public Observable<StoreMessage> receive() {
                return incomingStoreMessage;
            }

            @Override
            public void close() {

            }
        };

        rangeStore.start(messenger);
    }

    protected void doTearDown(Method method) {
        rangeStore.stop();
        queryExecutor.shutdownNow();
        bgTaskExecutor.shutdownNow();
        if (dbRootDir != null) {
            try {
                Files.walk(dbRootDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            } catch (IOException e) {
                log.error("Failed to delete db root dir", e);
            }
            dbRootDir = null;
        }
        log.info("Shutdown read task executor");
    }

    @Test(groups = "integration")
    public void testBootStrap() {
        boolean success = rangeStore.bootstrap(KVRangeIdUtil.generate(), FULL_BOUNDARY).join();
        assertTrue(success);
        KVRangeDescriptor descriptor = firstRangeDescriptor();
        KVRangeId id = descriptor.getId();
        assertEquals(descriptor.toBuilder().clearStatistics().setHlc(0).build(), KVRangeDescriptor.newBuilder()
            .setId(id)
            .setVer(0)
            .setRole(RaftNodeStatus.Leader)
            .setState(Normal)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder().addVoters(rangeStore.id()).build())
            .putSyncState(rangeStore.id(), RaftNodeSyncState.Replicating)
            .setFact(Any.getDefaultInstance())
            .setReadyForQuery(true)
            .build());
    }

    @Test(groups = "integration")
    public void bootstrapSameRangeTwice() {
        KVRangeId id = KVRangeIdUtil.generate();
        boolean success = rangeStore.bootstrap(id, FULL_BOUNDARY).join();
        assertTrue(success);
        success = rangeStore.bootstrap(id, FULL_BOUNDARY).join();
        assertFalse(success);
    }

    private KVRangeDescriptor firstRangeDescriptor() {
        return rangeStore.describe().mapOptional(this::mapToLeader).blockingFirst();
    }

    private Optional<KVRangeDescriptor> mapToLeader(KVRangeStoreDescriptor storeDescriptor) {
        KVRangeDescriptor descriptor = storeDescriptor.getRangesList().get(0);
        if (descriptor.getRole() == RaftNodeStatus.Leader) {
            return Optional.of(descriptor);
        }
        return Optional.empty();
    }
}
