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

import static org.apache.bifromq.basekv.proto.State.StateType.Merged;
import static org.apache.bifromq.basekv.proto.State.StateType.Normal;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.NULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.combine;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.basekv.MockableTest;
import org.apache.bifromq.basekv.TestCoProcFactory;
import org.apache.bifromq.basekv.localengine.rocksdb.RocksDBCPableKVEngineConfigurator;
import org.apache.bifromq.basekv.localengine.rocksdb.RocksDBWALableKVEngineConfigurator;
import org.apache.bifromq.basekv.proto.EnsureRange;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeMessage;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.proto.StoreMessage;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import org.apache.bifromq.basekv.store.exception.KVRangeException;
import org.apache.bifromq.basekv.store.option.KVRangeStoreOptions;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.util.VerUtil;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeStoreTest extends MockableTest {
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
        assertTrue(rangeStore.bootstrap(KVRangeIdUtil.generate(), FULL_BOUNDARY).join());
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
    public void testKeyExist() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);
        // split into [, key4) and [key4, )
        rangeStore.split(0, id, copyFromUtf8("key4")).toCompletableFuture().join();
        long verAfterSplit = VerUtil.bump(0, true);
        rangeStore.put(verAfterSplit, id, copyFromUtf8("key1"), copyFromUtf8("hello")).toCompletableFuture().join();
        {
            log.info("Test KeyExist");
            assertTrue(rangeStore.exist(verAfterSplit, id, copyFromUtf8("key1"), true).toCompletableFuture().join());

            assertFalse(rangeStore.exist(verAfterSplit, id, copyFromUtf8("key2"), true).toCompletableFuture().join());
        }
        {
            log.info("Test KeyExist with version mismatch");
            try {
                rangeStore.exist(0, id, copyFromUtf8("key2"), true).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.BadVersion);
            }
        }
        {
            log.info("Test KeyExist with out-of-range key");
            try {
                rangeStore.exist(verAfterSplit, id, copyFromUtf8("key4"), true).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.InternalException);
            }
        }
    }

    @SneakyThrows
    @Test(groups = "integration")
    public void testPutKey() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);
        // split into [, key4) and [key4, )
        rangeStore.split(0, id, copyFromUtf8("key4")).toCompletableFuture().join();
        long verAfterSplit = VerUtil.bump(0, true);
        {
            log.info("Test PutKV");
            assertEquals(
                rangeStore.put(verAfterSplit, id, copyFromUtf8("key1"), copyFromUtf8("hello")).toCompletableFuture()
                    .join(),
                ByteString.empty());

            assertEquals(
                rangeStore.put(verAfterSplit, id, copyFromUtf8("key2"), copyFromUtf8("hello")).toCompletableFuture()
                    .join(),
                ByteString.empty());

            assertEquals(
                rangeStore.put(verAfterSplit, id, copyFromUtf8("key3"), copyFromUtf8("hello")).toCompletableFuture()
                    .join(),
                ByteString.empty());
        }
        {
            log.info("Test PutKV with version mismatch");
            try {
                rangeStore.put(0, id, copyFromUtf8("key1"), copyFromUtf8("hello")).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.BadVersion);
            }
        }
        {
            log.info("Test PutKV with out-of-range key");
            try {
                rangeStore.put(verAfterSplit, id, copyFromUtf8("key4"), copyFromUtf8("hello")).toCompletableFuture()
                    .join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.InternalException);
            }
        }
    }

    @Test(groups = "integration")
    public void testGetKey() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);
        // split into [, key4) and [key4, )
        rangeStore.split(0, id, copyFromUtf8("key4")).toCompletableFuture().join();
        long verAfterSplit = VerUtil.bump(0, true);
        rangeStore.put(verAfterSplit, id, copyFromUtf8("key1"), copyFromUtf8("hello")).toCompletableFuture().join();
        {
            log.info("Test Get");
            assertEquals(
                rangeStore.get(verAfterSplit, id, copyFromUtf8("key1"), true).toCompletableFuture().join().get(),
                copyFromUtf8("hello"));
            assertEquals(
                rangeStore.get(verAfterSplit, id, copyFromUtf8("key1"), false).toCompletableFuture().join().get(),
                copyFromUtf8("hello"));
            assertFalse(
                rangeStore.get(verAfterSplit, id, copyFromUtf8("key2"), true).toCompletableFuture().join().isPresent());
        }
        {
            log.info("Test Get with version mismatch");
            try {
                rangeStore.get(0, id, copyFromUtf8("key1"), true).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.BadVersion);
            }
        }
        {
            log.info("Test Get with out-of-range key");
            try {
                rangeStore.get(verAfterSplit, id, copyFromUtf8("key4"), true).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.InternalException);
            }
        }
    }

    @Test(groups = "integration")
    public void testDeleteKey() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);
        // split into [, key4) and [key4, )
        rangeStore.split(0, id, copyFromUtf8("key4")).toCompletableFuture().join();
        long verAfterSplit = VerUtil.bump(0, true);
        rangeStore.put(verAfterSplit, id,
            copyFromUtf8("key1"), copyFromUtf8("hello")).toCompletableFuture().join();

        {
            log.info("Test Delete");
            ByteString delVal = rangeStore.delete(verAfterSplit, id, copyFromUtf8("key1")).toCompletableFuture().join();
            assertEquals(delVal, copyFromUtf8("hello"));

            assertFalse(
                rangeStore.get(verAfterSplit, id, copyFromUtf8("key1"), true).toCompletableFuture().join().isPresent());

            assertEquals(rangeStore.delete(verAfterSplit, id, copyFromUtf8("key2")).toCompletableFuture().join(),
                ByteString.empty());
        }
        {
            log.info("Test Delete with version mismatch");
            try {
                rangeStore.delete(0, id, copyFromUtf8("key1")).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.BadVersion);
            }
        }
        {
            log.info("Test Delete with out-of-range key");
            try {
                rangeStore.delete(verAfterSplit, id, copyFromUtf8("key4")).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.InternalException);
            }
        }
    }

    @Test(groups = "integration")
    public void testExecROCoProc() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);
        // split into [, key4) and [key4, )
        rangeStore.split(0, id, copyFromUtf8("key4")).toCompletableFuture().join();
        long verAfterSplit = VerUtil.bump(0, true);
        rangeStore.put(verAfterSplit, id, copyFromUtf8("key1"), copyFromUtf8("hello")).toCompletableFuture().join();

        {
            log.info("Test exec ReadOnly Co-Proc");
            assertEquals(
                rangeStore.queryCoProc(verAfterSplit, id, toInput(copyFromUtf8("key1")), true).toCompletableFuture()
                    .join()
                    .getRaw(),
                copyFromUtf8("hello"));
            assertEquals(
                rangeStore.queryCoProc(verAfterSplit, id, toInput(copyFromUtf8("key1")), false).toCompletableFuture()
                    .join()
                    .getRaw(),
                copyFromUtf8("hello"));
        }
        {
            log.info("Test exec ReadOnly CoProc with version mismatch");
            try {
                rangeStore.queryCoProc(0, id, toInput(copyFromUtf8("key1")), true).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.BadVersion);
            }
        }
        {
            log.info("Test exec ReadOnly Range with wrong ranges");
            try {
                rangeStore.queryCoProc(verAfterSplit, id, toInput(copyFromUtf8("key4")), true).toCompletableFuture()
                    .join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.InternalException);
            }
        }
    }

    @Test(groups = "integration")
    public void testExecRWCoProc() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);
        // split into [, key4) and [key4, )
        rangeStore.split(0, id, copyFromUtf8("key4")).toCompletableFuture().join();
        long verAfterSplit = VerUtil.bump(0, true);
        rangeStore.put(verAfterSplit, id, copyFromUtf8("key1"), copyFromUtf8("hello")).toCompletableFuture().join();

        {
            log.info("Test exec ReadWrite Co-Proc");
            assertEquals(
                rangeStore.mutateCoProc(verAfterSplit, id,
                        RWCoProcInput.newBuilder().setRaw(copyFromUtf8("key1_world")).build())
                    .toCompletableFuture().join().getRaw(),
                copyFromUtf8("hello"));

            assertTrue(
                rangeStore.get(verAfterSplit, id, copyFromUtf8("key1"), true).toCompletableFuture().join().isPresent());
        }
        {
            log.info("Test exec ReadWrite CoProc with version mismatch");
            try {
                rangeStore.mutateCoProc(0, id, RWCoProcInput.newBuilder().setRaw(copyFromUtf8("key1_hello")).build())
                    .toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.BadVersion);
            }
        }
        {
            log.info("Test exec ReadWrite Range with wrong ranges");
            try {
                rangeStore.mutateCoProc(verAfterSplit, id,
                        RWCoProcInput.newBuilder().setRaw(copyFromUtf8("key4")).build())
                    .toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.InternalException);
            }
        }
    }

    @Test(groups = "integration")
    public void testSplit() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);
        {
            log.info("Normal split test");
            // normal split
            rangeStore.split(0, id, copyFromUtf8("a")).toCompletableFuture().join();
            List<KVRangeDescriptor> ls = await().forever().until(() -> rangeStore.describe().blockingFirst(),
                storeDescriptor -> storeDescriptor.getRangesList().size() == 2).getRangesList();
            assertEquals(combine(ls.get(0).getBoundary(), ls.get(1).getBoundary()), FULL_BOUNDARY);
        }
        long verAfterSplit = VerUtil.bump(0, true);
        {
            log.info("Split with version mismatch test");
            // version mismatch
            try {
                rangeStore.split(0, id, copyFromUtf8("a")).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.BadVersion);
            }
        }
        {
            log.info("Split with invalid split key test");
            // invalid split key
            try {
                rangeStore.split(verAfterSplit, id, copyFromUtf8("a")).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.BadRequest);
            }
        }
    }

    @Test(groups = "integration")
    public void testMerge() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);
        log.info("Splitting bucket");
        rangeStore.split(0, id, copyFromUtf8("d")).toCompletableFuture().join();
        // [-,d) [d,-)
        KVRangeStoreDescriptor storeDescriptor = await().until(() ->
                rangeStore.describe().blockingFirst(),
            storeDesc -> {
                if (storeDesc.getRangesList().size() != 2) {
                    return false;
                }
                return storeDesc.getRangesList().stream()
                    .allMatch(d -> d.getState() == Normal && d.getRole() == RaftNodeStatus.Leader);
            }
        );
        log.info("{}", storeDescriptor);
        KVRangeDescriptor merger = storeDescriptor.getRangesList().get(0).getId().equals(id) ?
            storeDescriptor.getRangesList().get(0) : storeDescriptor.getRangesList().get(1);
        KVRangeDescriptor mergee = storeDescriptor.getRangesList().get(1).getId().equals(id) ?
            storeDescriptor.getRangesList().get(0) : storeDescriptor.getRangesList().get(1);
        log.info("Start Merging");
        rangeStore.merge(merger.getVer(), merger.getId(), mergee.getId()).toCompletableFuture().join();
        KVRangeDescriptor mergeeDesc = await().atMost(Duration.ofSeconds(10000)).until(() ->
                rangeStore.describe()
                    .flatMap(sd -> Observable.fromIterable(sd.getRangesList()))
                    .filter(rd -> rd.getId().equals(mergee.getId())).blockingFirst(),
            rangeDesc -> rangeDesc.getState() == Merged && rangeDesc.getRole() == RaftNodeStatus.Leader
        );
        log.info("Mergee Descriptor: {}", mergeeDesc);
        rangeStore.changeReplicaConfig(mergeeDesc.getVer(), mergeeDesc.getId(), emptySet(), emptySet())
            .toCompletableFuture().join();
        storeDescriptor = await().atMost(Duration.ofSeconds(10000)).until(() ->
            rangeStore.describe().blockingFirst(), storeDesc -> storeDesc.getRangesList().size() == 1
        );
        log.info("Store Descriptor: {}", storeDescriptor);
    }

    @Test(groups = "integration")
    public void testRangeEnsure() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);

        KVRangeId newId = KVRangeIdUtil.next(id);
        incomingStoreMessage.onNext(StoreMessage.newBuilder()
            .setFrom("newVoter")
            .setSrcRange(newId)
            .setPayload(KVRangeMessage.newBuilder()
                .setRangeId(newId)
                .setHostStoreId(rangeStore.id())
                .setEnsureRange(EnsureRange.newBuilder()
                    .setInitSnapshot(Snapshot.newBuilder()
                        .setClusterConfig(ClusterConfig.getDefaultInstance())
                        .setTerm(0)
                        .setIndex(0)
                        .setData(KVRangeSnapshot.newBuilder()
                            .setId(newId)
                            .setVer(0)
                            .setLastAppliedIndex(0)
                            .setState(State.newBuilder().setType(Normal).build())
                            .setBoundary(NULL_BOUNDARY)
                            .setClusterConfig(ClusterConfig.getDefaultInstance())
                            .build().toByteString())
                        .build())
                    .build())
                .build())
            .build());
        await().until(() -> rangeStore.describe().blockingFirst(),
            storeDescriptor -> storeDescriptor.getRangesList().size() == 2).getRangesList();
    }

    private void waitNormalState(KVRangeDescriptor rangeDescriptor, long timeoutInMS) {
        await().atMost(Duration.ofMillis(timeoutInMS))
            .until(() -> rangeDescriptor.getState() == Normal && rangeDescriptor.getRole() == RaftNodeStatus.Leader);
    }

    private long reqId() {
        return System.nanoTime();
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

    private ROCoProcInput toInput(ByteString raw) {
        return ROCoProcInput.newBuilder().setRaw(raw).build();
    }
}
