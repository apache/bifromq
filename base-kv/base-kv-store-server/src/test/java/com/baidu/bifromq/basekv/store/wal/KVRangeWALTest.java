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

package com.baidu.bifromq.basekv.store.wal;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.baidu.bifromq.basekv.MockableTest;
import com.baidu.bifromq.basekv.proto.KVRangeCommand;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.raft.IRaftStateStore;
import com.baidu.bifromq.basekv.raft.InMemoryStateStore;
import com.baidu.bifromq.basekv.raft.RaftConfig;
import com.baidu.bifromq.basekv.raft.event.CommitEvent;
import com.baidu.bifromq.basekv.raft.event.ElectionEvent;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.LogEntry;
import com.baidu.bifromq.basekv.raft.proto.RaftMessage;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import com.baidu.bifromq.basekv.raft.proto.Voting;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.observers.TestObserver;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeWALTest extends MockableTest {

    private static class InMemoryKVRangeWALStore implements IKVRangeWALStore {
        private IRaftStateStore delegate;

        InMemoryKVRangeWALStore(String replicaId) {
            delegate = new InMemoryStateStore(replicaId, Snapshot.newBuilder()
                .setClusterConfig(ClusterConfig.newBuilder()
                    .addVoters(replicaId)
                    .build())
                .build());
        }

        InMemoryKVRangeWALStore(String replicaId, Snapshot snapshot) {
            delegate = new InMemoryStateStore(replicaId, snapshot);
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public void destroy() {

        }

        @Override
        public String local() {
            return delegate.local();
        }

        @Override
        public long currentTerm() {
            return delegate.currentTerm();
        }

        @Override
        public void saveTerm(long term) {
            delegate.saveTerm(term);
        }

        @Override
        public Optional<Voting> currentVoting() {
            return delegate.currentVoting();
        }

        @Override
        public void saveVoting(Voting voting) {
            delegate.saveVoting(voting);
        }

        @Override
        public ClusterConfig latestClusterConfig() {
            return delegate.latestClusterConfig();
        }

        @Override
        public void applySnapshot(Snapshot snapshot) {
            delegate.applySnapshot(snapshot);
        }

        @Override
        public Snapshot latestSnapshot() {
            return delegate.latestSnapshot();
        }

        @Override
        public long firstIndex() {
            return delegate.firstIndex();
        }

        @Override
        public long lastIndex() {
            return delegate.lastIndex();
        }

        @Override
        public Optional<LogEntry> entryAt(long index) {
            return delegate.entryAt(index);
        }

        @Override
        public Iterator<LogEntry> entries(long lo, long hi, long maxSize) {
            return delegate.entries(lo, hi, maxSize);
        }

        @Override
        public void append(List<LogEntry> entries, boolean flush) {
            delegate.append(entries, flush);
        }

        @Override
        public void addStableListener(IRaftStateStore.StableListener listener) {
            delegate.addStableListener(listener);
        }

        @Override
        public void stop() {
            delegate.stop();
        }
    }

    private KVRangeId id = KVRangeIdUtil.generate();

    private String replicaId = "s1";

    private IKVRangeWALStore raftStateStorage = new InMemoryKVRangeWALStore(replicaId);
    private RaftConfig config = new RaftConfig();
    private ScheduledExecutorService ticker;

    @Override
    protected void doSetup(Method method) {
        raftStateStorage = new InMemoryKVRangeWALStore(replicaId);
        ticker = new ScheduledThreadPoolExecutor(1);
    }

    @Override
    public void doTeardown(Method method) {
        MoreExecutors.shutdownAndAwaitTermination(ticker, 5, TimeUnit.SECONDS);
        raftStateStorage.stop();
    }


    @Test
    public void testId() {

        KVRangeWAL wal = new KVRangeWAL("testcluster", replicaId, id, raftStateStorage, config, 1024);
        assertEquals(wal.storeId(), replicaId);
    }

    @SneakyThrows
    @Test
    public void testElectionObservable() {
        KVRangeWAL wal = new KVRangeWAL("testcluster", replicaId, id, raftStateStorage, config, 1024);
        TestObserver<ElectionEvent> testObserver = new TestObserver<>();
        wal.election().subscribe(testObserver);
        wal.start();
        ScheduledFuture<?> tickTask = ticker.scheduleAtFixedRate(wal::tick, 0, 100, TimeUnit.MILLISECONDS);

        testObserver.awaitCount(1);
        assertTrue(wal.isLeader());
        assertTrue(wal.currentLeader().isPresent());
        ElectionEvent election = testObserver.values().get(0);
        assertEquals(election.leaderId, replicaId);
        assertEquals(election.term, 1);

        tickTask.cancel(true);
        wal.close().join();
        testObserver.assertComplete();
    }

    @Test
    public void testCommitIndexObservable() {
        KVRangeWAL wal = new KVRangeWAL("testcluster", replicaId, id, raftStateStorage, config, 1024);
        wal.start();
        TestObserver<Long> testObserver = new TestObserver<>();
        wal.commitIndex().subscribe(testObserver);
        wal.onRaftEvent(new CommitEvent(replicaId, 10L));
        testObserver.awaitCount(1);
        testObserver.assertValue(10L);
        wal.close().join();
        testObserver.assertComplete();
    }

    @Test
    public void testSnapshotObservable() {
        KVRangeWAL wal = new KVRangeWAL("testcluster", replicaId, id, raftStateStorage, config, 1024);
        wal.start();

        TestObserver<IKVRangeWAL.SnapshotInstallTask> testObserver = new TestObserver<>();
        wal.snapshotInstallTask().subscribe(testObserver);
        CompletableFuture<ByteString> onDone = wal.install(ByteString.EMPTY, "leader");
        testObserver.awaitCount(1);
        IKVRangeWAL.SnapshotInstallTask task = testObserver.values().get(0);
        assertEquals(task.snapshot, KVRangeSnapshot.getDefaultInstance());
        wal.close().join();
        testObserver.assertComplete();
    }

    @SneakyThrows
    @Test
    public void snapshotTaskTest() {
        IKVRangeWAL.SnapshotInstallTask task =
            new IKVRangeWAL.SnapshotInstallTask(ByteString.copyFromUtf8("BadData"), "leader");
        assertTrue(task.onDone.isCompletedExceptionally());

        task = new IKVRangeWAL.SnapshotInstallTask(ByteString.empty(), "leader");
        assertEquals(task.snapshot, KVRangeSnapshot.parseFrom(ByteString.empty()));
        assertFalse(task.onDone.isDone());
    }

    @Test
    public void testPeerMessagesObservable() {
        KVRangeWAL wal = new KVRangeWAL("testcluster", replicaId, id, raftStateStorage, config, 1024);
        wal.start();
        TestObserver<Map<String, List<RaftMessage>>> testObserver = new TestObserver<>();
        wal.peerMessages().subscribe(testObserver);
        wal.sendRaftMessages(Collections.emptyMap());
        testObserver.awaitCount(1);
        testObserver.assertValueAt(0, Collections.emptyMap());
        wal.close().join();
        testObserver.assertComplete();
    }

    @Test
    public void testProposeOnce() {
        KVRangeWAL wal = new KVRangeWAL("testcluster", replicaId, id, raftStateStorage, config, 1024);
        wal.start();
        ScheduledFuture<?> tickTask = ticker.scheduleAtFixedRate(wal::tick, 0, 100, TimeUnit.MILLISECONDS);
        await().until(() -> wal.currentLeader().isPresent());
        wal.propose(KVRangeCommand.getDefaultInstance())
            .handle((v, e) -> {
                if (e != null) {
                    fail();
                }
                return null;
            })
            .join();
        tickTask.cancel(true);
        wal.close().join();
    }

    @Test
    public void testProposeOnceButFailed() {
        KVRangeWAL wal =
            new KVRangeWAL("testcluster", replicaId, id, new InMemoryKVRangeWALStore(replicaId, Snapshot.newBuilder()
                .setClusterConfig(ClusterConfig.newBuilder()
                    .addVoters(replicaId)
                    .addVoters("FakeReplica")
                    .build())
                .build()), config, 1024);
        wal.start();
        ScheduledFuture<?> tickTask = ticker.scheduleAtFixedRate(wal::tick, 0, 100, TimeUnit.MILLISECONDS);
        await().until(() -> wal.currentState() == RaftNodeStatus.Candidate);
        wal.propose(KVRangeCommand.getDefaultInstance())
            .handle((v, e) -> {
                if (e == null) {
                    fail();
                }
                return null;
            }).join();
        tickTask.cancel(true);
        wal.close().join();
    }
}
