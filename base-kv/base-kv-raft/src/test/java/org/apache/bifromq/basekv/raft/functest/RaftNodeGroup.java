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

package org.apache.bifromq.basekv.raft.functest;

import org.apache.bifromq.basekv.raft.IRaftNode;
import org.apache.bifromq.basekv.raft.IRaftStateStore;
import org.apache.bifromq.basekv.raft.InMemoryStateStore;
import org.apache.bifromq.basekv.raft.RaftConfig;
import org.apache.bifromq.basekv.raft.RaftNode;
import org.apache.bifromq.basekv.raft.event.CommitEvent;
import org.apache.bifromq.basekv.raft.event.ElectionEvent;
import org.apache.bifromq.basekv.raft.event.SnapshotRestoredEvent;
import org.apache.bifromq.basekv.raft.event.SyncStateChangedEvent;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.raft.proto.RaftMessage;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.raft.proto.RaftNodeSyncState;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;

@Slf4j
public final class RaftNodeGroup {
    public final static RaftConfig DefaultRaftConfig = new RaftConfig();
    public final static int RaftNodeTickMagnitude = 5;
    private final ConcurrentHashMap<String, RaftNode> nodes = new ConcurrentHashMap();
    private final RaftNodeNetwork network = new RaftNodeNetwork();
    private final ConcurrentHashMap<String, List<Long>> commitLogs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<ByteString>> snapshotLogs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<ElectionEvent>> electionLogs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<RaftNodeSyncState>> syncStateChangeLogs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<SnapshotRestoredEvent>> snapshotRestoreLogs =
        new ConcurrentHashMap<>();
    private final ScheduledExecutorService tickExecutor = Executors.newSingleThreadScheduledExecutor();
    private final Object commitSignal = new Object();
    private final Set<CountDownLatch> ticksWaiters = new ConcurrentHashMap<CountDownLatch, Integer>().newKeySet();
    private final AtomicBoolean ticking = new AtomicBoolean();
    private long tickInterval = 100;
    private TimeUnit tickUnit = TimeUnit.MILLISECONDS;
    private volatile long ticks = 0;

    public RaftNodeGroup(ClusterConfig initClusterConfig) {
        this(initClusterConfig, DefaultRaftConfig);
    }

    public RaftNodeGroup(ClusterConfig initClusterConfig, RaftConfig defaultRaftConfig) {
        initClusterConfig.getVotersList().forEach(id -> addRaftNode(id, 0, 0, initClusterConfig, defaultRaftConfig));
        initClusterConfig.getLearnersList().forEach(id -> addRaftNode(id, 0, 0, initClusterConfig, defaultRaftConfig));
        initClusterConfig.getNextVotersList()
            .forEach(id -> addRaftNode(id, 0, 0, initClusterConfig, defaultRaftConfig));
        initClusterConfig.getNextLearnersList()
            .forEach(id -> addRaftNode(id, 0, 0, initClusterConfig, defaultRaftConfig));
        initClusterConfig.getVotersList().forEach(this::connect);
        initClusterConfig.getLearnersList().forEach(this::connect);
        initClusterConfig.getNextVotersList().forEach(this::connect);
        initClusterConfig.getNextLearnersList().forEach(this::connect);
    }

    public RaftNodeGroup(ClusterConfig initClusterConfig, Map<String, RaftConfig> defaultRaftConfig) {
        initClusterConfig.getVotersList()
            .forEach(id -> addRaftNode(id, 0, 0, initClusterConfig, defaultRaftConfig.get(id)));
        initClusterConfig.getLearnersList()
            .forEach(id -> addRaftNode(id, 0, 0, initClusterConfig, defaultRaftConfig.get(id)));
        initClusterConfig.getNextVotersList()
            .forEach(id -> addRaftNode(id, 0, 0, initClusterConfig, defaultRaftConfig.get(id)));
        initClusterConfig.getNextLearnersList()
            .forEach(id -> addRaftNode(id, 0, 0, initClusterConfig, defaultRaftConfig.get(id)));
        initClusterConfig.getVotersList().forEach(this::connect);
        initClusterConfig.getLearnersList().forEach(this::connect);
        initClusterConfig.getNextVotersList().forEach(this::connect);
        initClusterConfig.getNextLearnersList().forEach(this::connect);
    }

    public void addRaftNode(String id, long snapLastIndex, long snapLastTerm, ClusterConfig clusterConfig,
                            RaftConfig raftConfig) {

        commitLogs.computeIfAbsent(id, k -> new ArrayList<>());
        snapshotLogs.computeIfAbsent(id, k -> new ArrayList<>());
        electionLogs.computeIfAbsent(id, k -> new ArrayList<>());
        syncStateChangeLogs.computeIfAbsent(id, k -> new ArrayList<>());

        Snapshot snapshot =
            Snapshot.newBuilder().setIndex(snapLastIndex).setTerm(snapLastTerm).setClusterConfig(clusterConfig).build();
        IRaftStateStore stateStorage = new InMemoryStateStore(id, snapshot);
        RaftNode raftNode =
            new RaftNode(raftConfig, stateStorage, Executors.defaultThreadFactory(), "id", id);
        nodes.put(id, raftNode);
    }

    public void removeRaftNode(String id) {
        log.info("Remove raft node: id={}", id);
        assert nodes.containsKey(id);
        network.disconnect(id);
        nodes.remove(id).stop().join();
        commitLogs.remove(id);
        snapshotLogs.remove(id);
        electionLogs.remove(id);
        syncStateChangeLogs.remove(id);
    }

    public void connect(String id) {
        assert nodes.containsKey(id);
        RaftNode node = nodes.get(id);
        IRaftNode.IRaftMessageSender msgSender = network.connect(node);
        node.start(msgSender,
            event -> {
                switch (event.type) {
                    case COMMIT:
                        if (commitLogs.containsKey(id)) {
                            commitLogs.get(id).add(((CommitEvent) event).index);
                            synchronized (commitSignal) {
                                commitSignal.notifyAll();
                            }
                        }
                        break;
                    case ELECTION:
                        if (electionLogs.containsKey(id)) {
                            electionLogs.get(id).add(((ElectionEvent) event));
                        }
                        break;
                    case STATUS_CHANGED:
                        break;
                    case SNAPSHOT_RESTORED:
                        snapshotRestoreLogs.computeIfAbsent(event.nodeId, k -> new ArrayList<>())
                            .add((SnapshotRestoredEvent) event);
                        break;
                    case SYNC_STATE_CHANGED:
                        Map<String, RaftNodeSyncState> peersRepStatus = ((SyncStateChangedEvent) event).states;
                        for (String peerId : peersRepStatus.keySet()) {
                            if (syncStateChangeLogs.containsKey(peerId)) {
                                List<RaftNodeSyncState> logs = syncStateChangeLogs.get(peerId);
                                if (!logs.isEmpty()) {
                                    if (logs.get(logs.size() - 1) != peersRepStatus.get(peerId)) {
                                        logs.add(peersRepStatus.get(peerId));
                                    }
                                } else {
                                    logs.add(peersRepStatus.get(peerId));
                                }
                            }
                        }
                        for (String peerId : syncStateChangeLogs.keySet()) {
                            if (!peersRepStatus.containsKey(peerId)) {
                                // mark peer has been stopped tracking
                                List<RaftNodeSyncState> logs = syncStateChangeLogs.get(peerId);
                                if (!logs.isEmpty()) {
                                    if (logs.get(logs.size() - 1) != null) {
                                        logs.add(null);
                                    }
                                }
                            }
                        }
                        break;
                }
            },
            (fsmSnapshot, leader, promise) -> {
                if (snapshotLogs.containsKey(id) && nodes.containsKey(id)) {
                    snapshotLogs.get(id).add(fsmSnapshot);
                }
                promise.call(fsmSnapshot, null);
            });
    }

    public void disconnect(String id) {
        assert nodes.containsKey(id);
        network.disconnect(id);
    }

    public List<Long> commitLog(String id) {
        assert commitLogs.containsKey(id);
        return Collections.unmodifiableList(commitLogs.get(id));
    }

    public List<LogEntry> logEntries(String id, long fromIndex) {
        assert commitLogs.containsKey(id);
        RaftNode node = nodes.get(id);
        try {
            List ret = new ArrayList();
            node.retrieveCommitted(fromIndex, Long.MAX_VALUE).get().forEachRemaining(ret::add);
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<ByteString> snapshotLog(String id) {
        assert snapshotLogs.containsKey(id);
        return Collections.unmodifiableList(snapshotLogs.get(id));
    }

    public List<SnapshotRestoredEvent> snapshotRestoredLogs(String id) {
        assert snapshotRestoreLogs.containsKey(id);
        return Collections.unmodifiableList(snapshotRestoreLogs.get(id));
    }

    public List<ElectionEvent> electionLog(String id) {
        assert electionLogs.containsKey(id);
        return Collections.unmodifiableList(electionLogs.get(id));
    }

    public boolean awaitIndexCommitted(String id, long index) {
        assert commitLogs.containsKey(id);
        synchronized (commitSignal) {
            List<Long> commitLog = commitLogs.get(id);
            while (commitLog.isEmpty() || commitLog.get(commitLog.size() - 1) < index) {
                try {
                    commitSignal.wait();
                } catch (InterruptedException e) {
                    break;
                }
            }
            return commitLog.get(commitLog.size() - 1) >= index;
        }
    }

    public void waitForNextElection() {
        Optional<ElectionEvent> prev = currentElection();
        Awaitility.await().until(() -> {
            Optional<ElectionEvent> now = currentElection();
            if (prev.isPresent()) {
                return now.isPresent() &&
                    (!now.get().leaderId.equals(prev.get().leaderId) || now.get().term > prev.get().term);
            } else {
                return now.isPresent();
            }
        });
    }

    public void await(int ticks) {
        try {
            CountDownLatch latch = new CountDownLatch(ticks);
            ticksWaiters.add(latch);
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Optional<ElectionEvent> currentElection() {
        ElectionEvent latest = null;
        for (IRaftNode raftNode : nodes.values()) {
            List<ElectionEvent> electionLog = electionLogs.get(raftNode.id());
            if (!electionLog.isEmpty()) {
                ElectionEvent event = electionLog.get(electionLog.size() - 1);
                if (latest == null) {
                    latest = event;
                } else {
                    latest = latest.term < event.term ? event : latest;
                }
            }
        }
        if (latest != null) {
            if (nodes.get(latest.leaderId).status() == RaftNodeStatus.Leader) {
                return Optional.of(latest);
            }
        }
        return Optional.empty();
    }

    public Optional<String> currentLeader() {
        return currentElection().map(e -> e.leaderId);
    }

    public List<String> currentFollowers() {
        return nodes.values()
            .stream()
            .filter(node -> node.status() == RaftNodeStatus.Follower &&
                (node.latestClusterConfig().getVotersList().contains(node.id())
                    || node.latestClusterConfig().getNextVotersList().contains(node.id())))
            .map(RaftNode::id)
            .collect(Collectors.toList());
    }

    public List<String> currentLearners() {
        return nodes.values()
            .stream()
            .filter(node -> node.status() == RaftNodeStatus.Follower && (
                node.latestClusterConfig().getLearnersList().contains(node.id())
                    || node.latestClusterConfig().getNextLearnersList().contains(node.id())))
            .map(RaftNode::id)
            .collect(Collectors.toList());
    }

    public List<String> currentCandidates() {
        return nodes.values()
            .stream()
            .filter(node -> node.status() == RaftNodeStatus.Candidate)
            .map(RaftNode::id)
            .collect(Collectors.toList());
    }

    public RaftNodeStatus nodeState(String id) {
        assert nodes.containsKey(id);
        return nodes.get(id).status();
    }

    public List<RaftNodeSyncState> syncStateLogs(String id) {
        return Arrays.asList(syncStateChangeLogs.get(id).toArray(new RaftNodeSyncState[] {}));
    }

    public RaftNodeSyncState latestReplicationStatus(String id) {
        if (!syncStateChangeLogs.isEmpty()) {
            return syncStateChangeLogs.get(id).get(syncStateChangeLogs.get(id).size() - 1);
        }
        return null;
    }

    public ClusterConfig latestClusterConfig(String id) {
        assert nodes.containsKey(id);
        return nodes.get(id).latestClusterConfig();
    }

    public ByteString latestSnapshot(String id) {
        assert nodes.containsKey(id);
        return nodes.get(id).latestSnapshot();
    }

    public boolean stepDown(String id) {
        assert nodes.containsKey(id);
        try {
            return nodes.get(id).stepDown();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<Void> recover(String id) {
        assert nodes.containsKey(id);
        try {
            return nodes.get(id).recover();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<Long> propose(String id, ByteString fsmCmd) {
        assert nodes.containsKey(id);
        return nodes.get(id).propose(fsmCmd);
    }

    public CompletableFuture<Long> readIndex(String id) {
        assert nodes.containsKey(id);
        return nodes.get(id).readIndex();
    }

    public CompletableFuture<Void> compact(String id, ByteString fsmSnapshot, long lastAppliedIndex) {
        assert nodes.containsKey(id);
        return nodes.get(id).compact(fsmSnapshot, lastAppliedIndex);
    }

    public CompletableFuture<Void> transferLeadership(String id, String newLeader) {
        assert nodes.containsKey(id);
        return nodes.get(id).transferLeadership(newLeader);
    }

    public CompletableFuture<Void> changeClusterConfig(String id, Set<String> voters, Set<String> learners) {
        assert nodes.containsKey(id);
        return nodes.get(id).changeClusterConfig(UUID.randomUUID().toString(), voters, learners);
    }

    public CompletableFuture<Void> changeClusterConfig(String id, String correlateId,
                                                       Set<String> voters, Set<String> learners) {
        assert nodes.containsKey(id);
        return nodes.get(id).changeClusterConfig(correlateId, voters, learners);
    }

    public List<LogEntry> retrieveCommitted(String id, long fromIndex, long maxSize) {
        assert nodes.containsKey(id);
        List<LogEntry> entries = new ArrayList<>();
        nodes.get(id).retrieveCommitted(fromIndex, maxSize).join().forEachRemaining(entries::add);
        return entries;
    }

    public Optional<LogEntry> entryAt(String id, long index) {
        assert nodes.containsKey(id);
        Iterator<LogEntry> entries = nodes.get(id).retrieveCommitted(index, Long.MAX_VALUE).join();
        if (entries.hasNext()) {
            return Optional.of(entries.next());
        }
        return Optional.empty();
    }

    public long commitIndex(String id) {
        assert nodes.containsKey(id);
        return commitLogs.get(id).get(commitLogs.get(id).size() - 1);
    }

    public void drop(String from, String to, float percent) {
        network.drop(from, to, percent);
    }

    public void isolate(String id) {
        network.isolate(id);
    }

    public void integrate(String id) {
        network.integrate(id);
    }

    public void cut(String from, String to) {
        network.cut(from, to);
    }

    public void ignore(String from, String to, RaftMessage.MessageTypeCase messageTypeCase) {
        network.ignore(from, to, messageTypeCase);
    }

    public void duplicate(String from, String to, float percent) {
        network.duplicate(from, to, percent);
    }

    public void reorder(String from, String to, float percent) {
        network.reorder(from, to, percent);
    }

    public void delay(String from, String to, int maxDelayTicks) {
        network.delay(from, to, maxDelayTicks);
    }

    public void recoverNetwork() {
        network.recover();
    }

    /**
     * Start the raft group by inputting clock source
     *
     * @param tickUnit
     * @param timeUnit
     */
    public void run(long tickUnit, TimeUnit timeUnit) {
        if (ticking.compareAndSet(false, true)) {
            this.tickInterval = tickUnit;
            this.tickUnit = timeUnit;
            tickExecutor.schedule(() -> this.tick(tickUnit, timeUnit), tickUnit, timeUnit);
        }
    }

    /**
     * Pause the raft group
     */
    public void pause() {
        ticking.set(false);
    }

    /**
     * Resume ticking
     */
    public void resume() {
        run(tickInterval, tickUnit);
    }

    public void shutdown() {
        log.info("Shutting down raft node group");
        for (String id : new HashSet<>(nodes.keySet())) {
            removeRaftNode(id);
        }
        try {
            log.info("Stopping raft node group ticker");
            tickExecutor.shutdown();
            tickExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Shutdown with exception", e);
        }
        network.shutdown();
    }

    private void tick(long tickUnit, TimeUnit timeUnit) {
        boolean tickNode = (ticks + 1) / RaftNodeTickMagnitude > ticks / RaftNodeTickMagnitude;
        ticks++;
        if (tickNode) {
            nodes.values().forEach(node -> {
                if (node.isStarted()) {
                    node.tick();
                }
            });
        }
        List<CountDownLatch> cleared = new ArrayList<>();
        for (CountDownLatch latch : ticksWaiters) {
            latch.countDown();
            if (latch.getCount() == 0) {
                cleared.add(latch);
            }
        }
        ticksWaiters.removeAll(cleared);
        network.tick();
        if (ticking.get()) {
            tickExecutor.schedule(() -> this.tick(tickUnit, timeUnit), tickUnit, timeUnit);
        }
    }
}
