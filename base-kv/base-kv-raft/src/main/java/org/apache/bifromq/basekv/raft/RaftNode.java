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

package org.apache.bifromq.basekv.raft;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.NonNull;
import org.apache.bifromq.basekv.raft.event.RaftEvent;
import org.apache.bifromq.basekv.raft.event.RaftEventType;
import org.apache.bifromq.basekv.raft.exception.InternalError;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.raft.proto.RaftMessage;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

public final class RaftNode implements IRaftNode {
    private final String id;
    private final IRaftStateStore stateStorage;
    private final RaftConfig config;
    private final Logger log;
    private final ExecutorService raftExecutor;
    private final AtomicReference<RaftNodeState> stateRef = new AtomicReference<>();
    private final AtomicReference<Status> status = new AtomicReference<>(Status.INIT);
    private final AtomicReference<CompletableFuture<Void>> stopFuture = new AtomicReference<>();
    private final String[] tags;
    private MetricManager metricMgr;

    public RaftNode(RaftConfig config,
                    IRaftStateStore stateStore,
                    ThreadFactory threadFactory,
                    String... tags) {
        verifyTags(tags);
        verifyConfig(config);
        verifyStateStore(stateStore);
        log = MDCLogger.getLogger(RaftNode.class, tags);
        this.tags = tags;
        this.stateStorage = new MetricMonitoredStateStore(stateStore, Tags.of(tags));
        this.id = stateStorage.local();
        this.config = config.toBuilder().build();
        this.raftExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedTransferQueue<>(), threadFactory), id, "raft", Tags.of(tags));
        stateStore.addStableListener(this::onStabilized);
    }

    @Override
    public boolean isStarted() {
        return status.get() == Status.STARTED;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public RaftNodeStatus status() {
        checkStarted();
        return stateRef.get().getState();
    }

    @Override
    public void tick() {
        submit(() -> {
            RaftNodeState state = stateRef.get();
            Timer.Sample sample = Timer.start();
            String leader = state.currentLeader();
            RaftNodeState nextState = state.tick();
            sample.stop(metricMgr.tickTimer);
            if (nextState != state) {
                stateRef.set(nextState);
                // leader elected or candidate transit to follower
                nextState.notifyStateChanged();
            }
            if (nextState.currentLeader() != null && !nextState.currentLeader().equals(leader)) {
                nextState.notifyLeaderElected(nextState.currentLeader(), nextState.currentTerm());
            }
        });
    }

    @Override
    public CompletableFuture<Long> propose(ByteString appCommand) {
        return submit(onDone -> stateRef.get().propose(appCommand, sampleLatency(onDone, metricMgr.proposeTimer)));
    }

    private void onStabilized(long stableIndex) {
        submit(() -> {
            Timer.Sample sample = Timer.start();
            RaftNodeState state = stateRef.get();
            String leader = state.currentLeader();
            RaftNodeState nextState = state.stableTo(stableIndex);
            sample.stop(metricMgr.stableToTimer);
            if (nextState != state) {
                stateRef.set(nextState);
                // leader elected or candidate transit to follower when acknowledging there is a leader
                nextState.notifyStateChanged();
            }
            if (nextState.currentLeader() != null && !nextState.currentLeader().equals(leader)) {
                nextState.notifyLeaderElected(nextState.currentLeader(), nextState.currentTerm());
            }
        });
    }

    @Override
    public CompletableFuture<Long> readIndex() {
        return submit(onDone -> stateRef.get().readIndex(sampleLatency(onDone, metricMgr.readIndexTimer)));
    }

    @Override
    public void receive(String fromPeer, RaftMessage message) {
        submit(() -> {
            RaftNodeState state = stateRef.get();
            String leader = state.currentLeader();
            Timer.Sample sample = Timer.start();
            RaftNodeState nextState = state.receive(fromPeer, message);
            sample.stop(metricMgr.peerMsgHandlingTimer);
            if (nextState != state) {
                stateRef.set(nextState);
                // leader elected or candidate transit to follower when acknowledging there is a leader
                nextState.notifyStateChanged();
            }
            if (nextState.currentLeader() != null && !nextState.currentLeader().equals(leader)) {
                nextState.notifyLeaderElected(nextState.currentLeader(), nextState.currentTerm());
            }
        });
    }

    @Override
    public CompletableFuture<Void> compact(ByteString fsmSnapshot, long compactIndex) {
        return submit(onDone -> stateRef.get()
            .compact(fsmSnapshot, compactIndex, sampleLatency(onDone, metricMgr.compactTimer)));
    }

    @Override
    public CompletableFuture<Void> transferLeadership(String newLeader) {
        return submit(onDone -> stateRef.get()
            .transferLeadership(newLeader, sampleLatency(onDone, metricMgr.transferLeadershipTimer)));
    }

    @Override
    public CompletableFuture<Void> recover() {
        return submit(onDone -> stateRef.get().recover(onDone));
    }

    @Override
    public ClusterConfig latestClusterConfig() {
        return unwrap(submit(onDone -> onDone.complete(stateRef.get().latestClusterConfig())));
    }

    @Override
    public Boolean stepDown() {
        return unwrap(submit(onDone -> {
            RaftNodeState state = stateRef.get();
            RaftNodeState nextState = state.stepDown();
            if (nextState != state) {
                stateRef.set(nextState);
                onDone.complete(true);
            } else {
                onDone.complete(false);
            }
        }));
    }

    @Override
    public ByteString latestSnapshot() {
        return unwrap(submit(onDone -> onDone.complete(stateRef.get().latestSnapshot())));
    }

    @Override
    public CompletableFuture<Void> changeClusterConfig(String correlateId,
                                                       Set<String> nextVoters,
                                                       Set<String> nextLearners) {
        return submit(onDone -> stateRef.get().changeClusterConfig(correlateId, nextVoters, nextLearners,
            sampleLatency(onDone, metricMgr.changeClusterConfigTimer)));
    }

    @Override
    public CompletableFuture<Iterator<LogEntry>> retrieveCommitted(long fromIndex, long maxSize) {
        return submit(onDone -> stateRef.get().retrieveCommitted(fromIndex, maxSize,
            sampleLatency(onDone, metricMgr.retrieveEntriesTimer)));
    }

    @Override
    public void start(@NonNull IRaftMessageSender sender,
                      @NonNull IRaftEventListener listener,
                      @NonNull IRaftNode.ISnapshotInstaller installer) {
        if (status.compareAndSet(Status.INIT, Status.STARTING)) {
            long currentTerm = stateStorage.currentTerm();
            stateRef.set(new RaftNodeStateFollower(
                currentTerm,
                0, // commitIndex
                null,
                config,
                stateStorage,
                new SampledRaftMessageListener(sender),
                new SampledRaftEventListener(listener),
                new SampledSnapshotInstaller(installer),
                this::onSnapshotRestored,
                tags
            ));
            metricMgr = new MetricManager(Tags.of(tags));
            status.set(Status.STARTED);
            log.debug("Raft node[{}] started: term={}", id(), currentTerm);
        }
    }

    @Override
    public CompletableFuture<Void> stop() {
        return switch (status.get()) {
            // fallthrough
            case INIT, STARTING -> CompletableFuture.failedFuture(new IllegalStateException("Raft node not started"));
            // fallthrough
            default -> {
                if (stopFuture.compareAndSet(null, new CompletableFuture<>())) {
                    log.debug("Stopping raft node[{}]", id());
                    CompletableFuture<Void> lastTask = new CompletableFuture<>();
                    lastTask.whenComplete((v, e) -> {
                        raftExecutor.shutdown();
                        stopFuture.get().complete(null);
                    });
                    Runnable stop = () -> {
                        assert status.get() == Status.STARTED;
                        status.set(Status.STOPPING);
                        stateRef.get().stop();
                        stateStorage.stop();
                        metricMgr.close();
                        status.set(Status.STOPPED);
                        log.debug("Raft node[{}] stopped", id());
                        lastTask.complete(null);
                    };
                    raftExecutor.execute(stop);
                }
                yield stopFuture.get();
            }
        };
    }

    CompletableFuture<Void> onSnapshotRestored(ByteString requested, ByteString installed, Throwable ex) {
        return submit(onDone -> stateRef.get().onSnapshotRestored(requested, installed, ex, onDone));
    }

    private void submit(Runnable task) {
        submit(onDone -> {
            task.run();
            onDone.complete(null);
        });
    }

    private <T> CompletableFuture<T> submit(Consumer<CompletableFuture<T>> task) {
        CompletableFuture<T> doneFuture = new CompletableFuture<>();
        try {
            raftExecutor.execute(() -> {
                switch (status.get()) {
                    // fallthrough
                    case INIT, STARTING ->
                        doneFuture.completeExceptionally(new IllegalStateException("Raft node not started"));
                    case STARTED -> {
                        try {
                            task.accept(doneFuture);
                        } catch (Throwable e) {
                            doneFuture.completeExceptionally(new InternalError(e));
                        }
                    }
                    // fallthrough
                    case STOPPING, STOPPED ->
                        doneFuture.completeExceptionally(new IllegalStateException("Raft node has stopped"));
                }
            });
        } catch (RejectedExecutionException e) {
            doneFuture.completeExceptionally(new IllegalStateException(e));
        }
        return doneFuture;
    }

    private void verifyConfig(RaftConfig config) {
        if (config.getHeartbeatTimeoutTick() > config.getElectionTimeoutTick()) {
            throw new IllegalArgumentException("heartbeat timeout must be less than election timeout, normally 1/10");
        }
    }

    private void verifyStateStore(IRaftStateStore stateStorage) {
        if (stateStorage.local() == null) {
            throw new IllegalArgumentException("local id cannot be null");
        }
        if (stateStorage.lastIndex() < 0) {
            throw new IllegalArgumentException("last index must be non-negative");
        }
        if (stateStorage.firstIndex() <= 0) {
            throw new IllegalArgumentException("first index must be positive");
        }
        if (stateStorage.latestClusterConfig() == null) {
            throw new IllegalArgumentException("latest cluster config cannot be null");
        }
        if (stateStorage.latestSnapshot() == null) {
            throw new IllegalArgumentException("latest snapshot cannot be null");
        }
        ClusterConfig clusterConfig = stateStorage.latestSnapshot().getClusterConfig();
        if (clusterConfig.getVotersList().stream().anyMatch(clusterConfig.getLearnersList()::contains)) {
            throw new IllegalArgumentException("voters and learners mustn't intersect with each other");
        }
    }

    private void verifyTags(String[] tags) {
        if (tags.length % 2 != 0) {
            throw new IllegalArgumentException("Tags must be even number representing key/value pairs");
        }
    }

    private <T> T unwrap(CompletableFuture<T> future) {
        try {
            return future.get();
        } catch (Throwable e) {
            throw new IllegalStateException("Future cannot be unwrapped", e);
        }
    }

    private <T> CompletableFuture<T> sampleLatency(CompletableFuture<T> future, Timer timer) {
        Timer.Sample sample = Timer.start();
        future.whenComplete((v, e) -> sample.stop(timer));
        return future;
    }

    private void checkStarted() {
        if (status.get() != Status.STARTED) {
            throw new IllegalStateException("Raft node not started");
        }
    }

    enum Status {
        INIT,
        STARTING,
        STARTED,
        STOPPING,
        STOPPED
    }

    private class MetricManager {
        final Timer tickTimer;
        final Timer proposeTimer;
        final Timer readIndexTimer;
        final Timer peerMsgHandlingTimer;
        final Timer compactTimer;
        final Timer transferLeadershipTimer;
        final Timer changeClusterConfigTimer;
        final Timer retrieveEntriesTimer;
        final Timer entryAtTimer;
        final Timer stableToTimer;
        final Timer messageSenderTimer;
        final Timer snapshotInstallTimer;
        final EnumMap<RaftEventType, Timer> eventTimers;
        final Gauge pendingProposalsGauge;
        final Gauge commitIndexGauge;
        final Gauge firstIndexGauge;
        final Gauge lastIndexGauge;
        final Gauge currentTermGauge;
        final Gauge currentStatusGauge;
        final Gauge currentRoleGauge;
        final Gauge currentVoters;
        final Gauge currentLearners;
        final Gauge currentNextVoters;
        final Gauge currentNextLearners;

        MetricManager(Tags tags) {
            // timer for tick operation latency
            tickTimer = Metrics.timer("raft.cmd.tick", tags);

            proposeTimer = Metrics.timer("raft.cmd.propose", tags);

            readIndexTimer = Metrics.timer("raft.cmd.readindex", tags);

            // timer for handling peer message operation
            peerMsgHandlingTimer = Metrics.timer("raft.cmd.recvmsg", tags);

            // timer for compact operation latency
            compactTimer = Metrics.timer("raft.cmd.compact", tags);

            // timer for transfer leadership operation latency
            transferLeadershipTimer = Metrics.timer("raft.cmd.trnsldr", tags);

            // timer for change cluster config operation latency
            changeClusterConfigTimer = Metrics.timer("raft.cmd.chgcfg", tags);

            // timer for retrieve entries
            retrieveEntriesTimer = Metrics.timer("raft.cmd.getentries", tags);

            // timer for get single entry
            entryAtTimer = Metrics.timer("raft.cmd.getentry", tags);

            // timer for stableTo operation
            stableToTimer = Metrics.timer("raft.cmd.markstable", tags);

            messageSenderTimer = Metrics.timer("raft.message.send", tags);
            snapshotInstallTimer = Metrics.timer("raft.snapshot.install", tags);
            eventTimers = new EnumMap<RaftEventType, Timer>(new HashMap<>() {{
                put(RaftEventType.ELECTION, Metrics.timer("raft.event.listener", tags.and("type", "election")));
                put(RaftEventType.COMMIT, Metrics.timer("raft.event.listener", tags.and("type", "commit")));
                put(RaftEventType.STATUS_CHANGED,
                    Metrics.timer("raft.event.listener", tags.and("type", "status_changed")));
                put(RaftEventType.SNAPSHOT_RESTORED,
                    Metrics.timer("raft.event.listener", tags.and("type", "snapshot_restored")));
                put(RaftEventType.SYNC_STATE_CHANGED,
                    Metrics.timer("raft.event.listener", tags.and("type", "sync_state_changed")));
            }});

            // gauge for pending proposal count
            pendingProposalsGauge = Gauge.builder("raft.cmd.propose.pending", RaftNode.this,
                    r -> r.stateRef.get().uncommittedProposals.size())
                .tags(tags)
                .register(Metrics.globalRegistry);

            // gauge for commit index
            commitIndexGauge = Gauge.builder("raft.log.commitindex", RaftNode.this,
                    r -> r.stateRef.get().commitIndex)
                .tags(tags)
                .register(Metrics.globalRegistry);
            // gauge for first index
            firstIndexGauge = Gauge.builder("raft.log.firstindex", RaftNode.this,
                    r -> r.stateStorage.firstIndex())
                .tags(tags)
                .register(Metrics.globalRegistry);
            // gauge for last index
            lastIndexGauge = Gauge.builder("raft.log.lastindex",
                    RaftNode.this, r -> r.stateStorage.lastIndex())
                .tags(tags)
                .register(Metrics.globalRegistry);
            // gauge for current term
            currentTermGauge = Gauge.builder("raft.log.term", RaftNode.this,
                    r -> r.stateRef.get().currentTerm())
                .tags(tags)
                .register(Metrics.globalRegistry);
            // gauge for current status
            currentStatusGauge =
                Gauge.builder("raft.status", RaftNode.this, r -> r.stateRef.get().getState().getNumber())
                    .tags(tags)
                    .register(Metrics.globalRegistry);
            // gauge for role in current cluster config
            // 0 for voter, 1 for learner, 2 for next voter, 3 for next learner, 4, for not a member
            currentRoleGauge = Gauge.builder("raft.role", RaftNode.this, r -> {
                    ClusterConfig clusterConfig = r.stateRef.get().latestClusterConfig();
                    if (clusterConfig.getVotersList().contains(id)) {
                        return 0;
                    }
                    if (clusterConfig.getLearnersList().contains(id)) {
                        return 1;
                    }
                    if (clusterConfig.getNextVotersList().contains(id)) {
                        return 2;
                    }
                    if (clusterConfig.getNextLearnersList().contains(id)) {
                        return 3;
                    }
                    return 4;
                })
                .tags(tags)
                .register(Metrics.globalRegistry);
            // gauge for voter number in current cluster config
            currentVoters = Gauge.builder("raft.voters.current",
                    RaftNode.this, r -> r.stateRef.get().latestClusterConfig().getVotersCount())
                .tags(tags)
                .register(Metrics.globalRegistry);
            // gauge for voter number in current cluster config
            currentLearners = Gauge.builder("raft.learners.current",
                    RaftNode.this, r -> r.stateRef.get().latestClusterConfig().getLearnersCount())
                .tags(tags)
                .register(Metrics.globalRegistry);
            // gauge for next voter number in current cluster config
            currentNextVoters = Gauge.builder("raft.voters.next",
                    RaftNode.this, r -> r.stateRef.get().latestClusterConfig().getNextVotersCount())
                .tags(tags)
                .register(Metrics.globalRegistry);
            // gauge for next voter number in current cluster config
            currentNextLearners = Gauge.builder("raft.learners.next",
                    RaftNode.this, r -> r.stateRef.get().latestClusterConfig().getNextLearnersCount())
                .tags(tags)
                .register(Metrics.globalRegistry);
        }

        void close() {
            Metrics.globalRegistry.removeByPreFilterId(tickTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(proposeTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(readIndexTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(peerMsgHandlingTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(compactTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(transferLeadershipTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(changeClusterConfigTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(retrieveEntriesTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(entryAtTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(stableToTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(messageSenderTimer.getId());
            Metrics.globalRegistry.removeByPreFilterId(snapshotInstallTimer.getId());

            eventTimers.forEach((e, t) -> Metrics.globalRegistry.removeByPreFilterId(t.getId()));

            Metrics.globalRegistry.removeByPreFilterId(pendingProposalsGauge.getId());

            Metrics.globalRegistry.removeByPreFilterId(commitIndexGauge.getId());

            Metrics.globalRegistry.removeByPreFilterId(firstIndexGauge.getId());

            Metrics.globalRegistry.removeByPreFilterId(lastIndexGauge.getId());

            Metrics.globalRegistry.removeByPreFilterId(currentTermGauge.getId());

            Metrics.globalRegistry.removeByPreFilterId(currentStatusGauge.getId());

            Metrics.globalRegistry.removeByPreFilterId(currentRoleGauge.getId());

            Metrics.globalRegistry.removeByPreFilterId(currentVoters.getId());

            Metrics.globalRegistry.removeByPreFilterId(currentLearners.getId());

            Metrics.globalRegistry.removeByPreFilterId(currentNextVoters.getId());

            Metrics.globalRegistry.removeByPreFilterId(currentNextLearners.getId());
        }
    }

    private class SampledRaftMessageListener implements IRaftMessageSender {
        final IRaftMessageSender delegate;

        SampledRaftMessageListener(IRaftMessageSender delegate) {
            this.delegate = delegate;
        }

        @Override
        public void send(Map<String, List<RaftMessage>> messages) {
            Timer.Sample sample = Timer.start();
            delegate.send(messages);
            sample.stop(metricMgr.messageSenderTimer);
        }
    }

    private class SampledRaftEventListener implements IRaftEventListener {
        final IRaftEventListener delegate;

        private SampledRaftEventListener(IRaftEventListener delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onEvent(RaftEvent event) {
            Timer.Sample sample = Timer.start();
            delegate.onEvent(event);
            sample.stop(metricMgr.eventTimers.get(event.type));
        }
    }

    private class SampledSnapshotInstaller implements ISnapshotInstaller {
        final ISnapshotInstaller delegate;

        private SampledSnapshotInstaller(ISnapshotInstaller delegate) {
            this.delegate = delegate;
        }

        @Override
        public void install(ByteString request, String leader, IAfterInstalledCallback callback) {
            Timer.Sample start = Timer.start();
            delegate.install(request, leader, (snapshot, ex) -> {
                start.stop(metricMgr.snapshotInstallTimer);
                return callback.call(snapshot, ex);
            });
        }
    }
}
