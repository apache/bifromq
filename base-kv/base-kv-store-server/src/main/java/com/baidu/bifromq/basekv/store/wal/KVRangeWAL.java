/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

import static com.baidu.bifromq.basekv.utils.KVRangeIdUtil.toShortString;
import static java.util.Collections.EMPTY_MAP;
import static org.slf4j.LoggerFactory.getLogger;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.proto.KVRangeCommand;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.raft.IRaftNode;
import com.baidu.bifromq.basekv.raft.RaftConfig;
import com.baidu.bifromq.basekv.raft.RaftNode;
import com.baidu.bifromq.basekv.raft.event.CommitEvent;
import com.baidu.bifromq.basekv.raft.event.ElectionEvent;
import com.baidu.bifromq.basekv.raft.event.RaftEvent;
import com.baidu.bifromq.basekv.raft.event.SnapshotRestoredEvent;
import com.baidu.bifromq.basekv.raft.event.StatusChangedEvent;
import com.baidu.bifromq.basekv.raft.event.SyncStateChangedEvent;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.LogEntry;
import com.baidu.bifromq.basekv.raft.proto.RaftMessage;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeSyncState;
import com.baidu.bifromq.basekv.store.exception.KVRangeException;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KVRangeWAL implements IKVRangeWAL {
    private final long maxFetchBytes;
    private final PublishSubject<SnapshotRestoredEvent> snapRestoreEventPublisher = PublishSubject.create();
    private final BehaviorSubject<Long> commitIndexSubject = BehaviorSubject.create();
    private final PublishSubject<SnapshotInstallTask> snapInstallTaskPublisher = PublishSubject.create();
    private final BehaviorSubject<ElectionEvent> electionPublisher = BehaviorSubject.create();
    private final BehaviorSubject<RaftNodeStatus> statusPublisher = BehaviorSubject.create();
    private final PublishSubject<Map<String, List<RaftMessage>>> raftMessagesPublisher = PublishSubject.create();
    private final BehaviorSubject<Map<String, RaftNodeSyncState>> syncStatePublisher =
        BehaviorSubject.createDefault(EMPTY_MAP);
    private final String clusterId;
    private final KVRangeId rangeId;
    private final String localId;
    private final IKVRangeWALStoreEngine stateStoreEngine;
    private final IRaftNode raftNode;
    private final AtomicLong ticks = new AtomicLong(0);

    public KVRangeWAL(String clusterId,
                      KVRangeId rangeId,
                      IKVRangeWALStoreEngine stateStoreEngine,
                      RaftConfig raftConfig,
                      int maxFetchBytes) {
        this.clusterId = clusterId;
        this.rangeId = rangeId;
        this.stateStoreEngine = stateStoreEngine;
        this.localId = stateStoreEngine.id();
        this.maxFetchBytes = maxFetchBytes;
        raftNode = new RaftNode(raftConfig, stateStoreEngine.get(rangeId),
            getLogger("raft.logger"),
            EnvProvider.INSTANCE.newThreadFactory("wal-raft-executor-" + KVRangeIdUtil.toShortString(rangeId)),
            "cluster", clusterId, "rangeId", KVRangeIdUtil.toString(rangeId));
    }

    @Override
    public String id() {
        return localId;
    }

    @Override
    public boolean isLeader() {
        return currentState() == RaftNodeStatus.Leader;
    }

    @Override
    public RaftNodeStatus currentState() {
        return statusPublisher.getValue();
    }

    @Override
    public Observable<RaftNodeStatus> state() {
        return statusPublisher;
    }

    @Override
    public Optional<String> currentLeader() {
        ElectionEvent latestElection = electionPublisher.getValue();
        if (latestElection == null) {
            return Optional.empty();
        }
        return Optional.of(latestElection.leaderId);
    }

    @Override
    public Observable<ElectionEvent> election() {
        return electionPublisher.distinctUntilChanged();
    }

    @Override
    public ClusterConfig clusterConfig() {
        return raftNode.latestClusterConfig();
    }

    @Override
    public Observable<Map<String, RaftNodeSyncState>> replicationStatus() {
        return syncStatePublisher.distinctUntilChanged();
    }

    @Override
    public IKVRangeWALSubscription subscribe(long startIndex, IKVRangeWALSubscriber subscriber, Executor executor) {
        return new KVRangeWALSubscription(maxFetchBytes, this, commitIndexSubject, startIndex, subscriber, executor);
    }

    @Override
    public CompletableFuture<LogEntry> once(long startIndex, Predicate<LogEntry> condition, Executor executor) {
        CompletableFuture<LogEntry> onDone = new CompletableFuture<>();
        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxFetchBytes, this, commitIndexSubject, startIndex,
                new IKVRangeWALSubscriber() {
                    @Override
                    public CompletableFuture<Void> apply(LogEntry log) {
                        try {
                            if (condition.test(log)) {
                                onDone.complete(log);
                            }
                        } catch (Throwable e) {
                            onDone.completeExceptionally(e);
                        }
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public CompletableFuture<Void> apply(KVRangeSnapshot snapshot) {
                        return CompletableFuture.failedFuture(new KVRangeException("Canceled once"));
                    }
                }, executor);
        onDone.whenComplete((v, e) -> walSub.stop());
        return onDone;
    }

    @Override
    public CompletableFuture<Void> propose(KVRangeCommand command) {
        log.trace("Propose KVRange Command[command={}]", command);
        return raftNode.propose(command.toByteString());
    }

    @Override
    public Observable<Long> commitIndex() {
        return commitIndexSubject;
    }

    @Override
    public Observable<SnapshotRestoredEvent> snapshotRestoreEvent() {
        return snapRestoreEventPublisher;
    }

    @Override
    public CompletableFuture<Iterator<LogEntry>> retrieveCommitted(long fromIndex, long maxSize) {
        return raftNode.retrieveCommitted(fromIndex, maxSize);
    }

    @Override
    public CompletableFuture<Long> readIndex() {
        return raftNode.readIndex();
    }

    @Override
    public CompletableFuture<Void> transferLeadership(String peerId) {
        return raftNode.transferLeadership(peerId);
    }

    @Override
    public boolean stepDown() {
        return raftNode.stepDown();
    }

    @Override
    public CompletableFuture<Void> changeClusterConfig(String correlateId, Set<String> voters, Set<String> learners) {
        return raftNode.changeClusterConfig(correlateId, voters, learners);
    }

    @SneakyThrows
    @Override
    public KVRangeSnapshot latestSnapshot() {
        return KVRangeSnapshot.parseFrom(raftNode.latestSnapshot());
    }

    @Override
    public CompletableFuture<Void> compact(KVRangeSnapshot snapshot) {
        return raftNode.compact(snapshot.toByteString(), snapshot.getLastAppliedIndex());
    }

    @Override
    public Observable<SnapshotInstallTask> snapshotInstallTask() {
        return snapInstallTaskPublisher;
    }

    @Override
    public Observable<Map<String, List<RaftMessage>>> peerMessages() {
        return raftMessagesPublisher;
    }

    @Override
    public CompletableFuture<Void> recover() {
        return raftNode.recover();
    }

    @Override
    public void destroy() {
        close();
        stateStoreEngine.destroy(rangeId);
    }

    @Override
    public long logDataSize() {
        return stateStoreEngine.storageSize(rangeId);
    }

    @Override
    public void receivePeerMessages(String fromPeer, List<RaftMessage> messages) {
        messages.forEach(m -> raftNode.receive(fromPeer, m));
    }

    @Override
    public void tick() {
        ticks.incrementAndGet();
        raftNode.tick();
    }

    @Override
    public void start() {
        raftNode.start(this::sendRaftMessages, this::onRaftEvent, this::installSnapshot);
        statusPublisher.onNext(raftNode.status());
    }

    @Override
    public CompletableFuture<Void> close() {
        log.debug("Closing WAL: rangeId={}, storeId={}", toShortString(rangeId), localId);
        raftMessagesPublisher.onComplete();
        commitIndexSubject.onComplete();
        snapInstallTaskPublisher.onComplete();
        electionPublisher.onComplete();
        syncStatePublisher.onComplete();
        return raftNode.stop();
    }

    void onRaftEvent(RaftEvent event) {
        switch (event.type) {
            case COMMIT:
                commitIndexSubject.onNext(((CommitEvent) event).index);
                break;
            case ELECTION:
                electionPublisher.onNext((ElectionEvent) event);
                break;
            case STATUS_CHANGED:
                statusPublisher.onNext(((StatusChangedEvent) event).status);
                break;
            case SNAPSHOT_RESTORED:
                snapRestoreEventPublisher.onNext((SnapshotRestoredEvent) event);
                break;
            case SYNC_STATE_CHANGED:
                syncStatePublisher.onNext(((SyncStateChangedEvent) event).states);
                break;
        }
    }

    @VisibleForTesting
    void sendRaftMessages(Map<String, List<RaftMessage>> peerMessages) {
        raftMessagesPublisher.onNext(peerMessages);
    }

    @VisibleForTesting
    CompletableFuture<Void> installSnapshot(ByteString snapshot) {
        SnapshotInstallTask task = new SnapshotInstallTask(snapshot);
        snapInstallTaskPublisher.onNext(task);
        return task.onDone;
    }
}
