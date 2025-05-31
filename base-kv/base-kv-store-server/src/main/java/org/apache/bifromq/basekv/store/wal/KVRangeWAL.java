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

package org.apache.bifromq.basekv.store.wal;

import static java.util.Collections.emptyMap;

import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.baseenv.ZeroCopyParser;
import org.apache.bifromq.basekv.proto.KVRangeCommand;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.raft.IRaftNode;
import org.apache.bifromq.basekv.raft.RaftConfig;
import org.apache.bifromq.basekv.raft.RaftNode;
import org.apache.bifromq.basekv.raft.event.CommitEvent;
import org.apache.bifromq.basekv.raft.event.ElectionEvent;
import org.apache.bifromq.basekv.raft.event.RaftEvent;
import org.apache.bifromq.basekv.raft.event.SnapshotRestoredEvent;
import org.apache.bifromq.basekv.raft.event.StatusChangedEvent;
import org.apache.bifromq.basekv.raft.event.SyncStateChangedEvent;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.raft.proto.RaftMessage;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.raft.proto.RaftNodeSyncState;
import org.apache.bifromq.basekv.store.exception.KVRangeException;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.logger.SiftLogger;
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
import org.slf4j.Logger;

public class KVRangeWAL implements IKVRangeWAL, IRaftNode.ISnapshotInstaller {
    private final Logger log;
    private final long maxFetchBytes;
    private final PublishSubject<SnapshotRestoredEvent> snapRestoreEventPublisher = PublishSubject.create();
    private final BehaviorSubject<Long> commitIndexSubject = BehaviorSubject.create();
    private final PublishSubject<RestoreSnapshotTask> snapRestoreTaskPublisher = PublishSubject.create();
    private final BehaviorSubject<ElectionEvent> electionPublisher = BehaviorSubject.create();
    private final BehaviorSubject<RaftNodeStatus> statusPublisher = BehaviorSubject.create();
    private final PublishSubject<Map<String, List<RaftMessage>>> raftMessagesPublisher = PublishSubject.create();
    private final BehaviorSubject<Map<String, RaftNodeSyncState>> syncStatePublisher =
        BehaviorSubject.createDefault(emptyMap());
    private final KVRangeId rangeId;
    private final String localId;
    private final IKVRangeWALStore walStore;
    private final IRaftNode raftNode;
    private final AtomicLong ticks = new AtomicLong(0);
    private final String[] tags;

    public KVRangeWAL(String clusterId,
                      String localId,
                      KVRangeId rangeId,
                      IKVRangeWALStore walStore,
                      RaftConfig raftConfig,
                      int maxFetchBytes) {
        this.rangeId = rangeId;
        this.localId = localId;
        this.maxFetchBytes = maxFetchBytes;
        this.walStore = walStore;
        tags =
            new String[] {"clusterId", clusterId, "storeId", localId, "rangeId", KVRangeIdUtil.toString(rangeId)};
        log = SiftLogger.getLogger(KVRangeWAL.class, tags);
        raftNode = new RaftNode(raftConfig, walStore,
            EnvProvider.INSTANCE.newThreadFactory("wal-raft-executor-" + KVRangeIdUtil.toString(rangeId)), tags);
    }

    @Override
    public String storeId() {
        return localId;
    }

    @Override
    public KVRangeId rangeId() {
        return rangeId;
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
    public Observable<Map<String, RaftNodeSyncState>> replicationStatus() {
        return syncStatePublisher.distinctUntilChanged();
    }

    @Override
    public IKVRangeWALSubscription subscribe(long lastFetchedIndex, IKVRangeWALSubscriber subscriber,
                                             Executor executor) {
        return new KVRangeWALSubscription(maxFetchBytes, this,
            commitIndexSubject, lastFetchedIndex, subscriber, executor, tags);
    }

    @Override
    public CompletableFuture<LogEntry> once(long lastFetchedIndex, Predicate<LogEntry> condition, Executor executor) {
        CompletableFuture<LogEntry> onDone = new CompletableFuture<>();
        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxFetchBytes, this, commitIndexSubject, lastFetchedIndex,
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
                    public CompletableFuture<Void> restore(KVRangeSnapshot requested, String leader,
                                                           IAfterRestoredCallback callback) {
                        callback.call(null, new KVRangeException("Canceled once"));
                        return CompletableFuture.failedFuture(new KVRangeException("Canceled once"));
                    }
                }, executor, tags);
        onDone.whenCompleteAsync((v, e) -> walSub.stop(), executor);
        return onDone;
    }

    @Override
    public CompletableFuture<Long> propose(KVRangeCommand command) {
        return raftNode.propose(command.toByteString());
    }

    @Override
    public Observable<Long> commitIndex() {
        return commitIndexSubject;
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
        return ZeroCopyParser.parse(raftNode.latestSnapshot(), KVRangeSnapshot.parser());
    }

    @Override
    public ClusterConfig latestClusterConfig() {
        return raftNode.latestClusterConfig();
    }

    @Override
    public CompletableFuture<Void> compact(KVRangeSnapshot snapshot) {
        return raftNode.compact(snapshot.toByteString(), snapshot.getLastAppliedIndex());
    }

    @Override
    public Observable<RestoreSnapshotTask> snapshotRestoreTask() {
        return snapRestoreTaskPublisher;
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
    public long logDataSize() {
        return walStore.size();
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
        log.debug("Starting KVRangeWAL");
        raftNode.start(this::sendRaftMessages, this::onRaftEvent, this);
        statusPublisher.onNext(raftNode.status());
    }

    @Override
    public CompletableFuture<Void> close() {
        log.debug("Closing KVRangeWAL");
        raftMessagesPublisher.onComplete();
        commitIndexSubject.onComplete();
        snapRestoreTaskPublisher.onComplete();
        electionPublisher.onComplete();
        syncStatePublisher.onComplete();
        return raftNode.stop().whenComplete((v, e) -> log.debug("KVRangeWAL closed"));
    }

    @Override
    public CompletableFuture<Void> destroy() {
        log.debug("Destroying KVRangeWAL store");
        return close().thenAccept(v -> {
            walStore.destroy();
            log.debug("KVRangeWAL store destroyed");
        });
    }

    @Override
    public void install(ByteString requested, String leader, IRaftNode.IAfterInstalledCallback callback) {
        RestoreSnapshotTask task = new RestoreSnapshotTask(requested, leader, callback);
        snapRestoreTaskPublisher.onNext(task);
    }

    void onRaftEvent(RaftEvent event) {
        switch (event.type) {
            case COMMIT -> commitIndexSubject.onNext(((CommitEvent) event).index);
            case ELECTION -> electionPublisher.onNext((ElectionEvent) event);
            case STATUS_CHANGED -> statusPublisher.onNext(((StatusChangedEvent) event).status);
            case SNAPSHOT_RESTORED -> snapRestoreEventPublisher.onNext((SnapshotRestoredEvent) event);
            case SYNC_STATE_CHANGED -> syncStatePublisher.onNext(((SyncStateChangedEvent) event).states);
        }
    }

    @VisibleForTesting
    void sendRaftMessages(Map<String, List<RaftMessage>> peerMessages) {
        raftMessagesPublisher.onNext(peerMessages);
    }
}
