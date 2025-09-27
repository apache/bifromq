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

import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.union;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;
import static org.apache.bifromq.basekv.proto.State.StateType.ConfigChanging;
import static org.apache.bifromq.basekv.proto.State.StateType.Merged;
import static org.apache.bifromq.basekv.proto.State.StateType.MergedQuiting;
import static org.apache.bifromq.basekv.proto.State.StateType.NoUse;
import static org.apache.bifromq.basekv.proto.State.StateType.Normal;
import static org.apache.bifromq.basekv.proto.State.StateType.PreparedMerging;
import static org.apache.bifromq.basekv.proto.State.StateType.Removed;
import static org.apache.bifromq.basekv.proto.State.StateType.ToBePurged;
import static org.apache.bifromq.basekv.proto.State.StateType.WaitingForMerge;
import static org.apache.bifromq.basekv.store.range.KVRangeFSM.Lifecycle.Closed;
import static org.apache.bifromq.basekv.store.range.KVRangeFSM.Lifecycle.Destroyed;
import static org.apache.bifromq.basekv.store.range.KVRangeFSM.Lifecycle.Destroying;
import static org.apache.bifromq.basekv.store.range.KVRangeFSM.Lifecycle.Init;
import static org.apache.bifromq.basekv.store.range.KVRangeFSM.Lifecycle.Open;
import static org.apache.bifromq.basekv.store.util.ExecutorServiceUtil.awaitShutdown;
import static org.apache.bifromq.basekv.store.util.VerUtil.boundaryCompatible;
import static org.apache.bifromq.basekv.store.util.VerUtil.print;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.NULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.canCombine;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.combine;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.isSplittable;

import com.google.common.collect.Maps;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.bifromq.base.util.AsyncRunner;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.baseenv.ZeroCopyParser;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.CancelMerging;
import org.apache.bifromq.basekv.proto.CancelMergingReply;
import org.apache.bifromq.basekv.proto.CancelMergingRequest;
import org.apache.bifromq.basekv.proto.ChangeConfig;
import org.apache.bifromq.basekv.proto.Delete;
import org.apache.bifromq.basekv.proto.EnsureRange;
import org.apache.bifromq.basekv.proto.EnsureRangeReply;
import org.apache.bifromq.basekv.proto.KVRangeCommand;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeMessage;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.Merge;
import org.apache.bifromq.basekv.proto.MergeDone;
import org.apache.bifromq.basekv.proto.MergeDoneReply;
import org.apache.bifromq.basekv.proto.MergeDoneRequest;
import org.apache.bifromq.basekv.proto.MergeReply;
import org.apache.bifromq.basekv.proto.MergeRequest;
import org.apache.bifromq.basekv.proto.PrepareMergeTo;
import org.apache.bifromq.basekv.proto.PrepareMergeToReply;
import org.apache.bifromq.basekv.proto.PrepareMergeToRequest;
import org.apache.bifromq.basekv.proto.PrepareMergeWith;
import org.apache.bifromq.basekv.proto.Put;
import org.apache.bifromq.basekv.proto.SnapshotSyncRequest;
import org.apache.bifromq.basekv.proto.SplitHint;
import org.apache.bifromq.basekv.proto.SplitRange;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.proto.WALRaftMessages;
import org.apache.bifromq.basekv.raft.exception.LeaderTransferException;
import org.apache.bifromq.basekv.raft.exception.SnapshotException;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.raft.proto.RaftMessage;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import org.apache.bifromq.basekv.store.api.IKVLoadRecord;
import org.apache.bifromq.basekv.store.api.IKVRangeCoProc;
import org.apache.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import org.apache.bifromq.basekv.store.api.IKVRangeSplitHinter;
import org.apache.bifromq.basekv.store.api.IKVReader;
import org.apache.bifromq.basekv.store.exception.KVRangeException;
import org.apache.bifromq.basekv.store.option.KVRangeOptions;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import org.apache.bifromq.basekv.store.stats.IStatsCollector;
import org.apache.bifromq.basekv.store.util.VerUtil;
import org.apache.bifromq.basekv.store.wal.IKVRangeWAL;
import org.apache.bifromq.basekv.store.wal.IKVRangeWALStore;
import org.apache.bifromq.basekv.store.wal.IKVRangeWALSubscriber;
import org.apache.bifromq.basekv.store.wal.IKVRangeWALSubscription;
import org.apache.bifromq.basekv.store.wal.KVRangeWAL;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

/**
 * The finite state machine of a KVRange.
 */
public class KVRangeFSM implements IKVRangeFSM {
    private static final Runnable NOOP = () -> {
    };
    private final Logger log;
    private final KVRangeId id;
    private final String hostStoreId;
    private final IKVRange kvRange;
    private final IKVRangeWAL wal;
    private final IKVRangeWALSubscription walSubscription;
    private final IStatsCollector statsCollector;
    private final ExecutorService fsmExecutor;
    private final ExecutorService mgmtExecutor;
    private final AsyncRunner mgmtTaskRunner;
    private final IKVRangeCoProc coProc;
    private final KVRangeQueryLinearizer linearizer;
    private final IKVRangeQueryRunner queryRunner;
    private final Map<String, CompletableFuture<?>> cmdFutures = new ConcurrentHashMap<>();
    private final Map<String, KVRangeDumpSession> dumpSessions = Maps.newConcurrentMap();
    private final SnapshotBandwidthGovernor snapshotBandwidthGovernor;
    private final AtomicInteger taskSeqNo = new AtomicInteger();
    private final BehaviorSubject<KVRangeDescriptor> descriptorSubject = BehaviorSubject.create();
    private final Subject<List<SplitHint>> splitHintsSubject = BehaviorSubject.<List<SplitHint>>create().toSerialized();
    private final Subject<Any> factSubject = BehaviorSubject.create();
    private final KVRangeOptions opts;
    private final AtomicBoolean recovering = new AtomicBoolean();
    private final AtomicReference<Lifecycle> lifecycle = new AtomicReference<>(Lifecycle.Init);
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final CompletableFuture<Void> closeSignal = new CompletableFuture<>();
    private final CompletableFuture<Void> quitSignal = new CompletableFuture<>();
    private final CompletableFuture<Void> destroyedSignal = new CompletableFuture<>();
    private final AtomicLong lastShrinkCheckAt = new AtomicLong();
    private final AtomicBoolean shrinkingWAL = new AtomicBoolean();
    private final KVRangeMetricManager metricManager;
    private final List<IKVRangeSplitHinter> splitHinters;
    private final StampedLock resetLock = new StampedLock();
    private final String[] tags;
    private volatile long zombieAt = -1;
    private IKVRangeMessenger messenger;
    private KVRangeRestorer restorer;

    public KVRangeFSM(String clusterId,
                      String hostStoreId,
                      KVRangeId id,
                      IKVRangeCoProcFactory coProcFactory,
                      IKVRange kvRange,
                      IKVRangeWALStore walStore,
                      Executor queryExecutor,
                      Executor bgExecutor,
                      KVRangeOptions opts,
                      QuitListener quitListener) {
        this.opts = opts.toBuilder().build();
        this.id = id;
        this.hostStoreId = hostStoreId; // keep a local copy to decouple it from store's state
        this.kvRange = kvRange;
        tags = new String[] {"clusterId", clusterId, "storeId", hostStoreId, "rangeId", KVRangeIdUtil.toString(id)};
        this.log = MDCLogger.getLogger(KVRangeFSM.class, tags);
        this.wal = new KVRangeWAL(clusterId, hostStoreId, id,
            walStore, opts.getWalRaftConfig(), opts.getMaxWALFatchBatchSize());
        this.fsmExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                EnvProvider.INSTANCE.newThreadFactory("basekv-range-mutator")),
            "mutator", "basekv.range", Tags.of(tags));
        this.mgmtExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                EnvProvider.INSTANCE.newThreadFactory("basekv-range-manager")),
            "manager", "basekv.range", Tags.of(tags));
        this.mgmtTaskRunner =
            new AsyncRunner("basekv.runner.rangemanager", mgmtExecutor, "rangeId", KVRangeIdUtil.toString(id));
        this.splitHinters = coProcFactory.createHinters(clusterId, hostStoreId, id, this.kvRange::newDataReader);
        this.coProc = coProcFactory.createCoProc(clusterId, hostStoreId, id, this.kvRange::newDataReader);
        this.snapshotBandwidthGovernor = new SnapshotBandwidthGovernor(opts.getSnapshotSyncBytesPerSec());

        long lastAppliedIndex = this.kvRange.lastAppliedIndex();
        this.linearizer = new KVRangeQueryLinearizer(wal::readIndex, queryExecutor, lastAppliedIndex, tags);
        this.queryRunner = new KVRangeQueryRunner(this.kvRange, coProc, queryExecutor, linearizer,
            splitHinters, this::latestLeaderDescriptor, resetLock, tags);
        this.statsCollector = new KVRangeStatsCollector(this.kvRange,
            wal, Duration.ofSeconds(opts.getStatsCollectIntervalSec()), bgExecutor);
        this.metricManager = new KVRangeMetricManager(clusterId, hostStoreId, id);
        this.walSubscription = wal.subscribe(lastAppliedIndex,
            new IKVRangeWALSubscriber() {
                @Override
                public CompletableFuture<Void> apply(LogEntry log) {
                    return metricManager.recordLogApply(() -> KVRangeFSM.this.apply(log));
                }

                @Override
                public CompletableFuture<Void> restore(KVRangeSnapshot snapshot, String leader,
                                                       IAfterRestoredCallback callback) {
                    return metricManager.recordSnapshotInstall(
                        () -> KVRangeFSM.this.restore(snapshot, leader, callback));
                }
            }, fsmExecutor);
        quitSignal.thenAccept(v -> quitListener.onQuit(this));
    }

    @Override
    public KVRangeId id() {
        return id;
    }

    public long ver() {
        return kvRange.version();
    }

    @Override
    public Boundary boundary() {
        return kvRange.boundary();
    }

    @Override
    public CompletableFuture<Void> open(IKVRangeMessenger messenger) {
        if (lifecycle.get() != Init) {
            return CompletableFuture.completedFuture(null);
        }
        return mgmtTaskRunner.add(() -> {
            if (lifecycle.compareAndSet(Init, Lifecycle.Opening)) {
                this.messenger = messenger;
                factSubject.onNext(reset(kvRange.boundary()));
                // start the wal
                wal.start();
                this.restorer = new KVRangeRestorer(wal.latestSnapshot(), kvRange, messenger,
                    metricManager, fsmExecutor, opts.getSnapshotSyncIdleTimeoutSec(),
                    tags);
                disposables.add(wal.peerMessages().observeOn(Schedulers.io())
                    .subscribe((messages) -> {
                        for (String peerId : messages.keySet()) {
                            messenger.send(KVRangeMessage.newBuilder()
                                .setRangeId(id)
                                .setHostStoreId(peerId)
                                .setWalRaftMessages(WALRaftMessages.newBuilder()
                                    .addAllWalMessages(messages.get(peerId))
                                    .build())
                                .build());
                        }
                    }));
                disposables.add(descriptorSubject.subscribe(metricManager::report));
                disposables.add(Observable.combineLatest(
                        kvRange.metadata().distinctUntilChanged(),
                        wal.state().distinctUntilChanged(),
                        wal.replicationStatus().distinctUntilChanged(),
                        statsCollector.collect().distinctUntilChanged(),
                        splitHintsSubject.distinctUntilChanged(),
                        factSubject.distinctUntilChanged(),
                        (meta,
                         role,
                         syncStats,
                         rangeStats,
                         splitHints,
                         fact) -> {
                            log.trace("Split hints: \n{}", splitHints);
                            return KVRangeDescriptor.newBuilder()
                                .setVer(meta.ver())
                                .setId(id)
                                .setBoundary(meta.boundary())
                                .setRole(role)
                                .setState(meta.state().getType())
                                .setConfig(meta.clusterConfig())
                                .putAllSyncState(syncStats)
                                .putAllStatistics(rangeStats)
                                .addAllHints(splitHints)
                                .setHlc(HLC.INST.get())
                                .setFact(fact)
                                .build();
                        })
                    .observeOn(Schedulers.from(mgmtExecutor))
                    .subscribe(descriptorSubject::onNext));
                disposables.add(messenger.receive().subscribe(this::handleMessage));
                disposables.add(descriptorSubject.subscribe(this::detectZombieState));
                lifecycle.set(Open);
                metricManager.reportLastAppliedIndex(kvRange.lastAppliedIndex());
                log.info("Open range: appliedIndex={}, state={}, ver={}",
                    kvRange.lastAppliedIndex(), kvRange.state().getType(), print(kvRange.version()));
                // make sure latest snapshot exists
                if (!kvRange.hasCheckpoint(wal.latestSnapshot())) {
                    log.debug("Latest snapshot not available, do compaction: \n{}", wal.latestSnapshot());
                    compactWAL();
                }
            }
        });
    }

    @Override
    public void tick() {
        if (isNotOpening()) {
            return;
        }
        wal.tick();
        statsCollector.tick();
        dumpSessions.values().forEach(KVRangeDumpSession::tick);
        shrinkWAL();
        checkAndRepairFromZombieState();
        estimateSplitHint();
    }

    @Override
    public CompletableFuture<Void> close() {
        return this.doClose().thenCompose(v -> awaitShutdown(mgmtExecutor));
    }

    private CompletableFuture<Void> doClose() {
        switch (lifecycle.get()) {
            case Init -> {
                return CompletableFuture.completedFuture(null);
            }
            case Opening -> {
                return CompletableFuture.failedFuture(new IllegalStateException("Range is opening"));
            }
            case Open -> {
                if (lifecycle.compareAndSet(Open, Lifecycle.Closing)) {
                    log.info("Closing range");
                    descriptorSubject.onComplete();
                    disposables.dispose();
                    walSubscription.stop();
                    splitHinters.forEach(IKVRangeSplitHinter::close);
                    coProc.close();
                    CompletableFuture.allOf(dumpSessions.values()
                            .stream()
                            .map(dumpSession -> {
                                dumpSession.cancel();
                                return dumpSession.awaitDone();
                            })
                            .toArray(CompletableFuture<?>[]::new))
                        .thenCompose(v -> restorer.awaitDone())
                        .thenCompose(v -> statsCollector.stop())
                        .thenCompose(v -> mgmtTaskRunner.awaitDone())
                        .thenCompose(v -> wal.close())
                        .thenCompose(v -> awaitShutdown(fsmExecutor))
                        .whenComplete((v, e) -> {
                            kvRange.close();
                            metricManager.close();
                            cmdFutures.values()
                                .forEach(f -> f.completeExceptionally(new KVRangeException.TryLater("Range closed")));
                            queryRunner.close();
                            log.info("Range closed");
                            lifecycle.set(Closed);
                            closeSignal.complete(null);
                        });
                }
                return closeSignal;
            }
            default -> {
                return closeSignal;
            }
        }
    }

    @Override
    public CompletableFuture<Void> destroy() {
        if (lifecycle.get() == Open) {
            log.info("Destroying range");
            doClose()
                .thenCompose(v -> {
                    if (lifecycle.compareAndSet(Closed, Destroying)) {
                        kvRange.destroy();
                        return wal.destroy();
                    }
                    return CompletableFuture.completedFuture(null);
                })
                .whenComplete((v, e) -> {
                    if (lifecycle.compareAndSet(Destroying, Destroyed)) {
                        log.info("Range destroyed");
                        destroyedSignal.complete(null);
                    }
                })
                .thenCompose(v -> awaitShutdown(mgmtExecutor));
        }
        return destroyedSignal;
    }

    @Override
    public CompletableFuture<Void> recover() {
        return wal.recover();
    }

    @Override
    public CompletableFuture<Void> transferLeadership(long ver, String newLeader) {
        return metricManager.recordTransferLeader(() -> {
            if (ver != kvRange.version()) {
                // version not exactly match
                return CompletableFuture.failedFuture(
                    new KVRangeException.BadVersion("Version Mismatch", latestLeaderDescriptor()));
            }
            log.info("Transferring leader[ver={}, state={}]: newLeader={}",
                print(ver), kvRange.state().getType(), newLeader);
            return wal.transferLeadership(newLeader)
                .exceptionally(unwrap(e -> {
                    if (e instanceof LeaderTransferException.NotFoundOrQualifiedException) {
                        throw new KVRangeException.BadRequest("Failed to transfer leadership", e);
                    } else {
                        throw new KVRangeException.TryLater("Failed to transfer leadership", e);
                    }
                }));
        });
    }

    @Override
    public CompletableFuture<Void> changeReplicaConfig(long ver, Set<String> newVoters, Set<String> newLearners) {
        if (isNotOpening()) {
            // treat un-opened range as not exist
            return CompletableFuture.failedFuture(
                new KVRangeException.InternalException("Range not open:" + KVRangeIdUtil.toString(id)));
        }
        return changeReplicaConfig(nextTaskId(), ver, newVoters, newLearners);
    }

    private CompletableFuture<Void> changeReplicaConfig(String taskId, long ver,
                                                        Set<String> newVoters,
                                                        Set<String> newLearners) {
        return metricManager.recordConfigChange(() -> submitManagementCommand(KVRangeCommand.newBuilder()
            .setTaskId(taskId)
            .setVer(ver)
            .setChangeConfig(ChangeConfig.newBuilder()
                .addAllVoters(newVoters)
                .addAllLearners(newLearners)
                .build())
            .build()));
    }

    @Override
    public CompletableFuture<Void> split(long ver, ByteString splitKey) {
        return metricManager.recordSplit(() -> submitManagementCommand(KVRangeCommand.newBuilder()
            .setTaskId(nextTaskId())
            .setVer(ver)
            .setSplitRange(SplitRange.newBuilder()
                .setSplitKey(splitKey)
                .setNewId(KVRangeIdUtil.next(id))
                .build())
            .build()));
    }

    @Override
    public CompletableFuture<Void> merge(long ver, KVRangeId mergeeId) {
        return metricManager.recordMerge(() -> submitManagementCommand(KVRangeCommand.newBuilder()
            .setTaskId(nextTaskId())
            .setVer(ver)
            .setPrepareMergeWith(PrepareMergeWith.newBuilder()
                .setMergeeId(mergeeId)
                .buildPartial())
            .build()));
    }

    @Override
    public CompletableFuture<Boolean> exist(long ver, ByteString key, boolean linearized) {
        if (isNotOpening()) {
            // treat un-opened range as not exist
            return CompletableFuture.failedFuture(
                new KVRangeException.InternalException("Range not open:" + KVRangeIdUtil.toString(id)));
        }
        return metricManager.recordExist(() -> queryRunner.exist(ver, key, linearized));
    }

    @Override
    public CompletableFuture<Optional<ByteString>> get(long ver, ByteString key, boolean linearized) {
        if (isNotOpening()) {
            // treat un-opened range as not exist
            return CompletableFuture.failedFuture(
                new KVRangeException.InternalException("Range not open:" + KVRangeIdUtil.toString(id)));
        }
        return metricManager.recordGet(() -> queryRunner.get(ver, key, linearized));
    }

    @Override
    public CompletableFuture<ROCoProcOutput> queryCoProc(long ver, ROCoProcInput query, boolean linearized) {
        if (isNotOpening()) {
            // treat un-opened range as not exist
            return CompletableFuture.failedFuture(
                new KVRangeException.InternalException("Range not open:" + KVRangeIdUtil.toString(id)));
        }
        return metricManager.recordQueryCoProc(() -> queryRunner.queryCoProc(ver, query, linearized));
    }

    @Override
    public CompletableFuture<ByteString> put(long ver, ByteString key, ByteString value) {
        return metricManager.recordPut(() -> submitMutationCommand(KVRangeCommand.newBuilder()
            .setVer(ver)
            .setTaskId(nextTaskId())
            .setPut(Put.newBuilder().setKey(key).setValue(value).build())
            .build()));
    }

    @Override
    public CompletableFuture<ByteString> delete(long ver, ByteString key) {
        return metricManager.recordDelete(() -> submitMutationCommand(KVRangeCommand.newBuilder()
            .setVer(ver)
            .setTaskId(nextTaskId())
            .setDelete(Delete.newBuilder().setKey(key).build())
            .build()));
    }

    @Override
    public CompletableFuture<RWCoProcOutput> mutateCoProc(long ver, RWCoProcInput mutate) {
        return metricManager.recordMutateCoProc(() -> submitMutationCommand(KVRangeCommand.newBuilder()
            .setVer(ver)
            .setTaskId(nextTaskId())
            .setRwCoProc(mutate)
            .build()));
    }

    private <T> CompletableFuture<T> submitMutationCommand(KVRangeCommand mutationCommand) {
        if (isNotOpening()) {
            // treat un-opened range as not exist
            return CompletableFuture.failedFuture(
                new KVRangeException.InternalException("Range not open:" + KVRangeIdUtil.toString(id)));
        }
        if (!boundaryCompatible(mutationCommand.getVer(), kvRange.version())) {
            return CompletableFuture.failedFuture(
                new KVRangeException.BadVersion("Version Mismatch", latestLeaderDescriptor()));
        }
        State state = kvRange.state();
        if (state.getType() == NoUse
            || state.getType() == WaitingForMerge
            || state.getType() == Merged
            || state.getType() == MergedQuiting
            || state.getType() == Removed
            || state.getType() == ToBePurged) {
            return CompletableFuture.failedFuture(new KVRangeException.TryLater(
                "Range is being merge or has been merged: state=" + state.getType().name()));
        }
        return submitCommand(mutationCommand);
    }

    private <T> CompletableFuture<T> submitManagementCommand(KVRangeCommand managementCommand) {
        if (isNotOpening()) {
            return CompletableFuture.failedFuture(
                new KVRangeException.InternalException("Range not open:" + KVRangeIdUtil.toString(id)));
        }
        if (managementCommand.getVer() != kvRange.version()) {
            return CompletableFuture.failedFuture(
                new KVRangeException.BadVersion("Version Mismatch", latestLeaderDescriptor()));
        }
        return submitCommand(managementCommand);
    }

    @Override
    public Observable<KVRangeDescriptor> describe() {
        return descriptorSubject;
    }

    private String nextTaskId() {
        return hostStoreId + "-" + taskSeqNo.getAndIncrement();
    }

    @SuppressWarnings("unchecked")
    private <T> CompletableFuture<T> submitCommand(KVRangeCommand command) {
        CompletableFuture<T> onDone = new CompletableFuture<>();

        // add to writeRequests must happen before proposing
        CompletableFuture<T> prev = (CompletableFuture<T>) cmdFutures.put(command.getTaskId(), onDone);
        assert prev == null;

        CompletableFuture<Long> proposeFuture = wal.propose(command);
        onDone.whenComplete((v, e) -> {
            cmdFutures.remove(command.getTaskId(), onDone);
            if (onDone.isCancelled()) {
                // canceled by caller, stop proposing as well
                proposeFuture.cancel(true);
            }
        });
        proposeFuture.whenCompleteAsync((r, e) -> {
            if (e != null) {
                onDone.completeExceptionally(e);
            }
        }, fsmExecutor);
        return onDone;
    }

    private void finishCommand(String taskId) {
        finishCommand(taskId, null);
    }

    @SuppressWarnings("unchecked")
    private <T> void finishCommand(String taskId, T result) {
        CompletableFuture<T> f = (CompletableFuture<T>) cmdFutures.get(taskId);
        if (f != null) {
            log.trace("Finish write request: taskId={}", taskId);
            f.complete(result);
        }
    }

    private void finishCommandWithError(String taskId, Throwable e) {
        CompletableFuture<?> f = cmdFutures.get(taskId);
        if (f != null) {
            log.trace("Finish write request with error: taskId={}", taskId, e);
            f.completeExceptionally(e);
        }
    }

    private CompletableFuture<Void> apply(LogEntry entry) {
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        if (kvRange.lastAppliedIndex() > entry.getIndex()) {
            // skip already applied log
            log.debug("Skip already applied log: index={}, term={}", entry.getIndex(), entry.getTerm());
            onDone.complete(null);
            return onDone;
        }
        switch (entry.getTypeCase()) {
            case CONFIG -> {
                IKVRangeWriter<?> rangeWriter = kvRange.toWriter();
                try {
                    Supplier<CompletableFuture<Void>> afterLogApplied = applyConfigChange(entry.getTerm(),
                        entry.getIndex(), entry.getConfig(), rangeWriter);
                    rangeWriter.lastAppliedIndex(entry.getIndex());
                    rangeWriter.done();
                    afterLogApplied.get()
                        .whenComplete((v, e) -> {
                            if (e != null) {
                                log.error("Failed to apply config change", e);
                                onDone.completeExceptionally(e);
                            } else {
                                linearizer.afterLogApplied(entry.getIndex());
                                metricManager.reportLastAppliedIndex(entry.getIndex());
                                onDone.complete(null);
                            }
                        });
                } catch (Throwable t) {
                    rangeWriter.abort();
                    log.error("Failed to apply command", t);
                    onDone.completeExceptionally(t);
                }
            }
            case DATA -> {
                try {
                    KVRangeCommand command = ZeroCopyParser.parse(entry.getData(), KVRangeCommand.parser());
                    IKVLoadRecorder loadRecorder = new KVLoadRecorder();
                    IKVRangeWriter<?> rangeWriter = kvRange.toWriter(loadRecorder);
                    IKVReader borrowedReader = kvRange.borrowDataReader();
                    IKVReader recordableReader = new LoadRecordableKVReader(borrowedReader, loadRecorder);
                    long version = kvRange.version();
                    State state = kvRange.state();
                    Boundary boundary = kvRange.boundary();
                    ClusterConfig clusterConfig = kvRange.clusterConfig();
                    applyCommand(version, state, boundary, clusterConfig,
                        entry.getTerm(), entry.getIndex(), command, recordableReader, rangeWriter)
                        .whenComplete((callback, e) -> {
                            if (onDone.isCancelled()) {
                                rangeWriter.abort();
                            } else {
                                try {
                                    if (e != null) {
                                        rangeWriter.abort();
                                        onDone.completeExceptionally(e);
                                    } else {
                                        rangeWriter.lastAppliedIndex(entry.getIndex());
                                        rangeWriter.done();
                                        if (command.hasRwCoProc()) {
                                            IKVLoadRecord loadRecord = loadRecorder.stop();
                                            splitHinters.forEach(
                                                hint -> hint.recordMutate(command.getRwCoProc(), loadRecord));
                                        }
                                        callback.run();
                                        linearizer.afterLogApplied(entry.getIndex());
                                        metricManager.reportLastAppliedIndex(entry.getIndex());
                                        onDone.complete(null);
                                    }
                                } catch (Throwable t) {
                                    log.error("Failed to apply log", t);
                                    onDone.completeExceptionally(t);
                                }
                            }
                            kvRange.returnDataReader(borrowedReader);
                        });
                } catch (Throwable t) {
                    log.error("Failed to apply command", t);
                    onDone.completeExceptionally(t);
                }
            }
            default -> {
                // no nothing
            }
        }
        return onDone;
    }

    private Supplier<CompletableFuture<Void>> applyConfigChange(long term,
                                                                long index,
                                                                ClusterConfig config,
                                                                IKVRangeWritable<?> rangeWriter) {
        State state = rangeWriter.state();
        log.info("Apply new config[term={}, index={}]: state={}, leader={}\n{}",
            term, index, state, wal.isLeader(), config);
        rangeWriter.clusterConfig(config);
        if (config.getNextVotersCount() != 0 || config.getNextLearnersCount() != 0) {
            // skip joint-config
            return () -> CompletableFuture.completedFuture(null);
        }
        Set<String> members = newHashSet();
        members.addAll(config.getVotersList());
        members.addAll(config.getLearnersList());
        switch (state.getType()) {
            case ConfigChanging -> {
                // reset back to normal
                String taskId = state.getTaskId();
                rangeWriter.bumpVer(false);
                if (taskId.equals(config.getCorrelateId())) {
                    // request config change success, requested config applied
                    boolean remove = !members.contains(hostStoreId);
                    if (remove) {
                        log.debug("Replica removed[newConfig={}]", config);
                        rangeWriter.state(State.newBuilder()
                            .setType(Removed)
                            .setTaskId(taskId)
                            .build());
                        return () -> {
                            quitSignal.complete(null);
                            finishCommand(taskId);
                            return CompletableFuture.completedFuture(null);
                        };
                    } else {
                        rangeWriter.state(State.newBuilder()
                            .setType(Normal)
                            .setTaskId(taskId)
                            .build());
                        return () -> compactWAL().thenRun(() -> finishCommand(taskId));
                    }
                } else {
                    // request config change failed, the config entry is appended due to leader reelection
                    boolean remove = !members.contains(hostStoreId);
                    if (remove) {
                        rangeWriter.state(State.newBuilder()
                            .setType(Removed)
                            .setTaskId(taskId)
                            .build());
                        return () -> {
                            quitSignal.complete(null);
                            finishCommand(taskId);
                            return CompletableFuture.completedFuture(null);
                        };
                    } else {
                        rangeWriter.state(State.newBuilder()
                            .setType(Normal)
                            .setTaskId(taskId)
                            .build());
                        return () -> compactWAL().thenRun(() -> finishCommand(taskId));
                    }
                }
            }
            case MergedQuiting -> {
                String taskId = state.getTaskId();
                boolean remove = !members.contains(hostStoreId);
                if (remove) {
                    rangeWriter.state(State.newBuilder()
                        .setType(Removed)
                        .setTaskId(taskId)
                        .build());
                } else {
                    rangeWriter.state(State.newBuilder()
                        .setType(Merged)
                        .setTaskId(taskId)
                        .build());
                }
                rangeWriter.bumpVer(false);
                return () -> {
                    finishCommand(taskId);
                    if (remove) {
                        quitSignal.complete(null);
                    }
                    return CompletableFuture.completedFuture(null);
                };
            }
            case ToBePurged -> {
                String taskId = state.getTaskId();
                if (taskId.equals(config.getCorrelateId())) {
                    // if configchange triggered by purge session succeed
                    rangeWriter.state(State.newBuilder()
                        .setType(Removed)
                        .setTaskId(taskId)
                        .build());
                    return () -> {
                        finishCommand(taskId);
                        quitSignal.complete(null);
                        return CompletableFuture.completedFuture(null);
                    };
                } else {
                    rangeWriter.state(State.newBuilder()
                        .setType(Normal)
                        .setTaskId(taskId)
                        .build());
                    return () -> compactWAL().thenRun(() -> {
                        // purge failed due to leader change, reset back to normal
                        log.debug("Purge failed due to leader change[newConfig={}]", config);
                        finishCommand(taskId);
                    });
                }
            }
            default -> {
                // skip internal config change triggered by leadership change, no need to compact WAL
                return () -> CompletableFuture.completedFuture(null);
            }
        }
    }

    private CompletableFuture<Runnable> applyCommand(long ver,
                                                     State state,
                                                     Boundary boundary,
                                                     ClusterConfig clusterConfig,
                                                     long logTerm,
                                                     long logIndex,
                                                     KVRangeCommand command,
                                                     IKVReader dataReader,
                                                     IKVRangeWritable<?> rangeWriter) {
        CompletableFuture<Runnable> onDone = new CompletableFuture<>();
        long reqVer = command.getVer();
        String taskId = command.getTaskId();
        if (log.isTraceEnabled()) {
            log.trace("Execute KVRange Command[term={}, index={}, taskId={}]: ver={}, state={}, \n{}",
                logTerm, logIndex, taskId, print(ver), state, command);
        }
        switch (command.getCommandTypeCase()) {
            // admin commands
            case CHANGECONFIG -> {
                ChangeConfig newConfig = command.getChangeConfig();
                if (reqVer != ver) {
                    // version not match, hint the caller
                    onDone.complete(() -> finishCommandWithError(taskId,
                        new KVRangeException.BadVersion("Version Mismatch", latestLeaderDescriptor())));
                    break;
                }
                if (state.getType() != Normal && state.getType() != Merged) {
                    onDone.complete(() -> finishCommandWithError(taskId, new KVRangeException.TryLater(
                        "Config change abort, range is in state:" + state.getType().name())));
                    break;
                }
                log.info(
                    "Changing Config[term={}, index={}, taskId={}, ver={}, state={}]: nextVoters={}, nextLearners={}",
                    logTerm, logIndex, taskId, print(ver), state, newConfig.getVotersList(),
                    newConfig.getLearnersList());
                // make a checkpoint if needed
                CompletableFuture<Void> compactWALFuture = CompletableFuture.completedFuture(null);
                if (wal.latestSnapshot().getLastAppliedIndex() < logIndex - 1) {
                    compactWALFuture = compactWAL();
                }
                compactWALFuture.whenCompleteAsync((v, e) -> {
                    if (e != null) {
                        log.error(
                            "WAL compact failed during ConfigChange[term={}, index={}, taskId={}]: ver={}, state={}",
                            logTerm, logIndex, taskId, print(ver), state);
                        // abort log apply and retry
                        onDone.completeExceptionally(e);
                    } else {
                        boolean toBePurged = isGracefulQuit(clusterConfig, newConfig);
                        // notify new voters and learners host store to ensure the range exist
                        Set<String> newHostingStoreIds = difference(
                            difference(
                                union(newHashSet(newConfig.getVotersList()), newHashSet(newConfig.getLearnersList())),
                                union(newHashSet(clusterConfig.getVotersList()),
                                    newHashSet(clusterConfig.getLearnersList()))
                            ),
                            singleton(hostStoreId)
                        );
                        Set<String> nextVoters = toBePurged
                            ? newHashSet(clusterConfig.getVotersList()) : newHashSet(newConfig.getVotersList());
                        Set<String> nextLearners = toBePurged
                            ? emptySet() : newHashSet(newConfig.getLearnersList());
                        if (wal.isLeader()) {
                            List<CompletableFuture<?>> onceFutures = newHostingStoreIds.stream()
                                .map(storeId -> messenger
                                    .once(m -> {
                                        if (m.hasEnsureRangeReply()) {
                                            EnsureRangeReply reply = m.getEnsureRangeReply();
                                            return reply.getResult() == EnsureRangeReply.Result.OK;
                                        }
                                        return false;
                                    })
                                    .orTimeout(5, TimeUnit.SECONDS)
                                )
                                .collect(Collectors.toList());
                            CompletableFuture.allOf(onceFutures.toArray(CompletableFuture[]::new))
                                .whenCompleteAsync((v1, t) -> {
                                    if (t != null) {
                                        String errorMessage = String.format("ConfigChange aborted[taskId=%s] due to %s",
                                            taskId, t.getMessage());
                                        log.warn(errorMessage);
                                        finishCommandWithError(taskId, new KVRangeException.TryLater(errorMessage));
                                        wal.stepDown();
                                        return;
                                    }
                                    wal.changeClusterConfig(taskId, nextVoters, nextLearners)
                                        .whenCompleteAsync((v2, e2) -> {
                                            if (e2 != null) {
                                                String errorMessage =
                                                    String.format("ConfigChange aborted[taskId=%s] due to %s",
                                                        taskId, e2.getMessage());
                                                log.debug(errorMessage);
                                                finishCommandWithError(taskId,
                                                    new KVRangeException.TryLater(errorMessage));
                                                wal.stepDown();
                                            }
                                            // postpone finishing command when config entry is applied
                                        }, fsmExecutor);
                                }, fsmExecutor);
                            newHostingStoreIds.forEach(storeId -> {
                                log.debug("Send EnsureRequest: taskId={}, targetStoreId={}", taskId, storeId);
                                ClusterConfig ensuredClusterConfig = ClusterConfig.getDefaultInstance();
                                messenger.send(KVRangeMessage.newBuilder()
                                    .setRangeId(id)
                                    .setHostStoreId(storeId)
                                    .setEnsureRange(EnsureRange.newBuilder()
                                        .setVer(ver) // ensure the new kvrange is compatible in target store
                                        .setBoundary(boundary)
                                        .setInitSnapshot(Snapshot.newBuilder()
                                            .setTerm(0)
                                            .setIndex(0)
                                            .setClusterConfig(ensuredClusterConfig) // empty voter set
                                            .setData(KVRangeSnapshot.newBuilder()
                                                .setVer(ver)
                                                .setId(id)
                                                // no checkpoint specified
                                                .setLastAppliedIndex(0)
                                                .setBoundary(boundary)
                                                .setState(state)
                                                .setClusterConfig(ensuredClusterConfig)
                                                .build().toByteString())
                                            .build())
                                        .build())
                                    .build());
                            });
                        } else {
                            wal.changeClusterConfig(taskId, nextVoters, nextLearners)
                                .whenCompleteAsync((v2, e2) -> {
                                    if (e2 != null) {
                                        String errorMessage =
                                            String.format("ConfigChange aborted[taskId=%s] due to %s",
                                                taskId, e2.getMessage());
                                        log.debug(errorMessage);
                                        finishCommandWithError(taskId,
                                            new KVRangeException.TryLater(errorMessage));
                                        wal.stepDown();
                                    }
                                    // postpone finishing command when config entry is applied
                                }, fsmExecutor);
                        }
                        if (state.getType() == Normal) {
                            if (toBePurged) {
                                rangeWriter.state(State.newBuilder()
                                    .setType(ToBePurged)
                                    .setTaskId(taskId)
                                    .build());
                            } else {
                                // only transit to ConfigChanging from Normal
                                rangeWriter.state(State.newBuilder()
                                    .setType(ConfigChanging)
                                    .setTaskId(taskId)
                                    .build());
                            }
                        } else if (state.getType() == Merged) {
                            if (toBePurged) {
                                rangeWriter.state(State.newBuilder()
                                    .setType(ToBePurged)
                                    .setTaskId(taskId)
                                    .build());
                            } else {
                                rangeWriter.state(State.newBuilder()
                                    .setType(MergedQuiting)
                                    .setTaskId(taskId)
                                    .build());
                            }
                        }
                        rangeWriter.bumpVer(false);
                        onDone.complete(NOOP);
                    }
                }, fsmExecutor);
            }
            case SPLITRANGE -> {
                SplitRange request = command.getSplitRange();
                if (reqVer != ver) {
                    // version not match, hint the caller
                    onDone.complete(() -> finishCommandWithError(taskId,
                        new KVRangeException.BadVersion("Version Mismatch", latestLeaderDescriptor())));
                    break;
                }
                if (state.getType() != Normal) {
                    onDone.complete(() -> finishCommandWithError(taskId, new KVRangeException.TryLater(
                        "Split abort, range is in state:" + state.getType().name())));
                    break;
                }
                // under at-least-once semantic, we need to check if the splitKey is still valid to skip
                // duplicated apply
                if (isSplittable(boundary, request.getSplitKey())) {
                    log.info(
                        "Splitting range[term={}, index={}, taskId={}, ver={}, state={}]: newRangeId={}, splitKey={}",
                        logTerm, logIndex, taskId, print(ver), state,
                        KVRangeIdUtil.toString(request.getNewId()), request.getSplitKey().toStringUtf8());
                    Boundary[] boundaries = BoundaryUtil.split(boundary, request.getSplitKey());
                    Boundary leftBoundary = boundaries[0];
                    Boundary rightBoundary = boundaries[1];
                    KVRangeSnapshot rhsSS = KVRangeSnapshot.newBuilder()
                        .setVer(VerUtil.bump(ver, true))
                        .setId(request.getNewId())
                        // no checkpoint specified
                        .setLastAppliedIndex(5)
                        .setBoundary(rightBoundary)
                        .setClusterConfig(clusterConfig)
                        .setState(State.newBuilder()
                            .setType(Normal)
                            .setTaskId(taskId)
                            .build())
                        .build();
                    rangeWriter.boundary(leftBoundary).bumpVer(true);
                    // migrate data to right-hand keyspace which created implicitly
                    rangeWriter.migrateTo(request.getNewId(), rightBoundary)
                        .resetVer(rhsSS.getVer())
                        .boundary(rhsSS.getBoundary())
                        .lastAppliedIndex(rhsSS.getLastAppliedIndex())
                        .state(rhsSS.getState())
                        .clusterConfig(rhsSS.getClusterConfig())
                        .done();
                    onDone.complete(() -> {
                        try {
                            log.debug("Range split completed[taskId={}]", taskId);
                            // reset hinter when boundary changed
                            splitHinters.forEach(hinter -> hinter.reset(leftBoundary));
                            factSubject.onNext(reset(leftBoundary));
                        } catch (Throwable t) {
                            log.error("Failed to reset hinter or coProc", t);
                            finishCommandWithError(taskId, t);
                        } finally {
                            finishCommand(taskId);
                        }
                        messenger.once(KVRangeMessage::hasEnsureRangeReply)
                            .orTimeout(300, TimeUnit.SECONDS)
                            .whenCompleteAsync((v, e) -> {
                                if (e != null || v.getEnsureRangeReply().getResult() != EnsureRangeReply.Result.OK) {
                                    log.error("Failed to load rhs range[taskId={}]: newRangeId={}",
                                        taskId, request.getNewId(), e);
                                }
                            }, fsmExecutor);
                        // ensure the new range is loaded in the store
                        log.debug("Sending EnsureRequest to load right KVRange[{}]",
                            KVRangeIdUtil.toString(request.getNewId()));
                        messenger.send(KVRangeMessage.newBuilder()
                            .setRangeId(rhsSS.getId())
                            .setHostStoreId(hostStoreId)
                            .setEnsureRange(EnsureRange.newBuilder()
                                .setVer(rhsSS.getVer())
                                .setBoundary(rhsSS.getBoundary())
                                .setInitSnapshot(Snapshot.newBuilder()
                                    .setTerm(0)
                                    .setIndex(rhsSS.getLastAppliedIndex())
                                    .setClusterConfig(rhsSS.getClusterConfig())
                                    .setData(rhsSS.toByteString())
                                    .build())
                                .build()).build());
                    });
                } else {
                    onDone.complete(() -> finishCommandWithError(taskId,
                        new KVRangeException.BadRequest("Invalid split key")));
                }
            }
            case PREPAREMERGEWITH -> {
                if (reqVer != ver) {
                    // version not match, hint the caller
                    onDone.complete(() -> finishCommandWithError(taskId,
                        new KVRangeException.BadVersion("Version Mismatch", latestLeaderDescriptor())));
                    break;
                }
                if (state.getType() != Normal) {
                    onDone.complete(() -> finishCommandWithError(taskId, new KVRangeException.TryLater(
                        "Merge abort, range is in state:" + state.getType().name())));
                    break;
                }
                log.info(
                    "Merging[term={}, index={}, taskId={}, ver={}, state={}]: mergerId={}, mergeeId={}",
                    logTerm, logIndex, taskId, print(ver), state,
                    KVRangeIdUtil.toString(id), KVRangeIdUtil.toString(command.getPrepareMergeWith().getMergeeId()));

                CompletableFuture<KVRangeMessage> onceFuture = messenger.once(m -> m.hasPrepareMergeToReply()
                    && m.getPrepareMergeToReply().getTaskId().equals(taskId)
                    && m.getPrepareMergeToReply().getAccept());
                onDone.whenCompleteAsync((v, e) -> {
                    if (onDone.isCancelled()) {
                        onceFuture.cancel(true);
                    }
                }, fsmExecutor);
                onceFuture.orTimeout(5, TimeUnit.SECONDS)
                    .whenCompleteAsync((v, e) -> {
                        if (e != null) {
                            onceFuture.cancel(true);
                            onDone.completeExceptionally(e);
                        } else {
                            Map<String, Boolean> waitingList = clusterConfig.getVotersList().stream()
                                .collect(Collectors.toMap(voter -> voter, voter -> false));
                            rangeWriter.state(State.newBuilder()
                                .setType(PreparedMerging)
                                .setTaskId(taskId)
                                .putAllWaitingList(waitingList)
                                .build());
                            rangeWriter.bumpVer(false);
                            onDone.complete(NOOP);
                        }
                    }, fsmExecutor);

                PrepareMergeWith request = command.getPrepareMergeWith();
                // broadcast
                messenger.send(KVRangeMessage.newBuilder()
                    .setRangeId(request.getMergeeId())
                    .setPrepareMergeToRequest(PrepareMergeToRequest.newBuilder()
                        .setTaskId(taskId)
                        .setId(id)
                        .setVer(VerUtil.bump(ver, false))
                        .setBoundary(boundary)
                        .setConfig(clusterConfig)
                        .build())
                    .build());
            }
            case CANCELMERGING -> {
                if (reqVer != ver) {
                    onDone.complete(NOOP);
                    break;
                }
                log.info(
                    "Merge canceled[term={}, index={}, taskId={}, ver={}, state={}]: mergerId={}, mergeeId={}",
                    logTerm, logIndex, taskId, print(ver), state,
                    KVRangeIdUtil.toString(id), KVRangeIdUtil.toString(command.getPrepareMergeWith().getMergeeId()));
                if (state.getType() == PreparedMerging && state.hasTaskId() && taskId.equals(state.getTaskId())) {
                    rangeWriter.state(State.newBuilder()
                        .setType(Normal)
                        .setTaskId(taskId)
                        .build());
                    rangeWriter.bumpVer(false);
                    onDone.complete(
                        () -> finishCommandWithError(taskId, new KVRangeException.TryLater("Merge canceled")));
                } else {
                    onDone.complete(NOOP);
                }
            }
            case PREPAREMERGETO -> {
                PrepareMergeTo request = command.getPrepareMergeTo();
                if (reqVer != ver) {
                    onDone.complete(NOOP);
                    break;
                }
                // here is the formal mergeable condition check
                if (state.getType() != Normal
                    || !isCompatible(request.getConfig(), clusterConfig)
                    || !canCombine(request.getBoundary(), boundary)) {
                    if (!taskId.equals(state.getTaskId())) {
                        log.debug("Cancel the loser merger[{}]",
                            KVRangeIdUtil.toString(request.getMergerId()));
                        // help the loser merger cancel its operation by broadcast CancelMerging
                        // via store message and wait for at lease one response to make sure
                        // the loser merger has got canceled
                        log.debug("Loser merger[{}] not found in local store",
                            KVRangeIdUtil.toString(request.getMergerId()));
                        CompletableFuture<KVRangeMessage> onceFuture = messenger.once(m ->
                            m.hasCancelMergingReply()
                                && m.getCancelMergingReply().getTaskId().equals(taskId)
                                && m.getCancelMergingReply().getAccept());
                        // cancel the future
                        onDone.whenCompleteAsync((v, e) -> {
                            if (onceFuture.isCancelled()) {
                                onceFuture.cancel(true);
                            }
                        }, fsmExecutor);
                        onceFuture.orTimeout(5, TimeUnit.SECONDS)
                            .whenCompleteAsync((v, e) -> {
                                if (e != null) {
                                    onDone.completeExceptionally(e);
                                } else {
                                    onDone.complete(NOOP);
                                }
                            }, fsmExecutor);
                        // broadcast
                        messenger.send(KVRangeMessage.newBuilder()
                            .setRangeId(request.getMergerId())
                            .setCancelMergingRequest(CancelMergingRequest.newBuilder()
                                .setTaskId(taskId)
                                .setVer(request.getMergerVer())
                                .setRequester(id)
                                .build())
                            .build());
                    } else {
                        // ignore duplicated requests from same merger
                        onDone.complete(NOOP);
                    }
                    break;
                }
                // tell merger replica to start merge by broadcast Merge command which may end up
                // multiple Merge commands targeting to same merger replica
                CompletableFuture<KVRangeMessage> onceFuture = messenger.once(m ->
                    m.hasMergeReply()
                        && m.getMergeReply().getTaskId().equals(taskId)
                        && m.getMergeReply().getAccept());
                // cancel the future
                onDone.whenCompleteAsync((v, e) -> {
                    if (onDone.isCancelled()) {
                        onceFuture.cancel(true);
                    }
                }, fsmExecutor);
                onceFuture.orTimeout(5, TimeUnit.SECONDS)
                    .whenCompleteAsync((v, e) -> {
                        if (e != null) {
                            onceFuture.cancel(true);
                            onDone.completeExceptionally(e);
                        } else {
                            rangeWriter.state(State.newBuilder()
                                .setType(WaitingForMerge)
                                .setTaskId(taskId)
                                .build());
                            rangeWriter.bumpVer(false);
                            onDone.complete(NOOP);
                        }
                    }, fsmExecutor);
                // broadcast
                messenger.send(KVRangeMessage.newBuilder()
                    .setRangeId(request.getMergerId())
                    .setMergeRequest(MergeRequest.newBuilder()
                        .setTaskId(taskId)
                        .setVer(request.getMergerVer())
                        .setMergeeId(id)
                        .setMergeeVer(VerUtil.bump(ver, false))
                        .setBoundary(boundary)
                        .setStoreId(hostStoreId)
                        .build())
                    .build());
            }
            case MERGE -> {
                Merge request = command.getMerge();
                if (reqVer != ver) {
                    onDone.complete(NOOP);
                    break;
                }
                assert state.getType() == PreparedMerging;
                Map<String, Boolean> waitingList = Maps.newHashMap(state.getWaitingListMap());
                // waiting list only track voter's progress
                if (waitingList.containsKey(request.getStoreId())) {
                    waitingList.put(request.getStoreId(), true);
                }
                int quorumSize = waitingList.size() / 2 + 1;
                int readyNum = Maps.filterValues(waitingList, v -> v).size();
                if (readyNum < quorumSize) {
                    // not ready for merge
                    // update waiting list only
                    rangeWriter.state(state.toBuilder()
                        .clearWaitingList()
                        .putAllWaitingList(waitingList)
                        .build());
                    onDone.complete(NOOP);
                    break;
                }
                // quorum meets
                CompletableFuture<Void> readyToMerge;
                if (waitingList.containsKey(hostStoreId) && waitingList.get(hostStoreId)) {
                    // if local merger is a voter and is ready to merge
                    readyToMerge = CompletableFuture.completedFuture(null);
                } else {
                    // waiting for Merge command from local mergee committed in merger's WAL
                    readyToMerge = wal.once(logIndex,
                            l -> {
                                if (l.hasData()) {
                                    try {
                                        KVRangeCommand nextCmd = ZeroCopyParser.parse(l.getData(), KVRangeCommand.parser());
                                        if (nextCmd.hasMerge()
                                            && nextCmd.getMerge().getStoreId().equals(hostStoreId)) {
                                            return true;
                                        }
                                    } catch (InvalidProtocolBufferException ex) {
                                        throw new KVRangeException.InternalException("Unable to parse logEntry",
                                            ex);
                                    }
                                }
                                return false;
                            }, fsmExecutor)
                        .thenAccept(v -> {
                        });
                    // cancel the future
                    onDone.whenCompleteAsync((v, e) -> {
                        if (onDone.isCancelled()) {
                            readyToMerge.cancel(true);
                        }
                    }, fsmExecutor);
                }
                readyToMerge.whenCompleteAsync((v1, e1) -> {
                    if (e1 != null) {
                        onDone.completeExceptionally(
                            new KVRangeException.TryLater("Merge condition not met"));
                        return;
                    }
                    // migrate data from mergee, and commit the migration after getting merge done reply
                    long newVer = Math.max(ver, request.getMergeeVer());
                    // make sure the version is odd
                    Boundary mergedBoundary = combine(boundary, request.getBoundary());
                    IKVRangeMetadataWriter<?> rightRangeWriter = rangeWriter.resetVer(VerUtil.bump(newVer, true))
                        .boundary(mergedBoundary)
                        .state(State.newBuilder()
                            .setType(Normal)
                            .setTaskId(taskId)
                            .build())
                        .migrateFrom(request.getMergeeId(), request.getBoundary());
                    rightRangeWriter.done();
                    CompletableFuture<KVRangeMessage> onceFuture = messenger.once(m ->
                        m.hasMergeDoneReply() && m.getMergeDoneReply().getTaskId().equals(taskId));
                    // cancel the future
                    onDone.whenCompleteAsync((v, e) -> {
                        if (onDone.isCancelled()) {
                            onceFuture.cancel(true);
                        }
                    }, fsmExecutor);
                    onceFuture.orTimeout(5, TimeUnit.SECONDS)
                        .whenCompleteAsync((v, e) -> {
                            if (e != null || !v.getMergeDoneReply().getAccept()) {
                                log.debug("Failed to send MergeDone request", e);
                                onceFuture.cancel(true);
                                onDone.completeExceptionally(e != null
                                    ? e : new KVRangeException.InternalException("Failed to send MergeDone request"));
                            } else {
                                log.info(
                                    "Merger done[term={}, index={}, taskId={}, ver={}, state={}]: mergerId={}, boundary={}",
                                    logTerm, logIndex, taskId, print(ver), state, KVRangeIdUtil.toString(id),
                                    mergedBoundary);
                                onDone.complete(() -> {
                                    try {
                                        // reset hinter when boundary changed
                                        splitHinters.forEach(hinter -> hinter.reset(mergedBoundary));
                                        factSubject.onNext(reset(mergedBoundary));
                                    } catch (Throwable t) {
                                        log.error("Failed to reset hinter and coProc", t);
                                    } finally {
                                        finishCommand(taskId);
                                    }
                                });
                            }
                        }, fsmExecutor);
                    // send merge done request to local mergee
                    log.debug("Send MergeDone request to Mergee[{}]", KVRangeIdUtil.toString(request.getMergeeId()));
                    messenger.send(KVRangeMessage.newBuilder()
                        .setRangeId(request.getMergeeId())
                        .setHostStoreId(hostStoreId)
                        .setMergeDoneRequest(MergeDoneRequest.newBuilder()
                            .setId(id)
                            .setTaskId(taskId)
                            .setMergeeVer(request.getMergeeVer())
                            .setStoreId(hostStoreId)
                            .build())
                        .build());
                }, fsmExecutor);
            }
            case MERGEDONE -> {
                MergeDone request = command.getMergeDone();
                if (reqVer != ver) {
                    onDone.complete(NOOP);
                    break;
                }
                log.info(
                    "Mergee done[term={}, index={}, taskId={}, ver={}, state={}]: mergeeId={}",
                    logTerm, logIndex, taskId, print(ver), state, KVRangeIdUtil.toString(id));
                if (request.getStoreId().equals(hostStoreId)) {
                    assert state.getType() == WaitingForMerge
                        && state.hasTaskId()
                        && taskId.equals(state.getTaskId());
                    rangeWriter.boundary(NULL_BOUNDARY)
                        .bumpVer(true)
                        .state(State.newBuilder()
                            .setType(Merged)
                            .setTaskId(taskId)
                            .build());
                    onDone.complete(() -> {
                        // reset hinter when boundary changed
                        splitHinters.forEach(hinter -> hinter.reset(NULL_BOUNDARY));
                        factSubject.onNext(reset(NULL_BOUNDARY));
                    });
                } else {
                    Map<String, Boolean> waitingList = Maps.newHashMap(state.getWaitingListMap());
                    waitingList.put(request.getStoreId(), true);
                    rangeWriter.state(state.toBuilder()
                        .clearWaitingList()
                        .putAllWaitingList(waitingList)
                        .build());
                    onDone.complete(NOOP);
                }
            }
            case PUT, DELETE, RWCOPROC -> {
                if (!boundaryCompatible(reqVer, ver)) {
                    onDone.complete(() -> finishCommandWithError(taskId,
                        new KVRangeException.BadVersion("Version Mismatch", latestLeaderDescriptor())));
                    break;
                }
                if (state.getType() == NoUse
                    || state.getType() == WaitingForMerge
                    || state.getType() == Merged
                    || state.getType() == MergedQuiting
                    || state.getType() == Removed
                    || state.getType() == ToBePurged) {
                    onDone.complete(() -> finishCommandWithError(taskId,
                        new KVRangeException.TryLater(
                            "Range is being merge or has been merged: state=" + state.getType().name())));
                    break;
                }
                try {
                    switch (command.getCommandTypeCase()) {
                        // normal commands
                        case DELETE -> {
                            Delete delete = command.getDelete();
                            Optional<ByteString> value = dataReader.get(delete.getKey());
                            if (value.isPresent()) {
                                rangeWriter.kvWriter().delete(delete.getKey());
                            }
                            onDone.complete(() -> finishCommand(taskId, value.orElse(ByteString.EMPTY)));
                        }
                        case PUT -> {
                            Put put = command.getPut();
                            Optional<ByteString> value = dataReader.get(put.getKey());
                            rangeWriter.kvWriter().put(put.getKey(), put.getValue());
                            onDone.complete(() -> finishCommand(taskId, value.orElse(ByteString.EMPTY)));
                        }
                        case RWCOPROC -> {
                            Supplier<IKVRangeCoProc.MutationResult> resultSupplier =
                                coProc.mutate(command.getRwCoProc(), dataReader, rangeWriter.kvWriter());
                            onDone.complete(() -> {
                                IKVRangeCoProc.MutationResult result = resultSupplier.get();
                                result.fact().ifPresent(factSubject::onNext);
                                finishCommand(taskId, result.output());
                            });
                        }
                        default -> {
                            // do nothing
                        }
                    }
                } catch (Throwable e) {
                    onDone.complete(
                        () -> finishCommandWithError(taskId, new KVRangeException.InternalException(
                            "Failed to execute " + command.getCommandTypeCase().name(), e)));
                }
            }
            default -> {
                log.error("Unknown KVRange Command[type={}]", command.getCommandTypeCase());
                onDone.complete(NOOP);
            }
        }
        return onDone;
    }

    private boolean isGracefulQuit(ClusterConfig currentConfig, ChangeConfig nextConfig) {
        return Set.of(hostStoreId).containsAll(currentConfig.getVotersList())
            && currentConfig.getLearnersCount() == 0
            && nextConfig.getVotersCount() == 0
            && nextConfig.getLearnersCount() == 0;
    }

    private KVRangeDescriptor latestLeaderDescriptor() {
        if (wal.isLeader()) {
            return descriptorSubject.getValue();
        }
        return null;
    }

    private CompletableFuture<Void> restore(KVRangeSnapshot snapshot,
                                            String leader,
                                            IKVRangeWALSubscriber.IAfterRestoredCallback onInstalled) {
        if (isNotOpening()) {
            return onInstalled.call(null,
                new KVRangeException.InternalException("Range not open:" + KVRangeIdUtil.toString(id)));
        }
        return mgmtTaskRunner.addFirst(() -> {
            if (isNotOpening()) {
                return CompletableFuture.completedFuture(null);
            }
            return restorer.restoreFrom(leader, snapshot)
                .handle((result, ex) -> {
                    if (ex != null) {
                        return onInstalled.call(null, ex);
                    } else {
                        return onInstalled.call(kvRange.checkpoint(), null);
                    }
                })
                .thenCompose(f -> f)
                .whenCompleteAsync(unwrap((v, e) -> {
                    if (e != null) {
                        if (e instanceof SnapshotException.ObsoleteSnapshotException) {
                            log.debug("Obsolete snapshot, reset kvRange to latest snapshot: \n{}", snapshot);
                            kvRange.toReseter(wal.latestSnapshot()).done();
                        }
                    } else {
                        linearizer.afterLogApplied(snapshot.getLastAppliedIndex());
                        metricManager.reportLastAppliedIndex(snapshot.getLastAppliedIndex());
                        // reset the co-proc
                        factSubject.onNext(reset(snapshot.getBoundary()));
                        // finish all pending tasks
                        cmdFutures.keySet().forEach(taskId -> finishCommandWithError(taskId,
                            new KVRangeException.TryLater("Restored from snapshot, try again")));
                    }
                }), fsmExecutor);
        });
    }

    private void estimateSplitHint() {
        splitHintsSubject.onNext(splitHinters.stream().map(IKVRangeSplitHinter::estimate).toList());
    }

    private void shrinkWAL() {
        if (isNotOpening()) {
            return;
        }
        if (shrinkingWAL.compareAndSet(false, true)) {
            long now = System.nanoTime();
            if (now - lastShrinkCheckAt.get() < Duration.ofSeconds(opts.getShrinkWALCheckIntervalSec()).toNanos()) {
                shrinkingWAL.set(false);
                return;
            }
            lastShrinkCheckAt.set(now);
            if (kvRange.lastAppliedIndex() - wal.latestSnapshot().getLastAppliedIndex()
                < opts.getCompactWALThreshold()) {
                shrinkingWAL.set(false);
                return;
            }
            mgmtTaskRunner.add(() -> {
                if (isNotOpening() || kvRange.state().getType() == ConfigChanging) {
                    // don't let compaction interferes with config changing process
                    shrinkingWAL.set(false);
                    return CompletableFuture.completedFuture(null);
                }
                KVRangeSnapshot latestSnapshot = wal.latestSnapshot();
                long lastAppliedIndex = kvRange.lastAppliedIndex();
                if (lastAppliedIndex - latestSnapshot.getLastAppliedIndex() < opts.getCompactWALThreshold()) {
                    shrinkingWAL.set(false);
                    return CompletableFuture.completedFuture(null);
                }
                if (!dumpSessions.isEmpty() || !restorer.awaitDone().isDone()) {
                    shrinkingWAL.set(false);
                    return CompletableFuture.completedFuture(null);
                }
                log.debug("Shrink wal with snapshot: lastAppliedIndex={}\n{}", lastAppliedIndex, latestSnapshot);
                return doCompactWAL().whenComplete((v, e) -> shrinkingWAL.set(false));
            });
        }
    }

    private CompletableFuture<Void> compactWAL() {
        // cancel all on-going dump sessions
        dumpSessions.forEach((sessionId, session) -> {
            session.cancel();
            dumpSessions.remove(sessionId, session);
        });
        return mgmtTaskRunner.add(this::doCompactWAL);
    }

    private CompletableFuture<Void> doCompactWAL() {
        return metricManager.recordCompact(() -> {
            KVRangeSnapshot snapshot = kvRange.checkpoint();
            log.debug("Compact wal using snapshot:\n{}", snapshot);
            return wal.compact(snapshot)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        log.error("Failed to compact WAL due to {}: \n{}", e.getMessage(), snapshot);
                    }
                });
        });
    }

    private void detectZombieState(KVRangeDescriptor descriptor) {
        if (zombieAt < 0) {
            if (descriptor.getRole() == RaftNodeStatus.Candidate) {
                zombieAt = HLC.INST.getPhysical();
            }
        } else {
            if (descriptor.getRole() != RaftNodeStatus.Candidate) {
                zombieAt = -1;
            }
        }
    }

    private void checkAndRepairFromZombieState() {
        if (zombieAt > 0
            && Duration.ofMillis(HLC.INST.getPhysical() - zombieAt).toSeconds() > opts.getZombieTimeoutSec()) {
            ClusterConfig clusterConfig = wal.latestClusterConfig();
            if (clusterConfig.getNextVotersCount() > 0
                && clusterConfig.getVotersList().equals(singletonList(hostStoreId))) {
                // recover from single voter change lost quorum
                if (recovering.compareAndSet(false, true)) {
                    log.info("Recovering from lost quorum during changing config from single voter: \n{}",
                        clusterConfig);
                    wal.recover().whenComplete((v, e) -> recovering.set(false));
                }
            } else if (!clusterConfig.getVotersList().contains(hostStoreId)
                && !clusterConfig.getLearnersList().contains(hostStoreId)) {
                log.info("Zombie state detected, send quit signal.");
                quitSignal.complete(null);
            }
        }
    }

    private boolean isNotOpening() {
        Lifecycle state = lifecycle.get();
        return state != Open;
    }

    private boolean isCompatible(ClusterConfig config1, ClusterConfig config2) {
        // both merger and mergee are not in config change process
        Set<String> voterSet1 = new HashSet<>(config1.getVotersList());
        Set<String> voterSet2 = new HashSet<>(config2.getVotersList());
        Set<String> learnerSet1 = new HashSet<>(config1.getLearnersList());
        Set<String> learnerSet2 = new HashSet<>(config2.getLearnersList());
        return voterSet1.equals(voterSet2)
            && learnerSet1.equals(learnerSet2)
            && config1.getNextVotersList().isEmpty()
            && config2.getNextVotersList().isEmpty()
            && config1.getNextLearnersList().isEmpty()
            && config2.getNextLearnersList().isEmpty();
    }

    private void handleMessage(KVRangeMessage message) {
        switch (message.getPayloadTypeCase()) {
            case WALRAFTMESSAGES ->
                handleWALMessages(message.getHostStoreId(), message.getWalRaftMessages().getWalMessagesList());
            case SNAPSHOTSYNCREQUEST ->
                handleSnapshotSyncRequest(message.getHostStoreId(), message.getSnapshotSyncRequest());
            case PREPAREMERGETOREQUEST ->
                handlePrepareMergeToRequest(message.getHostStoreId(), message.getPrepareMergeToRequest());
            case MERGEREQUEST -> handleMergeRequest(message.getHostStoreId(), message.getMergeRequest());
            case CANCELMERGINGREQUEST ->
                handleCancelMergingRequest(message.getHostStoreId(), message.getCancelMergingRequest());
            case MERGEDONEREQUEST -> handleMergeDoneRequest(message.getHostStoreId(), message.getMergeDoneRequest());
            default -> {
                // do nothing
            }
        }
    }

    private void handleWALMessages(String peerId, List<RaftMessage> messages) {
        wal.receivePeerMessages(peerId, messages);
    }

    private void handleSnapshotSyncRequest(String follower, SnapshotSyncRequest request) {
        log.info("Dumping snapshot: session={}: follower={}\n{}",
            request.getSessionId(), follower, request.getSnapshot());
        KVRangeDumpSession session = new KVRangeDumpSession(follower, request, kvRange, messenger,
            Duration.ofSeconds(opts.getSnapshotSyncIdleTimeoutSec()),
            opts.getSnapshotSyncBytesPerSec(), snapshotBandwidthGovernor, metricManager::reportDump, tags);
        dumpSessions.put(session.id(), session);
        session.awaitDone().whenComplete((result, e) -> {
            switch (result) {
                case OK -> log.info("Snapshot dumped: session={}, follower={}", session.id(), follower);
                case Canceled -> log.info("Snapshot dump canceled: session={}, follower={}", session.id(), follower);
                case NoCheckpoint -> {
                    log.info("No checkpoint found, compact WAL now");
                    compactWAL();
                }
                case Abort -> log.info("Snapshot dump aborted: session={}, follower={}", session.id(), follower);
                case Error -> log.warn("Snapshot dump failed: session={}, follower={}", session.id(), follower);
                default -> {
                    // do nothing
                }
            }
            dumpSessions.remove(session.id(), session);
        });
    }

    private void handlePrepareMergeToRequest(String peer, PrepareMergeToRequest request) {
        log.debug("Handle PrepareMergeTo request \n{}", request);
        // I'm the mergee
        descriptorSubject.firstElement()
            .timeout(5, TimeUnit.SECONDS)
            .subscribe(rangeDescriptor ->
                    wal.propose(KVRangeCommand.newBuilder()
                            .setTaskId(request.getTaskId())
                            .setVer(rangeDescriptor.getVer())
                            .setPrepareMergeTo(PrepareMergeTo.newBuilder()
                                .setMergerId(request.getId())
                                .setMergerVer(request.getVer())
                                .setBoundary(request.getBoundary())
                                .setConfig(request.getConfig())
                                .build())
                            .build())
                        .whenCompleteAsync((v, e) -> {
                            if (e != null) {
                                log.debug("Failed to propose command[PrepareMergeTo]: \n{}", request, e);
                            } else {
                                log.debug("Command[PrepareMergeTo] proposed: index={}\n{}", v, request);
                            }
                            messenger.send(KVRangeMessage.newBuilder()
                                .setRangeId(request.getId())
                                .setHostStoreId(peer)
                                .setPrepareMergeToReply(PrepareMergeToReply.newBuilder()
                                    .setTaskId(request.getTaskId())
                                    .setAccept(e == null)
                                    .build())
                                .build());
                        }, fsmExecutor),
                e -> messenger.send(KVRangeMessage.newBuilder()
                    .setRangeId(request.getId())
                    .setHostStoreId(peer)
                    .setPrepareMergeToReply(PrepareMergeToReply.newBuilder()
                        .setTaskId(request.getTaskId())
                        .setAccept(false)
                        .build())
                    .build()));
    }

    private void handleMergeRequest(String peer, MergeRequest request) {
        log.debug("Handle Merge request: \n{}", request);
        wal.propose(KVRangeCommand.newBuilder()
                .setTaskId(request.getTaskId())
                .setVer(request.getVer())
                .setMerge(Merge.newBuilder()
                    .setMergeeId(request.getMergeeId())
                    .setMergeeVer(request.getMergeeVer())
                    .setBoundary(request.getBoundary())
                    .setStoreId(request.getStoreId())
                    .build())
                .build())
            .whenCompleteAsync((v, e) -> {
                if (e != null) {
                    log.debug("Failed to propose command[Merge]: \n{}", request, e);
                } else {
                    log.debug("Command[Merge] proposed: index={}\n{}", v, request);
                }
                messenger.send(KVRangeMessage.newBuilder()
                    .setRangeId(request.getMergeeId())
                    .setHostStoreId(peer)
                    .setMergeReply(MergeReply.newBuilder()
                        .setTaskId(request.getTaskId())
                        .setAccept(e == null)
                        .build())
                    .build());
            }, fsmExecutor);
    }

    private void handleCancelMergingRequest(String peer, CancelMergingRequest request) {
        log.debug("Handle CancelMerging request: \n{}", request);
        wal.propose(KVRangeCommand.newBuilder()
                .setTaskId(request.getTaskId())
                .setVer(request.getVer())
                .setCancelMerging(CancelMerging.newBuilder()
                    .build())
                .build())
            .whenCompleteAsync((v, e) -> {
                if (e != null) {
                    log.debug("Failed to propose command[CancelMerging]: \n{}", request, e);
                } else {
                    log.debug("Command[CancelMerging] proposed: index={}\n{}", v, request);
                }
                messenger.send(KVRangeMessage.newBuilder()
                    .setRangeId(request.getRequester())
                    .setHostStoreId(peer)
                    .setCancelMergingReply(CancelMergingReply.newBuilder()
                        .setTaskId(request.getTaskId())
                        .setAccept(e == null)
                        .build())
                    .build());
            }, fsmExecutor);
    }

    private void handleMergeDoneRequest(String peer, MergeDoneRequest request) {
        log.debug("Handle MergeDone request: \n{}", request);
        wal.propose(KVRangeCommand.newBuilder()
                .setTaskId(request.getTaskId())
                .setVer(request.getMergeeVer())
                .setMergeDone(MergeDone.newBuilder()
                    .setStoreId(request.getStoreId())
                    .build())
                .build())
            .whenCompleteAsync((v, e) -> {
                if (e != null) {
                    log.debug("Failed to propose command[MergeDone]: \n{}", request, e);
                } else {
                    log.debug("Command[MergeDone] proposed: index={}\n{}", v, request);
                }
                messenger.send(KVRangeMessage.newBuilder()
                    .setRangeId(request.getId())
                    .setHostStoreId(peer)
                    .setMergeDoneReply(MergeDoneReply.newBuilder()
                        .setTaskId(request.getTaskId())
                        .setAccept(e == null)
                        .build())
                    .build());
            }, fsmExecutor);
    }

    private Any reset(Boundary boundary) {
        long stamp = resetLock.writeLock();
        try {
            return coProc.reset(boundary);
        } finally {
            resetLock.unlockWrite(stamp);
        }
    }

    enum Lifecycle {
        Init, // initialized but not open
        Opening,
        Open, // accepting operations, handling incoming messages, generate out-going messages
        Closing,
        Closed, // wait for tick activity stopped
        Destroying,
        Destroyed
    }

    /**
     * Callback for listening the quit signal which generated as the result of config change operation.
     */
    public interface QuitListener {
        void onQuit(IKVRangeFSM rangeToQuit);
    }
}
