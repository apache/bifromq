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
import static org.apache.bifromq.basekv.store.util.VerUtil.bump;
import static org.apache.bifromq.basekv.store.util.VerUtil.print;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.NULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.canCombine;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.combine;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.endKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.isSplittable;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.startKey;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableSource;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.bifromq.base.util.AsyncRetry;
import org.apache.bifromq.base.util.AsyncRunner;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.baseenv.ZeroCopyParser;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.CancelMerging;
import org.apache.bifromq.basekv.proto.CancelMergingReply;
import org.apache.bifromq.basekv.proto.CancelMergingRequest;
import org.apache.bifromq.basekv.proto.ChangeConfig;
import org.apache.bifromq.basekv.proto.DataMergeRequest;
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
import org.apache.bifromq.basekv.proto.MergeHelpRequest;
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
import org.apache.bifromq.basekv.raft.proto.RaftNodeSyncState;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import org.apache.bifromq.basekv.store.api.IKVRangeCoProc;
import org.apache.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import org.apache.bifromq.basekv.store.api.IKVRangeReader;
import org.apache.bifromq.basekv.store.api.IKVRangeRefreshableReader;
import org.apache.bifromq.basekv.store.exception.KVRangeException;
import org.apache.bifromq.basekv.store.option.KVRangeOptions;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import org.apache.bifromq.basekv.store.range.hinter.IKVLoadRecord;
import org.apache.bifromq.basekv.store.range.hinter.IKVRangeSplitHinter;
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
    private final IKVRangeCoProcFactory coProcFactory;
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
    private final Subject<Boolean> queryReadySubject = BehaviorSubject.createDefault(true).toSerialized();
    private final KVRangeOptions opts;
    private final AtomicBoolean recovering = new AtomicBoolean();
    private final AtomicReference<Lifecycle> lifecycle = new AtomicReference<>(Lifecycle.Init);
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final CompletableFuture<Void> closeSignal = new CompletableFuture<>();
    private final CompletableFuture<Boolean> quitSignal = new CompletableFuture<>();
    private final CompletableFuture<Void> destroyedSignal = new CompletableFuture<>();
    private final AtomicLong lastShrinkCheckAt = new AtomicLong();
    private final AtomicBoolean shrinkingWAL = new AtomicBoolean();
    private final KVRangeMetricManager metricManager;
    private final List<IKVRangeSplitHinter> splitHinters;
    private final StampedLock resetLock = new StampedLock();
    private final String[] tags;
    private final AtomicReference<CompletableFuture<Boolean>> quitZombie = new AtomicReference<>();
    private final AtomicBoolean cancelingMerge = new AtomicBoolean();
    private volatile long mergePendingAt = -1;
    private volatile long zombieAt = -1;
    private IKVRangeMessenger messenger;
    private KVRangeRestorer restorer;

    /**
     * The constructor of KVRange FSM.
     *
     * @param clusterId the cluster id
     * @param hostStoreId the store id that hosts this range
     * @param id the range id
     * @param coProcFactory the coprocessor factory
     * @param kvRange the backing data range
     * @param walStore the backing WAL Store
     * @param queryExecutor the query executor
     * @param bgExecutor the background task executor
     * @param opts the options
     * @param quitListener the listener to be notified when this range is quitting
     */
    public KVRangeFSM(String clusterId,
                      String hostStoreId,
                      KVRangeId id,
                      IKVRangeCoProcFactory coProcFactory,
                      IKVRange kvRange,
                      IKVRangeWALStore walStore,
                      Executor queryExecutor,
                      Executor bgExecutor,
                      KVRangeOptions opts,
                      List<IKVRangeSplitHinter> hinters,
                      QuitListener quitListener,
                      String... tags) {
        this.opts = opts.toBuilder().build();
        this.id = id;
        this.hostStoreId = hostStoreId;
        this.kvRange = kvRange;
        this.tags = tags;
        this.log = MDCLogger.getLogger(KVRangeFSM.class, tags);
        this.metricManager = new KVRangeMetricManager(clusterId, hostStoreId, id);
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
        this.splitHinters = hinters;
        this.coProcFactory = coProcFactory;
        this.coProc = coProcFactory.createCoProc(clusterId, hostStoreId, id, this.kvRange::newReader);
        this.snapshotBandwidthGovernor = new SnapshotBandwidthGovernor(opts.getSnapshotSyncBytesPerSec());

        long lastAppliedIndex = this.kvRange.lastAppliedIndex().blockingFirst();
        this.linearizer = new KVRangeQueryLinearizer(wal::readIndex, queryExecutor, lastAppliedIndex,
            metricManager::recordLinearization, tags);
        this.queryRunner = new KVRangeQueryRunner(this.kvRange, coProc, queryExecutor, linearizer,
            splitHinters, this::latestDescriptor, resetLock, tags);
        this.statsCollector = new KVRangeStatsCollector(this.kvRange,
            wal, Duration.ofSeconds(opts.getStatsCollectIntervalSec()), bgExecutor);
        this.walSubscription = wal.subscribe(lastAppliedIndex,
            new IKVRangeWALSubscriber() {
                @Override
                public CompletableFuture<Void> apply(LogEntry log, boolean isLeader) {
                    return metricManager.recordLogApply(() -> KVRangeFSM.this.apply(log, isLeader));
                }

                @Override
                public CompletableFuture<Void> restore(KVRangeSnapshot snapshot, String leader,
                                                       IAfterRestoredCallback callback) {
                    return metricManager.recordSnapshotInstall(
                        () -> KVRangeFSM.this.restore(snapshot, leader, callback));
                }
            }, fsmExecutor);
        quitSignal.thenAccept(reset -> quitListener.onQuit(this, reset));
    }

    @Override
    public KVRangeId id() {
        return id;
    }

    public long ver() {
        return kvRange.currentVer();
    }

    @Override
    public Boundary boundary() {
        return kvRange.currentBoundary();
    }

    @Override
    public CompletableFuture<Void> open(IKVRangeMessenger messenger) {
        if (lifecycle.get() != Init) {
            return CompletableFuture.completedFuture(null);
        }
        return mgmtTaskRunner.add(() -> {
            if (lifecycle.compareAndSet(Init, Lifecycle.Opening)) {
                log.info("Opening range: appliedIndex={}, state={}, ver={}",
                    kvRange.currentLastAppliedIndex(),
                    kvRange.state().blockingFirst().getType(),
                    print(kvRange.currentVer()));
                this.messenger = messenger;
                factSubject.onNext(reset(kvRange.boundary().blockingFirst()));
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
                disposables.add(Observable.combineLatestArray(
                        new ObservableSource<?>[] {kvRange.ver(),
                            kvRange.state(),
                            kvRange.boundary(),
                            kvRange.clusterConfig(),
                            wal.state().distinctUntilChanged(),
                            wal.replicationStatus().distinctUntilChanged(),
                            statsCollector.collect().distinctUntilChanged(),
                            splitHintsSubject.distinctUntilChanged(),
                            factSubject.distinctUntilChanged(),
                            queryReadySubject.distinctUntilChanged()
                                .switchMap(v -> {
                                    if (v) {
                                        return Observable.timer(5, TimeUnit.SECONDS, Schedulers.from(mgmtExecutor))
                                            .map(t -> true);
                                    } else {
                                        return Observable.just(false);
                                    }
                                })
                                .distinctUntilChanged()},
                        (latest) -> {
                            long ver = (long) latest[0];
                            State state = (State) latest[1];
                            Boundary boundary = (Boundary) latest[2];
                            ClusterConfig clusterConfig = (ClusterConfig) latest[3];
                            RaftNodeStatus role = (RaftNodeStatus) latest[4];
                            @SuppressWarnings("unchecked")
                            Map<String, RaftNodeSyncState> syncStats = (Map<String, RaftNodeSyncState>) latest[5];
                            @SuppressWarnings("unchecked")
                            Map<String, Double> rangeStats = (Map<String, Double>) latest[6];
                            @SuppressWarnings("unchecked")
                            List<SplitHint> splitHints = (List<SplitHint>) latest[7];
                            Any fact = (Any) latest[8];
                            boolean readyForQuery = (boolean) latest[9];
                            log.trace("Split hints: \n{}", splitHints);
                            List<SplitHint> alignedHints = splitHints.stream().map(h -> {
                                if (h.hasSplitKey()) {
                                    return coProcFactory.toSplitKey(h.getSplitKey(), boundary)
                                        .map(k -> h.toBuilder().setSplitKey(k).build())
                                        .orElseGet(() -> h.toBuilder().clearSplitKey().build());
                                }
                                return h;
                            }).toList();
                            return KVRangeDescriptor.newBuilder()
                                .setVer(ver)
                                .setId(id)
                                .setBoundary(boundary)
                                .setRole(role)
                                .setState(state.getType())
                                .setConfig(clusterConfig)
                                .putAllSyncState(syncStats)
                                .putAllStatistics(rangeStats)
                                .addAllHints(alignedHints)
                                .setHlc(HLC.INST.get())
                                .setFact(fact)
                                .setReadyForQuery(readyForQuery)
                                .build();
                        })
                    .observeOn(Schedulers.from(mgmtExecutor))
                    .subscribe(descriptorSubject::onNext));
                disposables.add(messenger.receive().subscribe(this::handleMessage));
                disposables.add(descriptorSubject.subscribe(this::detectZombieState));
                disposables.add(wal.state()
                    .observeOn(Schedulers.from(mgmtExecutor))
                    .subscribe(role -> coProc.onLeader(role == RaftNodeStatus.Leader)));
                lifecycle.set(Open);
                metricManager.reportLastAppliedIndex(kvRange.lastAppliedIndex().blockingFirst());
                log.info("Range opened: appliedIndex={}, state={}, ver={}",
                    kvRange.currentLastAppliedIndex(),
                    kvRange.state().blockingFirst().getType(),
                    print(kvRange.currentVer()));
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
        judgeZombieState();
        estimateSplitHint();
        checkMergeTimeout();
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
                    CompletableFuture.completedFuture(null)
                        .thenComposeAsync(v -> walSubscription.stop(), mgmtExecutor)
                        .thenAcceptAsync(v -> {
                            try {
                                splitHinters.forEach(IKVRangeSplitHinter::close);
                            } catch (Throwable e) {
                                log.error("Split hinter close error", e);
                            }
                            try {
                                coProc.close();
                            } catch (Throwable e) {
                                log.error("CoProc close error", e);
                            }
                        }, mgmtExecutor)
                        .thenComposeAsync(v -> CompletableFuture.allOf(dumpSessions.values()
                            .stream()
                            .map(dumpSession -> {
                                dumpSession.cancel();
                                return dumpSession.awaitDone();
                            })
                            .toArray(CompletableFuture<?>[]::new)), mgmtExecutor)
                        .thenComposeAsync(v -> restorer.awaitDone(), mgmtExecutor)
                        .thenComposeAsync(v -> statsCollector.stop(), mgmtExecutor)
                        .thenComposeAsync(v -> mgmtTaskRunner.awaitDone(), mgmtExecutor)
                        .thenComposeAsync(v -> wal.close(), mgmtExecutor)
                        .thenComposeAsync(v -> awaitShutdown(fsmExecutor), mgmtExecutor)
                        .whenComplete((v, e) -> {
                            kvRange.close();
                            metricManager.close();
                            cmdFutures.values()
                                .forEach(
                                    f -> f.completeExceptionally(new KVRangeException.TryLater("Range closed")));
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
                .thenComposeAsync(v -> {
                    if (lifecycle.compareAndSet(Closed, Destroying)) {
                        kvRange.destroy();
                        return wal.destroy();
                    }
                    return CompletableFuture.completedFuture(null);
                }, mgmtExecutor)
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
    public CompletableFuture<Boolean> quit() {
        quitZombie.compareAndSet(null, new CompletableFuture<>());
        return quitZombie.get();
    }

    @Override
    public CompletableFuture<Void> transferLeadership(long ver, String newLeader) {
        return metricManager.recordTransferLeader(() -> {
            if (ver != kvRange.currentVer()) {
                // version not exactly match
                return CompletableFuture.failedFuture(
                    new KVRangeException.BadVersion("Version Mismatch", latestLeaderDescriptor()));
            }
            log.info("Transferring leader[ver={}, state={}]: newLeader={}",
                print(ver), kvRange.currentState().getType(), newLeader);
            return wal.transferLeadership(newLeader)
                .exceptionally(unwrap(e -> {
                    if (e instanceof LeaderTransferException.NotFoundOrQualifiedException) {
                        throw new KVRangeException.BadRequest("Failed to transfer leadership",
                            latestDescriptor(), e);
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

    private CompletableFuture<Void> changeReplicaConfig(String taskId,
                                                        long ver,
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
    public CompletableFuture<Void> merge(long ver, KVRangeId mergeeId, Set<String> mergeeVoters) {
        return metricManager.recordMerge(() -> submitManagementCommand(KVRangeCommand.newBuilder()
            .setTaskId(nextTaskId())
            .setVer(ver)
            .setPrepareMergeWith(PrepareMergeWith.newBuilder()
                .setMergeeId(mergeeId)
                .addAllVoters(mergeeVoters)
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
        if (!boundaryCompatible(mutationCommand.getVer(), kvRange.currentVer())) {
            return CompletableFuture.failedFuture(
                new KVRangeException.BadVersion("Version Mismatch", latestLeaderDescriptor()));
        }
        State state = kvRange.currentState();
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
        if (managementCommand.getVer() != kvRange.currentVer()) {
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
        return hostStoreId + "-" + id.getId() + "-" + taskSeqNo.getAndIncrement();
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

    private CompletableFuture<Void> apply(LogEntry entry, boolean isLeader) {
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        if (kvRange.currentLastAppliedIndex() > entry.getIndex()) {
            // skip already applied log
            log.debug("Skip already applied log: index={}, term={}", entry.getIndex(), entry.getTerm());
            onDone.complete(null);
            return onDone;
        }
        switch (entry.getTypeCase()) {
            case CONFIG -> {
                IKVRangeWriter<?> rangeWriter = kvRange.toWriter();
                try (IKVRangeRefreshableReader rangeReader = kvRange.newReader()) {
                    Supplier<CompletableFuture<Void>> afterLogApplied = applyConfigChange(entry.getTerm(),
                        entry.getIndex(), entry.getConfig(), rangeReader, rangeWriter);
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
                IKVLoadRecorder loadRecorder = new KVLoadRecorder();
                IKVRangeWriter<?> rangeWriter = kvRange.toWriter(loadRecorder);
                IKVRangeRefreshableReader rangeReader = new LoadRecordableKVReader(kvRange.newReader(), loadRecorder);
                try {
                    KVRangeCommand command = ZeroCopyParser.parse(entry.getData(), KVRangeCommand.parser());
                    CompletableFuture<Runnable> applyFuture = applyCommand(isLeader, entry.getTerm(), entry.getIndex(),
                        command, rangeReader, rangeWriter)
                        .whenCompleteAsync((callback, e) -> {
                            try {
                                if (onDone.isCancelled()) {
                                    rangeWriter.abort();
                                } else {
                                    try {
                                        if (e != null) {
                                            log.debug("Failed to apply log: {}", log, e);
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
                            } finally {
                                rangeReader.close();
                            }
                        }, fsmExecutor);
                    onDone.whenCompleteAsync((v, e) -> {
                        if (onDone.isCancelled()) {
                            applyFuture.cancel(true);
                        }
                    });
                } catch (Throwable t) {
                    rangeReader.close();
                    log.error("Failed to apply log: {}", log, t);
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
                                                                IKVRangeReader rangeReader,
                                                                IKVRangeWritable<?> rangeWriter) {
        long ver = rangeReader.version();
        State state = rangeReader.state();
        log.info("Apply new config[term={}, index={}]: state={}, ver={}, leader={}\n{}",
            term, index, state, print(ver), wal.isLeader(), config);
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
                rangeWriter.ver(bump(ver, false));
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
                            quitSignal.complete(false);
                            finishCommand(taskId);
                            return CompletableFuture.completedFuture(null);
                        };
                    } else {
                        rangeWriter.state(State.newBuilder()
                            .setType(Normal)
                            .setTaskId(taskId)
                            .build());
                        return () -> compactWAL().thenRunAsync(() -> finishCommand(taskId), fsmExecutor);
                    }
                } else {
                    rangeWriter.state(State.newBuilder()
                        .setType(Normal)
                        .setTaskId(taskId)
                        .build());
                    // config entry append during leader change
                    return () -> {
                        finishCommandWithError(taskId,
                            new KVRangeException.TryLater("ConfigChange aborted by leader changes"));
                        return CompletableFuture.completedFuture(null);
                    };
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
                rangeWriter.ver(bump(ver, false));
                return () -> {
                    finishCommand(taskId);
                    if (remove) {
                        quitSignal.complete(false);
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
                        quitSignal.complete(false);
                        return CompletableFuture.completedFuture(null);
                    };
                } else {
                    rangeWriter.state(State.newBuilder()
                        .setType(Normal)
                        .setTaskId(taskId)
                        .build());
                    return () -> compactWAL().thenRunAsync(() -> {
                        // purge failed due to leader change, reset back to normal
                        log.debug("Purge failed due to leader change[newConfig={}]", config);
                        finishCommand(taskId);
                    }, fsmExecutor);
                }
            }
            default -> {
                // skip internal config change triggered by leadership change, no need to compact WAL
                return () -> CompletableFuture.completedFuture(null);
            }
        }
    }

    private CompletableFuture<Runnable> applyCommand(boolean isLeader,
                                                     long logTerm,
                                                     long logIndex,
                                                     KVRangeCommand command,
                                                     IKVRangeRefreshableReader rangeReader,
                                                     IKVRangeWritable<?> rangeWriter) {
        CompletableFuture<Runnable> onDone = new CompletableFuture<>();
        long reqVer = command.getVer();
        String taskId = command.getTaskId();
        long ver = rangeReader.version();
        State state = rangeReader.state();
        Boundary boundary = rangeReader.boundary();
        ClusterConfig clusterConfig = rangeReader.clusterConfig();
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
                        rangeWriter.ver(bump(ver, false));
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
                    rangeWriter.boundary(leftBoundary).ver(bump(ver, true));
                    // migrate data to right-hand keyspace which created implicitly
                    rangeWriter.migrateTo(request.getNewId(), rhsSS);
                    onDone.complete(() -> compactWAL().whenCompleteAsync((v, e) -> {
                        if (e != null) {
                            log.error("WAL compact failed after split", e);
                            quitSignal.complete(true);
                            return;
                        }
                        log.debug("Range split completed[taskId={}]", taskId);
                        resetHinterAndCoProc(leftBoundary);
                        finishCommand(taskId);
                        messenger.once(KVRangeMessage::hasEnsureRangeReply)
                            .orTimeout(300, TimeUnit.SECONDS)
                            .whenCompleteAsync((rangeMsg, t) -> {
                                if (t != null
                                    ||
                                    rangeMsg.getEnsureRangeReply().getResult() != EnsureRangeReply.Result.OK) {
                                    log.error("Failed to load rhs range[taskId={}]: newRangeId={}",
                                        taskId, request.getNewId(), t);
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
                    }, fsmExecutor));
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
                if (!clusterConfig.getNextVotersList().isEmpty() || !clusterConfig.getNextLearnersList().isEmpty()) {
                    onDone.complete(() -> finishCommandWithError(taskId,
                        new KVRangeException.TryLater("Merge abort, range is in config changing")));
                    break;
                }

                PrepareMergeWith request = command.getPrepareMergeWith();
                if (request.getVotersList().isEmpty()) {
                    onDone.complete(() -> finishCommandWithError(taskId,
                        new KVRangeException.BadRequest("Merge abort, empty mergee voter set")));
                    break;
                }
                boolean isVoter = clusterConfig.getVotersList().contains(hostStoreId);
                if (!isVoter) {
                    rangeWriter
                        .ver(bump(ver, false))
                        .state(State.newBuilder()
                            .setType(PreparedMerging)
                            .setTaskId(taskId)
                            .build());
                    onDone.complete(NOOP);
                    break;
                }
                CompletableFuture<Void> requestFuture = trySendPrepareMergeToRequest(taskId, request.getMergeeId(), ver,
                    boundary, request.getVotersList(),
                    clusterConfig)
                    .thenAcceptAsync(v -> {
                        rangeWriter
                            .ver(bump(ver, false))
                            .state(State.newBuilder()
                                .setType(PreparedMerging)
                                .setTaskId(taskId)
                                .build());
                        onDone.complete(NOOP);
                    }, fsmExecutor);
                onDone.whenCompleteAsync((v, e) -> {
                    if (onDone.isCancelled()) {
                        requestFuture.cancel(true);
                    }
                });
            }
            case CANCELMERGING -> {
                switch (state.getType()) {
                    case PreparedMerging -> {
                        // merger cancel workflow
                        if (reqVer != ver) {
                            onDone.complete(NOOP);
                            break;
                        }
                        log.info("Merger canceled[term={}, index={}, taskId={}, ver={}, state={}]",
                            logTerm, logIndex, taskId, print(ver), state);
                        rangeWriter
                            .ver(bump(ver, false))
                            .state(State.newBuilder()
                                .setType(Normal)
                                .setTaskId(taskId)
                                .build());
                        onDone.complete(() -> compactWAL()
                            .whenCompleteAsync((v, e) -> {
                                if (e != null) {
                                    log.error("WAL compact failed after merger cancel", e);
                                }
                                finishCommandWithError(taskId, new KVRangeException.TryLater("Merger canceled"));
                            }, fsmExecutor));
                    }
                    case WaitingForMerge -> {
                        // mergee cancel workflow
                        if (reqVer != ver) {
                            onDone.complete(NOOP);
                            break;
                        }
                        log.info("Mergee canceled[term={}, index={}, taskId={}, ver={}, state={}]",
                            logTerm, logIndex, taskId, print(ver), state);
                        rangeWriter.state(State.newBuilder()
                            .setType(Normal)
                            .setTaskId(taskId)
                            .build());
                        rangeWriter.ver(bump(ver, false));
                        onDone.complete(() -> compactWAL()
                            .whenComplete((v, e) -> {
                                if (e != null) {
                                    log.error("WAL compact failed after mergee cancel", e);
                                }
                            }));
                    }
                    default -> onDone.complete(NOOP);
                }
            }
            case PREPAREMERGETO -> {
                PrepareMergeTo request = command.getPrepareMergeTo();
                // skip PrepareMergeTo command either redundant from same merger or another merger
                // which will not receive any response and cancel itself eventually
                if (state.getType() == WaitingForMerge) {
                    onDone.complete(NOOP);
                    break;
                }
                // we don't compare request ver and current ver here
                // the formal mergeable condition check
                boolean isVoter = clusterConfig.getVotersList().contains(hostStoreId);
                ClusterConfig mergerConfig = request.getConfig();
                List<String> mergerVoters = mergerConfig.getVotersList();
                if (state.getType() != Normal
                    || !clusterConfig.getNextVotersList().isEmpty() // config changing
                    || !clusterConfig.getNextLearnersList().isEmpty() // config changing
                    || !canCombine(request.getBoundary(), boundary)) {
                    if (!isVoter) {
                        rangeWriter.ver(bump(ver, false));
                        onDone.complete(NOOP);
                        break;
                    }
                    CompletableFuture<Void> requestFuture = trySendCancelMergingRequest(taskId, request.getMergerId(),
                        request.getMergerVer(), mergerVoters,
                        clusterConfig.getVotersList())
                        .whenCompleteAsync((reply, e) -> {
                            rangeWriter.ver(bump(ver, false));
                            onDone.complete(NOOP);
                        }, fsmExecutor);
                    onDone.whenCompleteAsync((v, e) -> {
                        if (onDone.isCancelled()) {
                            requestFuture.cancel(true);
                        }
                    });
                    break;
                }
                // merge condition met
                if (!isVoter) {
                    rangeWriter
                        .ver(bump(ver, false))
                        .state(State.newBuilder()
                            .setType(WaitingForMerge)
                            .setTaskId(taskId)
                            .build());
                    onDone.complete(NOOP);
                    break;
                }
                CompletableFuture<Void> requestFuture = trySendMergeRequest(taskId, request.getMergerId(),
                    request.getMergerVer(), ver, boundary, mergerVoters, clusterConfig)
                    .thenAcceptAsync(v -> {
                        rangeWriter
                            .ver(bump(ver, false))
                            .state(State.newBuilder()
                                .setType(WaitingForMerge)
                                .setTaskId(taskId)
                                .build());
                        onDone.complete(NOOP);
                    }, fsmExecutor);
                onDone.whenCompleteAsync((v, e) -> {
                    if (onDone.isCancelled()) {
                        requestFuture.cancel(true);
                    }
                });
            }
            case MERGE -> {
                Merge request = command.getMerge();
                switch (state.getType()) {
                    case PreparedMerging -> {
                        if (reqVer != ver) {
                            onDone.complete(NOOP);
                            break;
                        }
                        KVRangeId mergeeId = request.getMergeeId();
                        ClusterConfig mergeeConfig = request.getConfig();
                        boolean isVoter = clusterConfig.getVotersList().contains(hostStoreId);
                        List<String> mergeeReplicas = new ArrayList<>(mergeeConfig.getVotersList());
                        mergeeReplicas.addAll(mergeeConfig.getLearnersList());
                        IKVRangeWritable.Migrater migrater = rangeWriter.startMerging(((count, bytes) -> {
                            mergePendingAt = -1;
                            log.info("Merging data from mergee: taskId={}, received entries={}, bytes={}",
                                taskId, count, bytes);
                        }));
                        CompletableFuture<Void> migrateFuture = tryMigrate(mergeeId, mergeeReplicas,
                            clusterConfig.getVotersList(), migrater)
                            .thenComposeAsync(result -> {
                                switch (result) {
                                    case SUCCESS_AFTER_RESET -> {
                                        return CompletableFuture.completedFuture(() -> {
                                            // quit and restore from leader
                                            log.debug("Restore from leader: mergeeId={}",
                                                KVRangeIdUtil.toString(mergeeId));
                                            quitSignal.complete(true);
                                        });
                                    }
                                    case SUCCESS -> {
                                        return tryConfirmMerged(taskId, ver)
                                            .thenComposeAsync(mergeResult -> {
                                                if (mergeResult == TryConfirmMergedResult.ALREADY_CANCELED) {
                                                    // rollback
                                                    migrater.abort();
                                                    return CompletableFuture.completedFuture(
                                                        TryConfirmMergedResult.ALREADY_CANCELED);
                                                }
                                                if (isVoter) {
                                                    return trySendMergeDoneRequest(taskId, mergeeId,
                                                        request.getMergeeVer(), mergeeConfig.getVotersList(),
                                                        clusterConfig.getVotersList())
                                                        .thenApply(v -> TryConfirmMergedResult.MERGED);
                                                }
                                                return CompletableFuture.completedFuture(TryConfirmMergedResult.MERGED);
                                            }, fsmExecutor)
                                            .thenApplyAsync(mergeResult -> {
                                                if (mergeResult == TryConfirmMergedResult.ALREADY_CANCELED) {
                                                    migrater.abort();
                                                    // leave state & ver unchanged
                                                    return NOOP;
                                                } else {
                                                    long newVer = Math.max(ver, request.getMergeeVer());
                                                    Boundary mergedBoundary = combine(boundary, request.getBoundary());
                                                    migrater
                                                        .ver(VerUtil.bump(newVer, true))
                                                        .boundary(mergedBoundary)
                                                        .state(State.newBuilder()
                                                            .setType(Normal)
                                                            .setTaskId(taskId)
                                                            .build());
                                                    return () -> {
                                                        log.info("Merger done[term={}, index={}, taskId={}, "
                                                                + "ver={}, state={}]: mergeeId={}, boundary={}",
                                                            logTerm, logIndex, taskId, print(ver), state,
                                                            KVRangeIdUtil.toString(request.getMergeeId()),
                                                            mergedBoundary);
                                                        compactWAL()
                                                            .whenCompleteAsync((v, t) -> {
                                                                if (t != null) {
                                                                    log.error("WAL compact failed after merge", t);
                                                                }
                                                                resetHinterAndCoProc(mergedBoundary);
                                                                finishCommand(taskId);
                                                            }, fsmExecutor);
                                                    };
                                                }
                                            }, fsmExecutor);
                                    }
                                    default -> {
                                        // retry failed
                                        return tryCancelMerging(taskId, ver)
                                            .thenApplyAsync(cancelMergingResult -> {
                                                if (cancelMergingResult == TryCancelMergingResult.CANCELLED) {
                                                    return NOOP;
                                                }
                                                // merged
                                                return (Runnable) () -> {
                                                    log.info("Merge confirmed, restore from leader: mergeeId={}",
                                                        KVRangeIdUtil.toString(mergeeId));
                                                    quitSignal.complete(true);
                                                };
                                            }, fsmExecutor);
                                    }
                                }
                            }, fsmExecutor)
                            .thenAccept(onDone::complete);
                        onDone.whenCompleteAsync((v, e) -> {
                            if (onDone.isCancelled()) {
                                migrater.abort();
                                migrateFuture.cancel(true);
                            }
                        });
                    }
                    case Normal -> {
                        if (VerUtil.boundaryCompatible(reqVer, ver)) {
                            // Cancel happens before Merge command is applied
                            if (!clusterConfig.getVotersList().contains(hostStoreId)) {
                                onDone.complete(NOOP);
                                break;
                            }
                            CompletableFuture<Void> requestFuture = trySendCancelMergingRequest(taskId,
                                request.getMergeeId(), request.getMergeeVer(),
                                request.getConfig().getVotersList(), clusterConfig.getVotersList())
                                .whenCompleteAsync((reply, e) -> {
                                    onDone.complete(NOOP);
                                }, fsmExecutor);
                            onDone.whenCompleteAsync((v, e) -> {
                                if (onDone.isCancelled()) {
                                    requestFuture.cancel(true);
                                }
                            });
                        } else {
                            // merge has been done, ignore late Merge
                            log.debug("Late Merge command ignored: mergeeId={}",
                                KVRangeIdUtil.toString(request.getMergeeId()));
                            onDone.complete(NOOP);
                        }
                    }
                    default -> onDone.complete(NOOP); // ignore the Merge command in other states
                }
            }
            case MERGEDONE -> {
                if (state.getType() != WaitingForMerge || reqVer != ver) {
                    onDone.complete(NOOP);
                    break;
                }
                log.info("Mergee done[term={}, index={}, taskId={}, ver={}, state={}]: mergeeId={}",
                    logTerm, logIndex, taskId, print(ver), state, KVRangeIdUtil.toString(id));
                rangeWriter.boundary(NULL_BOUNDARY)
                    .ver(bump(ver, true))
                    .state(State.newBuilder()
                        .setType(Merged)
                        .setTaskId(taskId)
                        .build());
                onDone.complete(() -> compactWAL()
                    .whenCompleteAsync((v, e) -> {
                        if (e != null) {
                            log.error("WAL compact failed after merge", e);
                            quitSignal.complete(true);
                            return;
                        }
                        resetHinterAndCoProc(NULL_BOUNDARY);
                    }, fsmExecutor));
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
                            Preconditions.checkArgument(BoundaryUtil.inRange(delete.getKey(), boundary));
                            Optional<ByteString> value = rangeReader.get(delete.getKey());
                            if (value.isPresent()) {
                                rangeWriter.kvWriter().delete(delete.getKey());
                            }
                            onDone.complete(() -> finishCommand(taskId, value.orElse(ByteString.EMPTY)));
                        }
                        case PUT -> {
                            Put put = command.getPut();
                            Preconditions.checkArgument(BoundaryUtil.inRange(put.getKey(), boundary));
                            Optional<ByteString> value = rangeReader.get(put.getKey());
                            rangeWriter.kvWriter().put(put.getKey(), put.getValue());
                            onDone.complete(() -> finishCommand(taskId, value.orElse(ByteString.EMPTY)));
                        }
                        case RWCOPROC -> {
                            Supplier<IKVRangeCoProc.MutationResult> resultSupplier =
                                coProc.mutate(command.getRwCoProc(), rangeReader, rangeWriter.kvWriter(), isLeader);
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

    private String randomPickOne(List<String> remoteVoters, List<String> localVoters) {
        if (remoteVoters.contains(hostStoreId)) {
            return hostStoreId;
        }
        Set<String> shared = Sets.intersection(Sets.newHashSet(remoteVoters), Sets.newHashSet(localVoters));
        if (shared.isEmpty()) {
            return remoteVoters.get(ThreadLocalRandom.current().nextInt(remoteVoters.size()));
        }
        return shared.iterator().next();
    }

    private CompletableFuture<TryMigrateResult> tryMigrate(KVRangeId mergeeId,
                                                           List<String> mergeeReplicas,
                                                           List<String> mergerVoters,
                                                           IKVRangeWritable.Migrater migrater) {
        String sessionId = UUID.randomUUID().toString();
        String mergeeVoter = randomPickOne(mergeeReplicas, mergerVoters);
        IKVRangeSnapshotReceiver receiver = new KVRangeSnapshotReceiver(sessionId, mergeeId,
            mergeeVoter, messenger, metricManager, fsmExecutor, Math.max(1, opts.getMergeTimeoutSec() / 4), log);
        log.debug("Start migrating data from mergee: mergeeId={}, store={}, session={}",
            KVRangeIdUtil.toString(mergeeId), mergeeVoter, sessionId);
        CompletableFuture<TryMigrateResult> migrateTask = receiver.start(migrater::put)
            .thenCompose(result -> {
                switch (result.code()) {
                    case TIME_OUT, NOT_FOUND, ERROR -> {
                        // restore failed, abort and check if merge has done
                        migrater.abort();
                        log.debug("Migration failed: mergeeId={}, store={}, result={}",
                            KVRangeIdUtil.toString(mergeeId), mergeeVoter, result);
                        return wal.retrieveCommitted(kvRange.currentLastAppliedIndex() + 1, Long.MAX_VALUE)
                            .handle((logEntryItr, e) -> {
                                if (e != null) {
                                    log.error("Failed to retrieve log from wal from index[{}]",
                                        kvRange.currentLastAppliedIndex() + 1, e);
                                    return TryMigrateResult.FAILED;
                                }
                                boolean migrationDone = false;
                                while (logEntryItr.hasNext()) {
                                    LogEntry logEntry = logEntryItr.next();
                                    if (logEntry.hasData()) {
                                        try {
                                            KVRangeCommand command = ZeroCopyParser.parse(logEntry.getData(),
                                                KVRangeCommand.parser());
                                            if (command.hasMergeDone()) {
                                                log.debug("Merge has done by {}",
                                                    command.getMergeDone().getStoreId());
                                                migrationDone = true;
                                                break;
                                            }
                                        } catch (Throwable t) {
                                            // should not happen
                                            log.error("Failed to parse logEntry", t);
                                            break;
                                        }
                                    }
                                }
                                logEntryItr.close();
                                return migrationDone
                                    ? TryMigrateResult.SUCCESS_AFTER_RESET : TryMigrateResult.FAILED;
                            });
                    }
                    default -> {
                        log.debug("Migration completed: mergeeId={}, store={}",
                            KVRangeIdUtil.toString(mergeeId), mergeeVoter);
                        return CompletableFuture.completedFuture(TryMigrateResult.SUCCESS);
                    }
                }
            });
        messenger.send(KVRangeMessage.newBuilder()
            .setRangeId(mergeeId)
            .setHostStoreId(mergeeVoter)
            .setDataMergeRequest(DataMergeRequest.newBuilder()
                .setSessionId(sessionId)
                .setMergerId(id)
                .build())
            .build());
        return migrateTask;
    }

    private CompletableFuture<TryCancelMergingResult> tryCancelMerging(String taskId, long ver) {
        Supplier<CompletableFuture<Long>> proposeCancelTask = () -> wal.propose(KVRangeCommand.newBuilder()
            .setTaskId(taskId)
            .setVer(ver)
            .setCancelMerging(CancelMerging.newBuilder().build())
            .build());
        // retry propose until success, the quorum must be hold to process
        return AsyncRetry.exec(proposeCancelTask, (index, e) -> e != null,
                Duration.ofSeconds(5).toNanos(), Long.MAX_VALUE)
            .thenComposeAsync((index) -> wal.once(kvRange.currentLastAppliedIndex(), (logEntry) -> {
                if (logEntry.hasData()) {
                    try {
                        KVRangeCommand command = ZeroCopyParser.parse(logEntry.getData(), KVRangeCommand.parser());
                        if (command.hasMergeDone() || command.hasCancelMerging()) {
                            return true;
                        }
                    } catch (Throwable t) {
                        // should not happen
                        log.error("Failed to parse logEntry", t);
                    }
                }
                return false;
            }, fsmExecutor), fsmExecutor)
            .thenApply(logEntry -> {
                try {
                    KVRangeCommand command = ZeroCopyParser.parse(logEntry.getData(), KVRangeCommand.parser());
                    return command.hasCancelMerging() ? TryCancelMergingResult.CANCELLED :
                        TryCancelMergingResult.ALREADY_MERGED;
                } catch (Throwable t) {
                    throw new KVRangeException("Should never happen", t);
                }
            });
    }

    private CompletableFuture<TryConfirmMergedResult> tryConfirmMerged(String taskId, long ver) {
        Supplier<CompletableFuture<Long>> proposeCancelTask = () -> wal.propose(KVRangeCommand.newBuilder()
            .setTaskId(taskId)
            .setVer(ver)
            .setMergeDone(MergeDone.newBuilder().setStoreId(hostStoreId).build())
            .build());
        // retry propose until success, the quorum must be hold to process
        return AsyncRetry.exec(proposeCancelTask, (index, e) -> e != null,
                Duration.ofSeconds(5).toNanos(), Long.MAX_VALUE)
            .thenComposeAsync((index) -> wal.once(kvRange.currentLastAppliedIndex(), (logEntry) -> {
                if (logEntry.hasData()) {
                    try {
                        KVRangeCommand command = ZeroCopyParser.parse(logEntry.getData(), KVRangeCommand.parser());
                        if (command.hasMergeDone() || command.hasCancelMerging()) {
                            return true;
                        }
                    } catch (Throwable t) {
                        // should not happen
                        log.error("Failed to parse logEntry", t);
                    }
                }
                return false;
            }, fsmExecutor), fsmExecutor)
            .thenApply(logEntry -> {
                try {
                    KVRangeCommand command = ZeroCopyParser.parse(logEntry.getData(), KVRangeCommand.parser());
                    return command.hasCancelMerging() ? TryConfirmMergedResult.ALREADY_CANCELED :
                        TryConfirmMergedResult.MERGED;
                } catch (Throwable t) {
                    throw new KVRangeException("Should never happen", t);
                }
            });
    }

    private CompletableFuture<Void> trySendPrepareMergeToRequest(String taskId,
                                                                 KVRangeId mergeeId,
                                                                 long mergerVer,
                                                                 Boundary mergerBoundary,
                                                                 List<String> mergeeVoters,
                                                                 ClusterConfig mergerConfig) {
        Supplier<CompletableFuture<PrepareMergeToReply>> sendTask = () -> {
            String mergeeVoter = randomPickOne(mergeeVoters, mergerConfig.getVotersList());
            CompletableFuture<PrepareMergeToReply> replyFuture = messenger.once(m -> m.hasPrepareMergeToReply()
                    && m.getRangeId().equals(mergeeId)
                    && m.getPrepareMergeToReply().getTaskId().equals(taskId))
                .orTimeout(1, TimeUnit.SECONDS)
                .thenApply(KVRangeMessage::getPrepareMergeToReply)
                .exceptionally(e -> PrepareMergeToReply.newBuilder()
                    .setTaskId(taskId)
                    .setAccept(false)
                    .build());
            log.debug("Send PrepareMergeTo request: mergeeId={}, mergeeStore={}", KVRangeIdUtil.toString(mergeeId),
                mergeeVoter);
            messenger.send(KVRangeMessage.newBuilder()
                .setHostStoreId(mergeeVoter)
                .setRangeId(mergeeId)
                .setPrepareMergeToRequest(PrepareMergeToRequest.newBuilder()
                    .setTaskId(taskId)
                    .setId(id)
                    .setVer(VerUtil.bump(mergerVer, false))
                    .setBoundary(mergerBoundary)
                    .setConfig(mergerConfig)
                    .build())
                .build());
            return replyFuture;
        };
        return AsyncRetry.exec(sendTask, (reply, e) -> !reply.getAccept(),
                Duration.ofSeconds(opts.getMergeTimeoutSec() / 2).toNanos(),
                Duration.ofSeconds(opts.getMergeTimeoutSec()).toNanos())
            .handle((reply, e) -> {
                if (e != null) {
                    log.warn("Failed to send PrepareMergeTo request: mergeeId={}", KVRangeIdUtil.toString(mergeeId), e);
                } else if (!reply.getAccept()) {
                    log.debug("Mergee rejected PrepareMergeTo request: mergeeId={} ", KVRangeIdUtil.toString(mergeeId));
                } else {
                    log.debug("Mergee accepted PrepareMergeTo request: mergeeId={} ", KVRangeIdUtil.toString(mergeeId));
                }
                return null;
            });
    }

    private CompletableFuture<Void> trySendMergeDoneRequest(String taskId,
                                                            KVRangeId mergeeId,
                                                            long mergeeVer,
                                                            List<String> mergeeVoters,
                                                            List<String> mergerVoters) {
        Supplier<CompletableFuture<MergeDoneReply>> sendTask = () -> {
            String mergeeVoter = randomPickOne(mergeeVoters, mergerVoters);
            CompletableFuture<MergeDoneReply> replyFuture = messenger.once(m -> m.hasMergeDoneReply()
                    && m.getRangeId().equals(mergeeId)
                    && m.getMergeDoneReply().getTaskId().equals(taskId))
                .thenApply(KVRangeMessage::getMergeDoneReply)
                .orTimeout(1, TimeUnit.SECONDS)
                .exceptionally(e -> MergeDoneReply.newBuilder().setTaskId(taskId).setAccept(false).build());
            log.debug("Send MergeDone request: mergeeId={}, mergeeStore={}",
                KVRangeIdUtil.toString(mergeeId), mergeeVoter);
            messenger.send(KVRangeMessage.newBuilder()
                .setRangeId(mergeeId)
                .setHostStoreId(mergeeVoter)
                .setMergeDoneRequest(MergeDoneRequest.newBuilder()
                    .setId(id)
                    .setTaskId(taskId)
                    .setMergeeVer(mergeeVer)
                    .setStoreId(hostStoreId)
                    .build())
                .build());
            return replyFuture;
        };
        return AsyncRetry.exec(sendTask, (reply, e) -> !reply.getAccept(),
                Duration.ofSeconds(opts.getMergeTimeoutSec() / 2).toNanos(),
                Duration.ofSeconds(opts.getMergeTimeoutSec()).toNanos())
            .handle((reply, e) -> {
                if (e != null) {
                    log.warn("Failed to send MergeDone request: mergeeId={}", KVRangeIdUtil.toString(mergeeId), e);
                } else if (!reply.getAccept()) {
                    log.debug("Mergee rejected MergeDone request: mergeeId={} ", KVRangeIdUtil.toString(mergeeId));
                } else {
                    log.debug("Mergee accepted MergeDone request: mergeeId={} ", KVRangeIdUtil.toString(mergeeId));
                }
                return null;
            });
    }

    private CompletableFuture<Void> trySendCancelMergingRequest(String taskId,
                                                                KVRangeId remoteRangeId,
                                                                long remoteRangeVer,
                                                                List<String> remoteRangeVoters,
                                                                List<String> localVoters) {
        Supplier<CompletableFuture<CancelMergingReply>> sendCancelRequestTask = () -> {
            String mergerVoter = randomPickOne(remoteRangeVoters, localVoters);
            CompletableFuture<CancelMergingReply> cancelReplyFuture = messenger.once(m -> m.hasCancelMergingReply()
                    && m.getRangeId().equals(remoteRangeId)
                    && m.getCancelMergingReply().getTaskId().equals(taskId))
                .orTimeout(1, TimeUnit.SECONDS)
                .thenApply(KVRangeMessage::getCancelMergingReply)
                .exceptionally(v -> CancelMergingReply.newBuilder().setTaskId(taskId).setAccept(false).build());
            log.debug("Send CancelMerging request: remoteRangeId={}, storeId={}", KVRangeIdUtil.toString(remoteRangeId),
                mergerVoter);
            messenger.send(KVRangeMessage.newBuilder()
                .setRangeId(remoteRangeId)
                .setHostStoreId(mergerVoter)
                .setCancelMergingRequest(CancelMergingRequest.newBuilder()
                    .setTaskId(taskId)
                    .setVer(remoteRangeVer)
                    .setRequester(id)
                    .build())
                .build());
            return cancelReplyFuture;
        };
        return AsyncRetry.exec(sendCancelRequestTask, (reply, e) -> !reply.getAccept(),
                Duration.ofSeconds(opts.getMergeTimeoutSec() / 2).toNanos(),
                Duration.ofSeconds(opts.getMergeTimeoutSec()).toNanos())
            .handle((reply, e) -> {
                if (e != null) {
                    log.warn("Failed to send CancelMerging request: remoteRangeId={}",
                        KVRangeIdUtil.toString(remoteRangeId), e);
                } else if (!reply.getAccept()) {
                    log.debug("Mergee rejected CancelMerging request: remoteRangeId={} ",
                        KVRangeIdUtil.toString(remoteRangeId));
                } else {
                    log.debug("Mergee accepted CancelMerging request: remoteRangeId={} ",
                        KVRangeIdUtil.toString(remoteRangeId));
                }
                return null;
            });
    }

    private CompletableFuture<Void> trySendMergeRequest(String taskId,
                                                        KVRangeId mergerId,
                                                        long mergerVer,
                                                        long mergeeVer,
                                                        Boundary boundary,
                                                        List<String> mergerVoters,
                                                        ClusterConfig mergeeConfig) {
        Supplier<CompletableFuture<MergeReply>> sendMergeRequestTask = () -> {
            String mergerVoter = randomPickOne(mergerVoters, mergeeConfig.getVotersList());
            CompletableFuture<MergeReply> replyFuture = messenger.once(m -> m.hasMergeReply()
                    && m.getRangeId().equals(mergerId)
                    && m.getMergeReply().getTaskId().equals(taskId))
                .orTimeout(1, TimeUnit.SECONDS)
                .thenApply(KVRangeMessage::getMergeReply)
                .exceptionally(e -> MergeReply.newBuilder().setTaskId(taskId).setAccept(false).build());
            log.debug("Send Merge request: mergerId={}, storeId={}", KVRangeIdUtil.toString(mergerId), mergerVoter);
            messenger.send(KVRangeMessage.newBuilder()
                .setHostStoreId(mergerVoter)
                .setRangeId(mergerId)
                .setMergeRequest(MergeRequest.newBuilder()
                    .setTaskId(taskId)
                    .setVer(mergerVer)
                    .setMergeeId(id)
                    .setMergeeVer(VerUtil.bump(mergeeVer, false))
                    .setStoreId(hostStoreId)
                    .setBoundary(boundary)
                    .setConfig(mergeeConfig)
                    .build())
                .build());
            return replyFuture;
        };
        return AsyncRetry.exec(sendMergeRequestTask, (reply, e) -> !reply.getAccept(),
                Duration.ofSeconds(opts.getMergeTimeoutSec() / 2).toNanos(),
                Duration.ofSeconds(opts.getMergeTimeoutSec()).toNanos())
            .handle((reply, e) -> {
                if (e != null) {
                    log.warn("Failed to send Merge request: mergerId={}", KVRangeIdUtil.toString(mergerId), e);
                } else if (!reply.getAccept()) {
                    log.debug("Mergee rejected Merge request: mergerId={} ", KVRangeIdUtil.toString(mergerId));
                } else {
                    log.debug("Merger accepted Merge request: mergerId={} ", KVRangeIdUtil.toString(mergerId));
                }
                return null;
            });
    }

    private void resetHinterAndCoProc(Boundary boundary) {
        try {
            splitHinters.forEach(hinter -> hinter.reset(boundary));
            factSubject.onNext(reset(boundary));
        } catch (Throwable ex) {
            log.error("Failed to reset hinter or coProc after boundary change", ex);
        }
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

    private KVRangeDescriptor latestDescriptor() {
        return descriptorSubject.getValue();
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
            if (wal.logDataSize() < opts.getCompactWALThreshold()) {
                shrinkingWAL.set(false);
                return;
            }
            mgmtTaskRunner.add(() -> {
                if (isNotOpening() || kvRange.currentState().getType() == ConfigChanging) {
                    // don't let compaction interferes with config changing process
                    shrinkingWAL.set(false);
                    return CompletableFuture.completedFuture(null);
                }
                KVRangeSnapshot latestSnapshot = wal.latestSnapshot();
                long lastAppliedIndex = kvRange.currentLastAppliedIndex();
                if (wal.logDataSize() < opts.getCompactWALThreshold()) {
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

    private void judgeZombieState() {
        CompletableFuture<Boolean> checkFuture = quitZombie.getAndSet(null);
        if (zombieAt > 0
            && Duration.ofMillis(HLC.INST.getPhysical() - zombieAt).toSeconds() > opts.getZombieTimeoutSec()) {
            ClusterConfig clusterConfig = wal.latestClusterConfig();
            if (clusterConfig.getNextVotersCount() > 0
                && clusterConfig.getVotersList().equals(singletonList(hostStoreId))) {
                // recover from single voter change lost quorum
                if (recovering.compareAndSet(false, true)) {
                    log.info("Recovering from lost quorum during changing config from single voter: \n{}",
                        clusterConfig);
                    wal.recover().whenComplete((v, e) -> {
                        recovering.set(false);
                        checkFuture.complete(false);
                    });
                } else {
                    checkFuture.complete(false);
                }
            } else {
                if (checkFuture != null) {
                    log.info("Zombie state detected, send quit signal.");
                    quitSignal.complete(false);
                    checkFuture.complete(true);
                }
            }
        } else if (checkFuture != null) {
            checkFuture.complete(false);
        }
    }

    private void checkMergeTimeout() {
        State state = kvRange.currentState();
        switch (state.getType()) {
            case PreparedMerging -> {
                if (mergePendingAt < 0) {
                    mergePendingAt = System.nanoTime();
                } else {
                    boolean timeout = Duration.ofSeconds(opts.getMergeTimeoutSec())
                        .compareTo(Duration.ofNanos(System.nanoTime() - mergePendingAt)) <= 0;
                    if (timeout) {
                        if (wal.isLeader() && cancelingMerge.compareAndSet(false, true)) {
                            log.debug("Merge timeout, auto cancel merging: mergeeId={}", KVRangeIdUtil.toString(id));
                            wal.propose(KVRangeCommand.newBuilder()
                                    .setTaskId(state.getTaskId())
                                    .setVer(kvRange.currentVer())
                                    .setCancelMerging(CancelMerging.newBuilder().build())
                                    .build())
                                .thenRun(() -> mergePendingAt = -1)
                                .whenComplete((logIdx, e) -> cancelingMerge.set(false));
                        }
                    }
                }
            }
            case WaitingForMerge -> {
                if (mergePendingAt < 0) {
                    mergePendingAt = System.nanoTime();
                } else {
                    boolean timeout = Duration.ofSeconds(opts.getMergeTimeoutSec())
                        .compareTo(Duration.ofNanos(System.nanoTime() - mergePendingAt)) <= 0;
                    if (timeout) {
                        if (wal.isLeader()) {
                            log.debug("Merge timeout, broadcast merge help: mergeeId={}", KVRangeIdUtil.toString(id));
                            messenger.send(KVRangeMessage.newBuilder()
                                .setMergeHelpRequest(MergeHelpRequest.newBuilder()
                                    .setTaskId(state.getTaskId())
                                    .setMergeeId(id)
                                    .setVer(kvRange.currentVer())
                                    .setBoundary(boundary())
                                    .setConfig(wal.latestClusterConfig())
                                    .build())
                                .build());
                            mergePendingAt = -1;
                        }
                    }
                }
            }
            default -> mergePendingAt = -1;
        }
    }

    private boolean isNotOpening() {
        Lifecycle state = lifecycle.get();
        return state != Open;
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
            case DATAMERGEREQUEST -> handleDataMergeRequest(message.getHostStoreId(), message.getDataMergeRequest());
            case MERGEHELPREQUEST -> handleMergeHelpRequest(message.getHostStoreId(), message.getMergeHelpRequest());
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
        startDumpSession(request.getSessionId(), request.getSnapshot(), request.getSnapshot().getId(), follower);
    }

    private void handlePrepareMergeToRequest(String peer, PrepareMergeToRequest request) {
        log.debug("Handle PrepareMergeTo request \n{}", request);
        // I'm the mergee
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();
        disposables.add(descriptorSubject.firstElement()
            .observeOn(Schedulers.from(mgmtExecutor))
            .doOnSubscribe(disposableRef::set)
            .doOnDispose(() -> {
                Disposable disposable = disposableRef.get();
                if (disposable != null) {
                    disposables.delete(disposableRef.get());
                }
            })
            .subscribe(latestDesc ->
                wal.propose(KVRangeCommand.newBuilder()
                        .setTaskId(request.getTaskId())
                        .setVer(latestDesc.getVer()) // use current fsm ver, may be mismatched when applied
                        .setPrepareMergeTo(PrepareMergeTo.newBuilder()
                            .setMergerId(request.getId())
                            .setMergerVer(request.getVer())
                            .setBoundary(request.getBoundary())
                            .setConfig(request.getConfig())
                            .build())
                        .build())
                    .whenCompleteAsync((proposalIndex, e) -> {
                        if (e != null) {
                            log.debug("Failed to propose command[PrepareMergeTo]: \n{}", request, e);
                        } else {
                            log.debug("Command[PrepareMergeTo] proposed: index={}\n{}", proposalIndex, request);
                        }
                        messenger.send(KVRangeMessage.newBuilder()
                            .setRangeId(request.getId())
                            .setHostStoreId(peer)
                            .setPrepareMergeToReply(PrepareMergeToReply.newBuilder()
                                .setTaskId(request.getTaskId())
                                .setAccept(e == null)
                                .build())
                            .build());
                    }, fsmExecutor)));
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
                    .setConfig(request.getConfig())
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

    private void handleDataMergeRequest(String peer, DataMergeRequest request) {
        log.debug("Handle DataMerge request: \n{}", request);
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();
        disposables.add(descriptorSubject
            .filter(desc -> desc.getState() == WaitingForMerge)
            .firstElement()
            .observeOn(Schedulers.from(mgmtExecutor))
            .doOnSubscribe(disposableRef::set)
            .doOnDispose(() -> {
                Disposable disposable = disposableRef.get();
                if (disposable != null) {
                    disposables.delete(disposableRef.get());
                }
            })
            .subscribe(desc -> {
                KVRangeSnapshot checkpoint = kvRange.checkpoint();
                startDumpSession(request.getSessionId(), checkpoint, request.getMergerId(), peer);
            }));
    }

    private void handleMergeHelpRequest(String peer, MergeHelpRequest request) {
        log.debug("Handle MergeHelp request: \n{}", request);
        KVRangeId mergeeId = request.getMergeeId();
        Boundary mergeeBoundary = request.getBoundary();
        Boundary myBoundary = boundary();
        ByteString myEndKey = endKey(myBoundary);
        ByteString mergeeStartKey = startKey(mergeeBoundary);
        // handle help request only when I'm in Normal state and the request is in same epoch
        if (kvRange.currentState().getType() == Normal && mergeeId.getEpoch() == id.getEpoch()) {
            if (Objects.equals(myEndKey, mergeeStartKey)) {
                log.debug("help mergee cancel: mergeeId={}", KVRangeIdUtil.toString(request.getMergeeId()));
                messenger.send(KVRangeMessage.newBuilder()
                    .setRangeId(mergeeId)
                    .setHostStoreId(peer)
                    .setCancelMergingRequest(CancelMergingRequest.newBuilder()
                        .setTaskId(request.getTaskId())
                        .setVer(request.getVer())
                        .setRequester(id)
                        .build())
                    .build());
            } else if (BoundaryUtil.inRange(mergeeBoundary, myBoundary)) {
                log.debug("help mergee finish: mergeeId={}", KVRangeIdUtil.toString(request.getMergeeId()));
                messenger.send(KVRangeMessage.newBuilder()
                    .setRangeId(mergeeId)
                    .setHostStoreId(peer)
                    .setMergeDoneRequest(MergeDoneRequest.newBuilder()
                        .setId(id)
                        .setTaskId(request.getTaskId())
                        .setMergeeVer(request.getVer())
                        .setStoreId(hostStoreId)
                        .build())
                    .build());
            }
        }
    }

    private void startDumpSession(String sessionId,
                                  KVRangeSnapshot snapshot,
                                  KVRangeId targetRangeId,
                                  String targetStoreId) {
        KVRangeDumpSession session = new KVRangeDumpSession(sessionId, snapshot, targetRangeId, targetStoreId,
            kvRange, messenger, Duration.ofSeconds(opts.getSnapshotSyncIdleTimeoutSec()),
            opts.getSnapshotSyncBytesPerSec(),
            snapshotBandwidthGovernor,
            bytes -> {
                // reset merge timeout timer if there is a live data dump activity
                if (kvRange.currentState().getType() == WaitingForMerge) {
                    mergePendingAt = -1;
                }
                metricManager.reportDump(bytes);
            }, tags);
        dumpSessions.put(session.id(), session);
        session.awaitDone().whenComplete((result, e) -> {
            switch (result) {
                case OK -> log.info("Snapshot dumped: session={}, follower={}", session.id(), targetStoreId);
                case Canceled ->
                    log.info("Snapshot dump canceled: session={}, follower={}", session.id(), targetStoreId);
                case NoCheckpoint -> {
                    log.info("No checkpoint found, compact WAL now");
                    compactWAL();
                }
                case Abort -> log.info("Snapshot dump aborted: session={}, follower={}", session.id(), targetStoreId);
                case Error -> log.warn("Snapshot dump failed: session={}, follower={}", session.id(), targetStoreId);
                default -> {
                    // do nothing
                }
            }
            dumpSessions.remove(session.id(), session);
        });
    }

    private Any reset(Boundary boundary) {
        long startAt = System.nanoTime();
        long stamp = resetLock.writeLock();
        try {
            queryReadySubject.onNext(false);
            return coProc.reset(boundary);
        } finally {
            resetLock.unlockWrite(stamp);
            queryReadySubject.onNext(true);
            log.debug("Reset coProc done, took {} ms", Duration.ofNanos(System.nanoTime() - startAt).toMillis());
        }
    }

    private enum TryMigrateResult {
        SUCCESS_AFTER_RESET, SUCCESS, FAILED
    }

    private enum TryCancelMergingResult {
        CANCELLED, ALREADY_MERGED
    }

    private enum TryConfirmMergedResult {
        ALREADY_CANCELED, MERGED
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
        void onQuit(IKVRangeFSM rangeToQuit, boolean reset);
    }
}
