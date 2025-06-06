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

import static java.util.Collections.emptyList;
import static org.apache.bifromq.basekv.InProcStores.regInProcStore;
import static org.apache.bifromq.basekv.proto.State.StateType.Normal;
import static org.apache.bifromq.basekv.store.exception.KVRangeStoreException.rangeNotFound;
import static org.apache.bifromq.basekv.store.util.ExecutorServiceUtil.awaitShutdown;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import lombok.NonNull;
import org.apache.bifromq.base.util.AsyncRunner;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVEngine;
import org.apache.bifromq.basekv.localengine.KVEngineFactory;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.EnsureRange;
import org.apache.bifromq.basekv.proto.EnsureRangeReply;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeMessage;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.proto.StoreMessage;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import org.apache.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import org.apache.bifromq.basekv.store.exception.KVRangeStoreException;
import org.apache.bifromq.basekv.store.option.KVRangeStoreOptions;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import org.apache.bifromq.basekv.store.range.IKVRange;
import org.apache.bifromq.basekv.store.range.IKVRangeFSM;
import org.apache.bifromq.basekv.store.range.KVRange;
import org.apache.bifromq.basekv.store.range.KVRangeFSM;
import org.apache.bifromq.basekv.store.stats.IStatsCollector;
import org.apache.bifromq.basekv.store.wal.IKVRangeWALStore;
import org.apache.bifromq.basekv.store.wal.KVRangeWALStorageEngine;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

public class KVRangeStore implements IKVRangeStore {
    private final Logger log;
    private final String clusterId;
    private final String id;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.INIT);
    private final Map<KVRangeId, RangeFSMHolder> kvRangeMap = Maps.newConcurrentMap();
    private final Subject<List<Observable<KVRangeDescriptor>>> descriptorListSubject =
        BehaviorSubject.<List<Observable<KVRangeDescriptor>>>create().toSerialized();
    private final IKVRangeCoProcFactory coProcFactory;
    private final KVRangeWALStorageEngine walStorageEngine;
    private final IKVEngine<? extends ICPableKVSpace> kvRangeEngine;
    private final IStatsCollector storeStatsCollector;
    private final CompositeDisposable disposable = new CompositeDisposable();
    private final Executor queryExecutor;
    private final ScheduledExecutorService tickExecutor;
    private final ScheduledExecutorService bgTaskExecutor;
    private final ScheduledExecutorService mgmtTaskExecutor;
    private final AsyncRunner mgmtTaskRunner;
    private final KVRangeStoreOptions opts;
    private final MetricsManager metricsManager;
    private final Map<String, String> attributes;
    private volatile ScheduledFuture<?> tickFuture;
    private IStoreMessenger messenger;

    public KVRangeStore(String clusterId,
                        KVRangeStoreOptions opts,
                        IKVRangeCoProcFactory coProcFactory,
                        @NonNull Executor queryExecutor,
                        int tickerThreads,
                        @NonNull ScheduledExecutorService bgTaskExecutor,
                        Map<String, String> attributes) {
        this.clusterId = clusterId;
        this.coProcFactory = coProcFactory;
        this.opts = opts.toBuilder().build();
        this.walStorageEngine =
            new KVRangeWALStorageEngine(clusterId, opts.getOverrideIdentity(), opts.getWalEngineConfigurator());
        id = walStorageEngine.id();
        String[] tags = new String[] {"clusterId", clusterId, "storeId", id};
        log = MDCLogger.getLogger(KVRangeStore.class, tags);
        if (opts.getOverrideIdentity() != null
            && !opts.getOverrideIdentity().trim().isEmpty()
            && !opts.getOverrideIdentity().equals(id)) {
            log.warn("KVRangeStore has been initialized with identity[{}], the override[{}] is ignored",
                id, opts.getOverrideIdentity());
        }
        kvRangeEngine = KVEngineFactory.createCPable(null, opts.getDataEngineConfigurator());
        this.queryExecutor = queryExecutor;
        this.bgTaskExecutor = bgTaskExecutor;
        this.tickExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ScheduledThreadPoolExecutor(tickerThreads,
                EnvProvider.INSTANCE.newThreadFactory("basekv-store-ticker-" + clusterId)),
            "ticker", "basekv.store", Tags.of(tags));
        this.mgmtTaskExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ScheduledThreadPoolExecutor(1,
                EnvProvider.INSTANCE.newThreadFactory("basekv-store-manager-" + clusterId)),
            "manager", "basekv.store", Tags.of(tags));
        this.mgmtTaskRunner = new AsyncRunner(mgmtTaskExecutor);
        this.metricsManager = new MetricsManager(clusterId, id);
        this.attributes = attributes;
        storeStatsCollector =
            new KVRangeStoreStatsCollector(opts, Duration.ofSeconds(opts.getStatsCollectIntervalSec()),
                this.bgTaskExecutor);
    }

    @Override
    public String clusterId() {
        return clusterId;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public boolean isStarted() {
        return status.get() == Status.STARTED;
    }

    @Override
    public void start(IStoreMessenger messenger) {
        if (status.compareAndSet(Status.INIT, Status.STARTING)) {
            try {
                this.messenger = messenger;
                walStorageEngine.start();
                kvRangeEngine.start("clusterId", clusterId, "storeId", id, "type", "data");
                log.debug("KVRangeStore started");
                status.set(Status.STARTED);
                disposable.add(messenger.receive().subscribe(this::receive));
                loadExisting();
                scheduleTick(0);
                regInProcStore(clusterId, id);
            } catch (Throwable e) {
                status.set(Status.FATAL_FAILURE);
                throw new KVRangeStoreException("Failed to start kv range store", e);
            }
        } else {
            log.warn("KVRangeStore cannot start in {} status", status.get().name());
        }
    }

    private void loadExisting() {
        mgmtTaskRunner.add(() -> {
            kvRangeEngine.spaces().forEach((id, keyRange) -> {
                KVRangeId rangeId = KVRangeIdUtil.fromString(id);
                if (walStorageEngine.has(rangeId)) {
                    IKVRangeWALStore walStore = walStorageEngine.get(rangeId);
                    IKVRange range = new KVRange(rangeId, keyRange);
                    // verify the integrity of wal and range state
                    if (!validate(range, walStore)) {
                        log.warn("Destroy inconsistent KVRange: {}", id);
                        keyRange.destroy();
                        walStore.destroy();
                        return;
                    }
                    putAndOpen(loadKVRangeFSM(rangeId, range, walStore)).join();
                } else {
                    log.debug("Destroy orphan KVRange: {}", id);
                    keyRange.destroy();
                }
            });
            updateDescriptorList();
        }).toCompletableFuture().join();
    }

    private boolean validate(IKVRange range, IKVRangeWALStore walStore) {
        return range.lastAppliedIndex() <= -1
            || range.lastAppliedIndex() >= walStore.latestSnapshot().getIndex();
    }

    @Override
    public void stop() {
        if (status.compareAndSet(Status.STARTED, Status.CLOSING)) {
            try {
                log.debug("Stopping KVRange store");
                log.debug("Await for all management tasks to finish");
                mgmtTaskRunner.awaitDone().toCompletableFuture().join();
                List<CompletableFuture<Void>> closeFutures = new ArrayList<>();
                try {
                    for (RangeFSMHolder holder : kvRangeMap.values()) {
                        closeFutures.add(holder.fsm.close());
                    }
                } catch (Throwable e) {
                    log.error("Failed to close range", e);
                }

                CompletableFuture.allOf(closeFutures.toArray(CompletableFuture[]::new)).join();
                disposable.dispose();
                storeStatsCollector.stop().toCompletableFuture().join();
                log.debug("Stopping WAL Engine");
                walStorageEngine.stop();
                log.debug("Stopping KVRange Engine");
                kvRangeEngine.stop();
                descriptorListSubject.onComplete();
                status.set(Status.CLOSED);
                status.set(Status.TERMINATING);
                tickFuture.get();
            } catch (Throwable e) {
                log.error("Error occurred during stopping range store", e);
            } finally {
                messenger.close();
                awaitShutdown(tickExecutor).join();
                awaitShutdown(mgmtTaskExecutor).join();
                status.set(Status.TERMINATED);
            }
        }
    }

    @Override
    public CompletableFuture<Boolean> bootstrap(KVRangeId rangeId, Boundary boundary) {
        return mgmtTaskRunner.add(() -> {
            if (status.get() != Status.STARTED) {
                return CompletableFuture.failedFuture(new IllegalStateException("Store not running"));
            }
            if (kvRangeMap.containsKey(rangeId)) {
                return CompletableFuture.completedFuture(false);
            }
            log.debug("Bootstrap KVRange: rangeId={}, boundary={}", KVRangeIdUtil.toString(rangeId), boundary);
            if (kvRangeEngine.spaces().containsKey(KVRangeIdUtil.toString(rangeId))) {
                ICPableKVSpace keyRange = kvRangeEngine.spaces().get(KVRangeIdUtil.toString(rangeId));
                log.debug("Destroy existing KeySpace: rangeId={}", KVRangeIdUtil.toString(rangeId));
                keyRange.destroy();
            }
            if (walStorageEngine.has(rangeId)) {
                log.debug("Destroy staled WALStore: rangeId={}", KVRangeIdUtil.toString(rangeId));
                walStorageEngine.get(rangeId).destroy();
            }
            ClusterConfig initConfig = ClusterConfig.newBuilder()
                .addVoters(id())
                .build();
            KVRangeSnapshot rangeSnapshot = KVRangeSnapshot.newBuilder()
                .setId(rangeId)
                .setVer(0)
                .setLastAppliedIndex(5)
                .setState(State.newBuilder().setType(Normal).build())
                .setBoundary(boundary)
                .setClusterConfig(initConfig)
                .build();
            Snapshot snapshot = Snapshot.newBuilder()
                .setClusterConfig(initConfig)
                .setTerm(0)
                .setIndex(5)
                .setData(rangeSnapshot.toByteString())
                .build();
            return putAndOpen(createKVRangeFSM(rangeId, snapshot, rangeSnapshot))
                .thenApply(v -> {
                    updateDescriptorList();
                    return true;
                });
        });
    }

    @Override
    public boolean isHosting(KVRangeId rangeId) {
        return kvRangeMap.containsKey(rangeId);
    }

    @Override
    public CompletionStage<Void> recover(KVRangeId rangeId) {
        checkStarted();
        RangeFSMHolder holder = kvRangeMap.get(rangeId);
        if (holder != null) {
            metricsManager.runningRecoverNum.increment();
            return holder.fsm.recover()
                .whenComplete((v, e) -> metricsManager.runningRecoverNum.decrement());
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public Observable<KVRangeStoreDescriptor> describe() {
        checkStarted();
        return descriptorListSubject
            .distinctUntilChanged()
            .switchMap(descriptorList -> {
                Observable<List<KVRangeDescriptor>> descListObservable = descriptorList.isEmpty()
                    ? BehaviorSubject.createDefault(emptyList()) :
                    Observable.combineLatest(descriptorList, descs ->
                        Arrays.stream(descs).map(desc -> (KVRangeDescriptor) desc).collect(Collectors.toList()));
                return Observable.combineLatest(storeStatsCollector.collect().distinctUntilChanged(),
                    descListObservable,
                    (storeStats, descList) -> KVRangeStoreDescriptor.newBuilder()
                        .setId(id)
                        .putAllStatistics(storeStats)
                        .addAllRanges(descList)
                        .setHlc(HLC.INST.get())
                        .putAllAttributes(attributes)
                        .build());
            })
            .distinctUntilChanged();
    }

    private void receive(StoreMessage storeMessage) {
        if (status.get() == Status.STARTED) {
            KVRangeMessage payload = storeMessage.getPayload();
            if (payload.hasEnsureRange()) {
                EnsureRange request = storeMessage.getPayload().getEnsureRange();
                mgmtTaskRunner.add(() -> {
                    if (status.get() != Status.STARTED) {
                        return CompletableFuture.completedFuture(null);
                    }
                    KVRangeId rangeId = payload.getRangeId();
                    RangeFSMHolder holder = kvRangeMap.get(rangeId);
                    try {
                        Snapshot walSnapshot = request.getInitSnapshot();
                        KVRangeSnapshot rangeSnapshot = KVRangeSnapshot.parseFrom(walSnapshot.getData());
                        if (holder != null) {
                            // pin the range
                            if (holder.fsm.ver() < request.getVer()) {
                                log.debug("Range already exists, pinning it: rangeId={}",
                                    KVRangeIdUtil.toString(rangeId));
                                holder.pin(request.getVer(), walSnapshot, rangeSnapshot);
                            }
                            messenger.send(StoreMessage.newBuilder()
                                .setFrom(id)
                                .setSrcRange(payload.getRangeId())
                                .setPayload(KVRangeMessage.newBuilder()
                                    .setRangeId(storeMessage.getSrcRange())
                                    .setHostStoreId(storeMessage.getFrom())
                                    .setEnsureRangeReply(EnsureRangeReply.newBuilder()
                                        .setResult(EnsureRangeReply.Result.OK).build())
                                    .build())
                                .build());
                            return CompletableFuture.completedFuture(null);
                        } else {
                            return ensureRange(rangeId, walSnapshot, rangeSnapshot)
                                .whenComplete((v, e) -> {
                                    updateDescriptorList();
                                    messenger.send(StoreMessage.newBuilder()
                                        .setFrom(id)
                                        .setSrcRange(payload.getRangeId())
                                        .setPayload(KVRangeMessage.newBuilder()
                                            .setRangeId(storeMessage.getSrcRange())
                                            .setHostStoreId(storeMessage.getFrom())
                                            .setEnsureRangeReply(EnsureRangeReply.newBuilder()
                                                .setResult(EnsureRangeReply.Result.OK)
                                                .build())
                                            .build())
                                        .build());
                                });
                        }
                    } catch (Throwable e) {
                        // should never happen
                        log.error("Unexpected error", e);
                        messenger.send(StoreMessage.newBuilder()
                            .setFrom(id)
                            .setSrcRange(payload.getRangeId())
                            .setPayload(KVRangeMessage.newBuilder()
                                .setRangeId(storeMessage.getSrcRange())
                                .setHostStoreId(storeMessage.getFrom())
                                .setEnsureRangeReply(EnsureRangeReply.newBuilder()
                                    .setResult(EnsureRangeReply.Result.Error)
                                    .build())
                                .build())
                            .build());
                        return CompletableFuture.completedFuture(null);
                    }
                });
            }
        }
    }

    @Override
    public CompletionStage<Void> transferLeadership(long ver, KVRangeId rangeId, String newLeader) {
        checkStarted();
        RangeFSMHolder holder = kvRangeMap.get(rangeId);
        if (holder != null) {
            metricsManager.runningTransferLeaderNum.increment();
            return holder.fsm.transferLeadership(ver, newLeader)
                .whenComplete((v, e) -> metricsManager.runningTransferLeaderNum.decrement());
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<Void> changeReplicaConfig(long ver, KVRangeId rangeId,
                                                     Set<String> newVoters,
                                                     Set<String> newLearners) {
        checkStarted();
        RangeFSMHolder holder = kvRangeMap.get(rangeId);
        if (holder != null) {
            metricsManager.runningConfigChangeNum.increment();
            return holder.fsm.changeReplicaConfig(ver, newVoters, newLearners)
                .whenComplete((v, e) -> metricsManager.runningConfigChangeNum.decrement());
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<Void> split(long ver, KVRangeId rangeId, ByteString splitKey) {
        checkStarted();
        RangeFSMHolder holder = kvRangeMap.get(rangeId);
        if (holder != null) {
            metricsManager.runningSplitNum.increment();
            return holder.fsm.split(ver, splitKey).whenComplete((v, e) -> metricsManager.runningSplitNum.decrement());
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<Void> merge(long ver, KVRangeId mergerId, KVRangeId mergeeId) {
        checkStarted();
        RangeFSMHolder holder = kvRangeMap.get(mergerId);
        if (holder != null) {
            metricsManager.runningMergeNum.increment();
            return holder.fsm.merge(ver, mergeeId).whenComplete((v, e) -> metricsManager.runningMergeNum.decrement());
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<Boolean> exist(long ver, KVRangeId id, ByteString key, boolean linearized) {
        checkStarted();
        RangeFSMHolder holder = kvRangeMap.get(id);
        if (holder != null) {
            return holder.fsm.exist(ver, key, linearized);
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<Optional<ByteString>> get(long ver, KVRangeId id, ByteString key,
                                                     boolean linearized) {
        checkStarted();
        RangeFSMHolder holder = kvRangeMap.get(id);
        if (holder != null) {
            return holder.fsm.get(ver, key, linearized);
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<ROCoProcOutput> queryCoProc(long ver, KVRangeId id, ROCoProcInput query,
                                                       boolean linearized) {
        checkStarted();
        RangeFSMHolder holder = kvRangeMap.get(id);
        if (holder != null) {
            metricsManager.runningQueryNum.increment();
            return holder.fsm.queryCoProc(ver, query, linearized)
                .whenComplete((v, e) -> metricsManager.runningQueryNum.decrement());
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<ByteString> put(long ver, KVRangeId id, ByteString key, ByteString value) {
        checkStarted();
        RangeFSMHolder holder = kvRangeMap.get(id);
        if (holder != null) {
            return holder.fsm.put(ver, key, value);
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<ByteString> delete(long ver, KVRangeId id, ByteString key) {
        checkStarted();
        RangeFSMHolder holder = kvRangeMap.get(id);
        if (holder != null) {
            return holder.fsm.delete(ver, key);
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<RWCoProcOutput> mutateCoProc(long ver, KVRangeId id, RWCoProcInput mutate) {
        checkStarted();
        RangeFSMHolder holder = kvRangeMap.get(id);
        if (holder != null) {
            metricsManager.runningMutateNum.increment();
            return holder.fsm.mutateCoProc(ver, mutate)
                .whenComplete((v, e) -> metricsManager.runningMutateNum.decrement());
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    private void scheduleTick(long delayInMS) {
        tickFuture = tickExecutor.schedule(this::tick, delayInMS, TimeUnit.MILLISECONDS);
    }

    private void tick() {
        if (status.get() != Status.STARTED && status.get() != Status.CLOSING) {
            return;
        }
        try {
            kvRangeMap.forEach((v, r) -> r.fsm.tick());
            storeStatsCollector.tick();
        } catch (Throwable e) {
            log.error("Unexpected error during tick", e);
        } finally {
            scheduleTick(opts.getKvRangeOptions().getTickUnitInMS());
        }
    }

    private CompletableFuture<Void> ensureRange(KVRangeId rangeId, Snapshot walSnapshot,
                                                KVRangeSnapshot rangeSnapshot) {
        ICPableKVSpace keyRange = kvRangeEngine.spaces().get(KVRangeIdUtil.toString(rangeId));
        if (keyRange == null) {
            if (walStorageEngine.has(rangeId)) {
                log.warn("Destroy staled WALStore: rangeId={}", KVRangeIdUtil.toString(rangeId));
                walStorageEngine.get(rangeId).destroy();
            }
            return putAndOpen(createKVRangeFSM(rangeId, walSnapshot, rangeSnapshot));
        } else {
            // for split workflow, the keyspace is already created, just create walstore and load it
            IKVRangeWALStore walStore = walStorageEngine.get(rangeId);
            if (walStore == null) {
                walStore = walStorageEngine.create(rangeId, walSnapshot);
            }
            return putAndOpen(loadKVRangeFSM(rangeId, new KVRange(rangeId, keyRange), walStore));
        }
    }

    private void quitKVRange(IKVRangeFSM range) {
        if (status.get() != Status.STARTED) {
            return;
        }
        CompletableFuture<Optional<RangeFSMHolder.PinnedRange>> afterDestroyed = mgmtTaskRunner.add(() -> {
            if (status.get() != Status.STARTED) {
                return CompletableFuture.failedFuture(new KVRangeStoreException("Not started"));
            }
            RangeFSMHolder holder = kvRangeMap.remove(range.id());
            assert holder.fsm == range;
            long ver = range.ver();
            log.debug("Destroy kvrange: rangeId={}", KVRangeIdUtil.toString(range.id()));
            return range.destroy()
                .thenApply(v -> {
                    if (holder.pinned != null && holder.pinned.ver > ver) {
                        return Optional.of(holder.pinned);
                    }
                    return Optional.empty();
                });
        });
        afterDestroyed.thenCompose(pinned ->
                mgmtTaskRunner.add(() -> {
                    if (pinned.isPresent()) {
                        RangeFSMHolder.PinnedRange pinnedRange = pinned.get();
                        log.debug("Recreate range after destroy: rangeId={}", KVRangeIdUtil.toString(range.id()));
                        return ensureRange(range.id(), pinnedRange.walSnapshot, pinnedRange.fsmSnapshot);
                    }
                    return CompletableFuture.completedFuture(null);
                }))
            .thenAccept(v -> updateDescriptorList());
    }

    private IKVRangeFSM loadKVRangeFSM(KVRangeId rangeId, IKVRange range, IKVRangeWALStore walStore) {
        log.debug("Load existing kvrange: rangeId={}", KVRangeIdUtil.toString(rangeId));
        return new KVRangeFSM(clusterId,
            id,
            rangeId,
            coProcFactory,
            range,
            walStore,
            queryExecutor,
            bgTaskExecutor,
            opts.getKvRangeOptions(),
            this::quitKVRange);
    }

    private IKVRangeFSM createKVRangeFSM(KVRangeId rangeId, Snapshot snapshot, KVRangeSnapshot rangeSnapshot) {
        log.debug("Creating new kvrange: rangeId={}", KVRangeIdUtil.toString(rangeId));
        IKVRangeWALStore walStore = walStorageEngine.create(rangeId, snapshot);
        return new KVRangeFSM(clusterId,
            id,
            rangeId,
            coProcFactory,
            new KVRange(rangeId, kvRangeEngine.createIfMissing(KVRangeIdUtil.toString(rangeId)), rangeSnapshot),
            walStore,
            queryExecutor,
            bgTaskExecutor,
            opts.getKvRangeOptions(),
            this::quitKVRange);
    }

    private void updateDescriptorList() {
        descriptorListSubject.onNext(
            kvRangeMap.values().stream().map(h -> h.fsm).map(IKVRangeFSM::describe).collect(Collectors.toList()));
    }

    private CompletableFuture<Void> putAndOpen(IKVRangeFSM kvRangeFSM) {
        kvRangeMap.put(kvRangeFSM.id(), new RangeFSMHolder(kvRangeFSM));
        return kvRangeFSM.open(new KVRangeMessenger(id, kvRangeFSM.id(), messenger));
    }

    private void checkStarted() {
        Preconditions.checkState(status.get() == Status.STARTED, "Store not running");
    }

    private enum Status {
        INIT, // store is created but cannot serve requests
        STARTING, // store is starting
        STARTED, // store can serve requests
        FATAL_FAILURE, // fatal failure happened during starting
        CLOSING, // store closing, no more outgoing messages
        CLOSED, // store closed, no tasks running
        TERMINATING, // releasing all resources
        TERMINATED // resource released
    }

    private static class RangeFSMHolder {
        private final IKVRangeFSM fsm;
        private PinnedRange pinned;

        RangeFSMHolder(IKVRangeFSM fsm) {
            this.fsm = fsm;
        }

        // if the range should be recreated after destroy using given snapshot state
        void pin(long ver, Snapshot walSnapshot, KVRangeSnapshot fsmSnapshot) {
            if (pinned == null || ver >= pinned.ver) {
                this.pinned = new PinnedRange(ver, walSnapshot, fsmSnapshot);
            }
        }

        record PinnedRange(long ver, Snapshot walSnapshot, KVRangeSnapshot fsmSnapshot) {

        }
    }

    private static class MetricsManager {
        private final LongAdder runningConfigChangeNum;
        private final LongAdder runningTransferLeaderNum;
        private final LongAdder runningSplitNum;
        private final LongAdder runningMergeNum;
        private final LongAdder runningRecoverNum;
        private final LongAdder runningQueryNum;
        private final LongAdder runningMutateNum;

        MetricsManager(String clusterId, String storeId) {
            Tags tags = Tags.of("clusterId", clusterId).and("storeId", storeId);
            runningConfigChangeNum =
                Metrics.gauge("basekv.store.running", tags.and("cmd", "configchange"), new LongAdder());
            runningTransferLeaderNum =
                Metrics.gauge("basekv.store.running", tags.and("cmd", "transferleader"), new LongAdder());
            runningSplitNum =
                Metrics.gauge("basekv.store.running", tags.and("cmd", "split"), new LongAdder());
            runningMergeNum =
                Metrics.gauge("basekv.store.running", tags.and("cmd", "merge"), new LongAdder());
            runningRecoverNum =
                Metrics.gauge("basekv.store.running", tags.and("cmd", "recover"), new LongAdder());
            runningQueryNum =
                Metrics.gauge("basekv.store.running", tags.and("cmd", "query"), new LongAdder());
            runningMutateNum =
                Metrics.gauge("basekv.store.running", tags.and("cmd", "mutate"), new LongAdder());
        }
    }
}
