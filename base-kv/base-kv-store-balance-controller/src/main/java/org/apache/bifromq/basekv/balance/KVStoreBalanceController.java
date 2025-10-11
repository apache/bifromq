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

package org.apache.bifromq.basekv.balance;

import com.google.common.collect.Lists;
import com.google.protobuf.Struct;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Sample;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Builder;
import org.apache.bifromq.basekv.balance.command.BalanceCommand;
import org.apache.bifromq.basekv.balance.command.BootstrapCommand;
import org.apache.bifromq.basekv.balance.command.ChangeConfigCommand;
import org.apache.bifromq.basekv.balance.command.MergeCommand;
import org.apache.bifromq.basekv.balance.command.QuitCommand;
import org.apache.bifromq.basekv.balance.command.RangeCommand;
import org.apache.bifromq.basekv.balance.command.RecoveryCommand;
import org.apache.bifromq.basekv.balance.command.SplitCommand;
import org.apache.bifromq.basekv.balance.command.TransferLeadershipCommand;
import org.apache.bifromq.basekv.balance.impl.RangeBootstrapBalancer;
import org.apache.bifromq.basekv.balance.impl.RedundantRangeRemovalBalancer;
import org.apache.bifromq.basekv.balance.impl.UnreachableReplicaRemovalBalancer;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.basekv.metaservice.IBaseKVStoreBalancerStatesProposal;
import org.apache.bifromq.basekv.metaservice.IBaseKVStoreBalancerStatesReporter;
import org.apache.bifromq.basekv.proto.BalancerStateSnapshot;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.store.proto.BootstrapRequest;
import org.apache.bifromq.basekv.store.proto.ChangeReplicaConfigReply;
import org.apache.bifromq.basekv.store.proto.ChangeReplicaConfigRequest;
import org.apache.bifromq.basekv.store.proto.KVRangeMergeReply;
import org.apache.bifromq.basekv.store.proto.KVRangeMergeRequest;
import org.apache.bifromq.basekv.store.proto.KVRangeSplitReply;
import org.apache.bifromq.basekv.store.proto.KVRangeSplitRequest;
import org.apache.bifromq.basekv.store.proto.RecoverRequest;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.basekv.store.proto.TransferLeadershipReply;
import org.apache.bifromq.basekv.store.proto.TransferLeadershipRequest;
import org.apache.bifromq.basekv.store.proto.ZombieQuitRequest;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

/**
 * The controller to manage the balance of KVStore.
 */
public class KVStoreBalanceController {
    private final IBaseKVMetaService metaService;
    private final IBaseKVStoreClient storeClient;
    private final Map<KVRangeId, Long> rangeCommandHistory = new ConcurrentHashMap<>();
    private final AtomicBoolean scheduling = new AtomicBoolean();
    private final AtomicReference<State> state = new AtomicReference<>(State.Init);
    private final ScheduledExecutorService executor;
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final List<? extends IStoreBalancerFactory> builtinBalancerFactories;
    private final List<? extends IStoreBalancerFactory> customBalancerFactories;
    private final Map<String, StoreBalancerState> balancers;
    private final Duration retryDelay;
    private final IBaseKVStoreBalancerStatesProposal statesProposal;
    private IBaseKVStoreBalancerStatesReporter statesReporter;
    private String localStoreId;
    private Logger log;
    private MetricManager metricsManager;
    private volatile Map<String, BalancerStateSnapshot> expectedBalancerStates;
    private volatile Set<KVRangeStoreDescriptor> landscape;
    private volatile ScheduledFuture<?> task;

    /**
     * Create a new KVStoreBalanceController.
     *
     * @param metaService the metadata service
     * @param storeClient the store client
     * @param factories   the balancer factories
     * @param retryDelay  the delay before retry
     * @param executor    the executor
     */
    public KVStoreBalanceController(IBaseKVMetaService metaService,
                                    IBaseKVStoreClient storeClient,
                                    List<? extends IStoreBalancerFactory> factories,
                                    Duration bootstrapDelay,
                                    Duration zombieProbeDelay,
                                    Duration retryDelay,
                                    ScheduledExecutorService executor) {
        this.metaService = metaService;
        this.storeClient = storeClient;
        this.customBalancerFactories = Lists.newArrayList(factories);
        this.builtinBalancerFactories = Lists.newArrayList(
            new RangeBootstrapBalancerFactory(bootstrapDelay),
            new RedundantRangeRemovalBalancerFactory(zombieProbeDelay),
            new UnreachableReplicaRemovalBalancerFactory(zombieProbeDelay));
        this.statesProposal = metaService.balancerStatesProposal(storeClient.clusterId());
        this.balancers = new HashMap<>();
        this.retryDelay = retryDelay;
        this.executor = executor;
    }

    /**
     * Start the controller.
     *
     * @param localStoreId the local store id
     */
    public void start(String localStoreId) {
        if (state.compareAndSet(State.Init, State.Started)) {
            this.localStoreId = localStoreId;
            statesReporter = metaService.balancerStatesReporter(storeClient.clusterId(), localStoreId);
            log = MDCLogger.getLogger("balancer.logger",
                "clusterId", storeClient.clusterId(), "storeId", localStoreId, "balancer", "CONTROLLER");

            for (IStoreBalancerFactory factory : builtinBalancerFactories) {
                StoreBalancer balancer = factory.newBalancer(storeClient.clusterId(), localStoreId);
                log.info("Create builtin balancer: {}", balancer.getClass().getSimpleName());
                balancers.put(factory.getClass().getName(), new StoreBalancerState(balancer, true));
            }
            for (IStoreBalancerFactory factory : customBalancerFactories) {
                String balancerFactoryFQN = factory.getClass().getName();
                StoreBalancer balancer = factory.newBalancer(storeClient.clusterId(), localStoreId);
                log.info("Create balancer[{}] from factory: {}", balancer.getClass().getName(), balancerFactoryFQN);
                if (balancer instanceof RangeBootstrapBalancer
                    || balancer instanceof RedundantRangeRemovalBalancer
                    || balancer instanceof UnreachableReplicaRemovalBalancer) {
                    log.warn("Builtin balancer[{}] should not be created from custom balancer factory",
                        balancer.getClass().getSimpleName());
                    continue;
                }
                StoreBalancerState balancerState = new StoreBalancerState(balancer, false);
                balancers.put(balancerFactoryFQN, balancerState);
                statesReporter.reportBalancerState(balancerFactoryFQN, false, balancerState.loadRules.get());
            }
            this.metricsManager = new MetricManager(localStoreId, storeClient.clusterId());
            log.info("BalancerController start");
            disposables.add(statesProposal.expectedBalancerStates()
                .subscribe(currentExpected -> {
                    log.trace("Expected balancer states changed: {}", currentExpected);
                    this.expectedBalancerStates = currentExpected;
                    trigger();
                }));
            disposables.add(storeClient.describe().subscribe(descriptors -> {
                log.trace("Landscape changed: {}", descriptors);
                this.landscape = descriptors;
                trimRangeHistory(descriptors);
                trigger();
            }));
            disposables.add(statesReporter.refreshSignal()
                .subscribe(ts -> {
                    for (Map.Entry<String, StoreBalancerState> entry : balancers.entrySet()) {
                        String balancerFacClassFQN = entry.getKey();
                        StoreBalancerState balancerState = entry.getValue();
                        if (!balancerState.isBuiltin) {
                            log.debug("Report balancer state for {}", balancerFacClassFQN);
                            statesReporter.reportBalancerState(balancerFacClassFQN,
                                balancerState.disabled.get(), balancerState.loadRules.get());
                        }
                    }
                }));
        }
    }

    /**
     * Stop the controller.
     */
    public void stop() {
        if (state.compareAndSet(State.Started, State.Closed)) {
            statesProposal.stop();
            if (task != null) {
                task.cancel(true);
                if (!task.isDone()) {
                    try {
                        task.get(5, TimeUnit.SECONDS);
                    } catch (Throwable e) {
                        // ignore
                    }
                }
            }
            statesReporter.stop();
            disposables.dispose();
            balancers.values().forEach(sbs -> sbs.balancer.close());
        }
    }

    private void trigger() {
        if (state.get() == State.Started && scheduling.compareAndSet(false, true)) {
            long jitter = ThreadLocalRandom.current().nextLong(0, retryDelay.toMillis());
            if (task != null && !task.isDone()) {
                log.trace("Cancel scheduled balance task");
                task.cancel(true);
            }
            task = executor.schedule(this::updateAndBalance, jitter, TimeUnit.MILLISECONDS);
        }
    }

    private void updateAndBalance() {
        Map<String, BalancerStateSnapshot> expectedBalancerState = this.expectedBalancerStates;
        Set<KVRangeStoreDescriptor> landscape = this.landscape;
        if (landscape == null || landscape.isEmpty()) {
            scheduling.set(false);
            if (!Objects.equals(this.landscape, landscape)) {
                trigger();
            }
            return;
        }
        for (Map.Entry<String, StoreBalancerState> entry : balancers.entrySet()) {
            String balancerFacClassFQN = entry.getKey();
            StoreBalancerState balancerState = entry.getValue();
            try {
                if (expectedBalancerState != null) {
                    BalancerStateSnapshot expectedState = expectedBalancerState.get(balancerFacClassFQN);
                    if (expectedState != null) {
                        if (!balancerState.isBuiltin) {
                            boolean disable = expectedState.getDisable();
                            Struct loadRules = balancerState.loadRules.get();
                            boolean needReport = false;
                            if (balancerState.disabled.get() != disable) {
                                log.info("Balancer[{}] is {}", balancerState.balancer.getClass().getSimpleName(),
                                    disable ? "disabled" : "enabled");
                                balancerState.disabled.set(disable);
                                needReport = true;
                            }
                            Struct expectedLoadRules = loadRules.toBuilder()
                                .mergeFrom(expectedState.getLoadRules())
                                .build();
                            if (!loadRules.equals(expectedLoadRules)) {
                                if (balancerState.balancer.validate(expectedLoadRules)) {
                                    loadRules = expectedLoadRules;
                                    // report the balancer state
                                    balancerState.loadRules.set(expectedLoadRules);
                                    balancerState.balancer.update(expectedLoadRules);
                                    needReport = true;
                                } else {
                                    log.warn("Balancer[{}] load rules not valid: {}",
                                        balancerState.balancer.getClass().getSimpleName(), expectedLoadRules);
                                }
                            }
                            if (needReport) {
                                statesReporter.reportBalancerState(balancerFacClassFQN, disable, loadRules);
                            }
                        } else {
                            log.warn("Cannot change the state of builtin balancer: {}", balancerFacClassFQN);
                        }
                    }
                }
                balancerState.balancer.update(landscape);
            } catch (Throwable e) {
                log.error("Balancer[{}] update failed", balancerState.balancer.getClass().getSimpleName(), e);
            }
        }
        balance(expectedBalancerState, landscape);
    }

    private void scheduleRetry(Map<String, BalancerStateSnapshot> expected,
                               Set<KVRangeStoreDescriptor> landscape,
                               Duration delay) {
        log.debug("Retry balance after {}s", delay.toSeconds());
        task = executor.schedule(() -> {
            if (!Objects.equals(expected, this.expectedBalancerStates) || landscape != this.landscape) {
                // retry is preemptive
                log.trace("Balance retry is preempted");
                return;
            }
            if (scheduling.compareAndSet(false, true)) {
                balance(expected, landscape);
            }
        }, delay.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void balance(final Map<String, BalancerStateSnapshot> expected,
                         final Set<KVRangeStoreDescriptor> landscape) {
        metricsManager.scheduleCount.increment();
        Duration delay = null;
        for (Map.Entry<String, StoreBalancerState> entry : balancers.entrySet()) {
            StoreBalancerState fromBalancerState = entry.getValue();
            StoreBalancer fromBalancer = fromBalancerState.balancer;
            String balancerName = fromBalancer.getClass().getSimpleName();
            if (fromBalancerState.disabled.get()) {
                continue;
            }
            try {
                BalanceResult result = fromBalancer.balance();
                switch (result.type()) {
                    case BalanceNow -> {
                        BalanceCommand commandToRun = ((BalanceNow<?>) result).command;
                        if (!isStaleCommand(commandToRun)) {
                            String cmdName = commandToRun.getClass().getSimpleName();
                            log.info("Balancer[{}] command run: {}", balancerName, commandToRun);
                            Sample start = Timer.start();
                            runCommand(commandToRun)
                                .whenCompleteAsync((success, e) -> {
                                    MetricManager.CommandMetrics
                                        metrics = metricsManager.getCommandMetrics(balancerName, cmdName);
                                    if (e != null) {
                                        log.error("Should not be here, error when run command", e);
                                        metrics.cmdFailedCounter.increment();
                                    } else {
                                        log.info("Balancer[{}] command run result[{}]: {}",
                                            balancerName, success, commandToRun);
                                        if (success) {
                                            metrics.cmdSucceedCounter.increment();
                                            start.stop(metrics.cmdRunTimer);
                                        } else {
                                            metrics.cmdFailedCounter.increment();
                                        }
                                    }
                                    scheduling.set(false);
                                    if (success) {
                                        if (!Objects.equals(this.landscape, landscape)
                                            || !Objects.equals(this.expectedBalancerStates, expected)) {
                                            trigger();
                                        }
                                    } else {
                                        scheduleRetry(expected, landscape, retryDelay);
                                    }
                                }, executor);
                            return;
                        }
                    }
                    case AwaitBalance -> {
                        Duration await = ((AwaitBalance) result).await;
                        delay = delay != null ? (await.toNanos() < delay.toNanos() ? await : delay) : await;
                    }
                    default -> {
                        // do nothing
                    }
                }
            } catch (Throwable e) {
                log.warn("Balancer[{}] unexpected error", balancerName, e);
            }
        }
        // no command to run
        scheduling.set(false);
        if (!Objects.equals(this.landscape, landscape) || !Objects.equals(this.expectedBalancerStates, expected)) {
            trigger();
        } else if (delay != null) {
            // if some balancers are in the progress of generating balance command, wait for a while
            scheduleRetry(expected, landscape, delay);
        }
    }

    private boolean isStaleCommand(BalanceCommand command) {
        if (command instanceof RangeCommand rangeCommand) {
            Long prevCMDVer = rangeCommandHistory.getOrDefault(rangeCommand.getKvRangeId(), null);
            if (prevCMDVer != null && prevCMDVer >= rangeCommand.getExpectedVer()) {
                log.debug("Ignore staled command: {}", rangeCommand);
                return true;
            }
        }
        return false;
    }

    private void trimRangeHistory(Set<KVRangeStoreDescriptor> landscape) {
        for (KVRangeStoreDescriptor storeDescriptor : landscape) {
            if (storeDescriptor.getId().equals(localStoreId)) {
                Set<KVRangeId> localRangeIds = storeDescriptor.getRangesList().stream()
                    .map(KVRangeDescriptor::getId)
                    .collect(Collectors.toSet());
                rangeCommandHistory.keySet().retainAll(localRangeIds);
            }
        }
    }

    private CompletableFuture<Boolean> runCommand(BalanceCommand command) {
        return switch (command.type()) {
            case CHANGE_CONFIG -> {
                assert command instanceof ChangeConfigCommand;
                ChangeConfigCommand changeConfigCommand = (ChangeConfigCommand) command;
                ChangeReplicaConfigRequest changeConfigRequest = ChangeReplicaConfigRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setKvRangeId(changeConfigCommand.getKvRangeId())
                    .setVer(changeConfigCommand.getExpectedVer())
                    .addAllNewVoters(changeConfigCommand.getVoters())
                    .addAllNewLearners(changeConfigCommand.getLearners())
                    .build();
                yield handleStoreReplyCode(command,
                    storeClient.changeReplicaConfig(command.getToStore(), changeConfigRequest)
                        .thenApply(ChangeReplicaConfigReply::getCode)
                );
            }
            case MERGE -> {
                assert command instanceof MergeCommand;
                MergeCommand mergeCommand = (MergeCommand) command;
                KVRangeMergeRequest rangeMergeRequest = KVRangeMergeRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setVer(mergeCommand.getExpectedVer())
                    .setMergerId(mergeCommand.getKvRangeId())
                    .setMergeeId(mergeCommand.getMergeeId())
                    .addAllMergeeVoters(mergeCommand.getVoters())
                    .build();
                yield handleStoreReplyCode(command,
                    storeClient.mergeRanges(command.getToStore(), rangeMergeRequest)
                        .thenApply(KVRangeMergeReply::getCode));
            }
            case SPLIT -> {
                assert command instanceof SplitCommand;
                SplitCommand splitCommand = (SplitCommand) command;
                KVRangeSplitRequest kvRangeSplitRequest = KVRangeSplitRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setKvRangeId(splitCommand.getKvRangeId())
                    .setVer(splitCommand.getExpectedVer())
                    .setSplitKey(splitCommand.getSplitKey())
                    .build();
                yield handleStoreReplyCode(command,
                    storeClient.splitRange(command.getToStore(), kvRangeSplitRequest)
                        .thenApply(KVRangeSplitReply::getCode));
            }
            case TRANSFER_LEADERSHIP -> {
                assert command instanceof TransferLeadershipCommand;
                TransferLeadershipCommand transferLeadershipCommand = (TransferLeadershipCommand) command;
                TransferLeadershipRequest transferLeadershipRequest = TransferLeadershipRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setKvRangeId(transferLeadershipCommand.getKvRangeId())
                    .setVer(transferLeadershipCommand.getExpectedVer())
                    .setNewLeaderStore(transferLeadershipCommand.getNewLeaderStore())
                    .build();
                yield handleStoreReplyCode(command,
                    storeClient.transferLeadership(command.getToStore(), transferLeadershipRequest)
                        .thenApply(TransferLeadershipReply::getCode));
            }
            case RECOVERY -> {
                assert command instanceof RecoveryCommand;
                RecoveryCommand recoveryCommand = (RecoveryCommand) command;
                RecoverRequest recoverRequest = RecoverRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setKvRangeId(recoveryCommand.getKvRangeId())
                    .build();
                yield storeClient.recover(command.getToStore(), recoverRequest)
                    .handle((r, e) -> {
                        if (e != null) {
                            log.error("Unexpected error when recover, req: {}", recoverRequest, e);
                        }
                        return true;
                    });
            }
            case QUIT -> {
                assert command instanceof QuitCommand;
                QuitCommand quitCommand = (QuitCommand) command;
                ZombieQuitRequest zombieQuitRequest = ZombieQuitRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setKvRangeId(quitCommand.getKvRangeId())
                    .build();
                yield storeClient.zombieQuit(command.getToStore(), zombieQuitRequest)
                    .handle((r, e) -> {
                        if (e != null) {
                            log.error("Unexpected error when recover, req: {}", zombieQuitRequest, e);
                        }
                        log.debug("Range[{}] in zombie state and quit?: {}",
                            KVRangeIdUtil.toString(quitCommand.getKvRangeId()), r.getQuit());
                        return true;
                    });
            }
            case BOOTSTRAP -> {
                assert command instanceof BootstrapCommand;
                BootstrapCommand bootstrapCommand = (BootstrapCommand) command;
                BootstrapRequest bootstrapRequest = BootstrapRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setKvRangeId(bootstrapCommand.getKvRangeId())
                    .setBoundary(bootstrapCommand.getBoundary())
                    .build();
                yield storeClient.bootstrap(command.getToStore(), bootstrapRequest)
                    .handle((r, e) -> {
                        if (e != null) {
                            log.error("Unexpected error when bootstrap: {}", command, e);
                        }
                        return true;
                    });
            }
        };
    }

    private CompletableFuture<Boolean> handleStoreReplyCode(BalanceCommand command,
                                                            CompletableFuture<ReplyCode> storeReply) {
        CompletableFuture<Boolean> onDone = new CompletableFuture<>();
        storeReply.whenComplete((code, e) -> {
            if (e != null) {
                log.error("Unexpected error when run command: {}", command, e);
                onDone.complete(false);
                return;
            }
            switch (code) {
                case Ok -> {
                    switch (command.type()) {
                        case SPLIT, MERGE, CHANGE_CONFIG -> {
                            RangeCommand rangeCommand = (RangeCommand) command;
                            rangeCommandHistory.compute(rangeCommand.getKvRangeId(), (k, v) -> {
                                if (v == null) {
                                    v = rangeCommand.getExpectedVer();
                                }
                                return Math.max(v, rangeCommand.getExpectedVer());
                            });
                        }
                        default -> {
                            // no nothing
                        }
                    }
                    onDone.complete(true);
                }
                case BadRequest, BadVersion, TryLater, InternalError -> {
                    log.warn("Failed with reply: {}, command: {}", code, command);
                    onDone.complete(false);
                }
                default -> onDone.complete(false);
            }
        });
        return onDone;
    }

    private enum State {
        Init,
        Started,
        Closed
    }

    private static class StoreBalancerState {
        final StoreBalancer balancer;
        final boolean isBuiltin;
        final AtomicReference<Struct> loadRules;
        final AtomicBoolean disabled = new AtomicBoolean(false);

        private StoreBalancerState(StoreBalancer balancer, boolean isBuiltin) {
            this.balancer = balancer;
            this.loadRules = new AtomicReference<>(balancer.initialLoadRules());
            this.isBuiltin = isBuiltin;
        }
    }

    static class MetricManager {

        private final Tags tags;
        private final Counter scheduleCount;
        private final Map<MetricsKey, CommandMetrics> metricsMap = new HashMap<>();

        public MetricManager(String localStoreId, String clusterId) {
            tags = Tags.of("storeId", localStoreId).and("clusterId", clusterId);
            scheduleCount = Counter.builder("basekv.balance.scheduled")
                .tags(tags)
                .register(Metrics.globalRegistry);
        }

        public CommandMetrics getCommandMetrics(String fromBalancer, String command) {
            MetricsKey metricsKey = MetricsKey.builder()
                .balancer(fromBalancer)
                .cmdName(command)
                .build();
            return metricsMap.computeIfAbsent(metricsKey,
                k -> new CommandMetrics(tags.and("balancer", k.balancer).and("cmd", k.cmdName)));
        }

        public void close() {
            Metrics.globalRegistry.remove(scheduleCount);
            metricsMap.values().forEach(CommandMetrics::clear);
        }

        @Builder
        private static class MetricsKey {
            private String balancer;
            private String cmdName;
        }

        static class CommandMetrics {
            Counter cmdSucceedCounter;
            Counter cmdFailedCounter;
            Timer cmdRunTimer;

            private CommandMetrics(Tags tags) {
                cmdSucceedCounter = Counter.builder("basekv.balance.cmd.succeed")
                    .tags(tags)
                    .register(Metrics.globalRegistry);
                cmdFailedCounter = Counter.builder("basekv.balance.cmd.failed")
                    .tags(tags)
                    .register(Metrics.globalRegistry);
                cmdRunTimer = Timer.builder("basekv.balance.cmd.run")
                    .tags(tags)
                    .register(Metrics.globalRegistry);
            }

            private void clear() {
                Metrics.globalRegistry.remove(cmdSucceedCounter);
                Metrics.globalRegistry.remove(cmdFailedCounter);
                Metrics.globalRegistry.remove(cmdRunTimer);
            }
        }
    }
}
