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

package com.baidu.bifromq.basekv.balance;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basehookloader.BaseHookLoader;
import com.baidu.bifromq.basekv.balance.KVRangeBalanceController.MetricManager.CommandMetrics;
import com.baidu.bifromq.basekv.balance.command.BalanceCommand;
import com.baidu.bifromq.basekv.balance.command.ChangeConfigCommand;
import com.baidu.bifromq.basekv.balance.command.MergeCommand;
import com.baidu.bifromq.basekv.balance.command.SplitCommand;
import com.baidu.bifromq.basekv.balance.command.TransferLeadershipCommand;
import com.baidu.bifromq.basekv.balance.option.KVRangeBalanceControllerOptions;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.store.proto.ChangeReplicaConfigReply;
import com.baidu.bifromq.basekv.store.proto.ChangeReplicaConfigRequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeMergeReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeMergeRequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeSplitReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeSplitRequest;
import com.baidu.bifromq.basekv.store.proto.RecoverRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basekv.store.proto.TransferLeadershipReply;
import com.baidu.bifromq.basekv.store.proto.TransferLeadershipRequest;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Sample;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.reactivex.rxjava3.disposables.Disposable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Builder;
import org.slf4j.Logger;

public class KVRangeBalanceController {
    private enum State {
        Init,
        Started,
        Closed
    }

    private final KVRangeBalanceControllerOptions options;
    private final IBaseKVStoreClient storeClient;
    private final Cache<KVRangeId, Long> historyCommandCache;
    private final List<StoreBalancer> balancers = new ArrayList<>();
    private final ScheduledExecutorService executor;
    private final AtomicBoolean scheduling = new AtomicBoolean();
    private final AtomicReference<State> state = new AtomicReference<>(State.Init);
    private final boolean executorOwner;
    private Logger log;
    private MetricManager metricsManager;
    private Disposable descriptorSub;
    private ScheduledFuture<?> scheduledFuture;

    public KVRangeBalanceController(IBaseKVStoreClient storeClient,
                                    KVRangeBalanceControllerOptions balancerOptions,
                                    ScheduledExecutorService executor) {
        this.options = balancerOptions.toBuilder().balancers(balancerOptions.getBalancers().stream()
                .distinct()
                .collect(Collectors.toList())
            )
            .build();
        this.storeClient = storeClient;
        this.historyCommandCache = Caffeine.newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build();
        executorOwner = executor == null;
        if (executor == null) {
            this.executor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ScheduledThreadPoolExecutor(1,
                    EnvProvider.INSTANCE.newThreadFactory("balance-executor-" + storeClient.clusterId())),
                "balance-executor-" + storeClient.clusterId());
        } else {
            this.executor = executor;
        }
    }

    public void start(String localStoreId) {
        if (state.compareAndSet(State.Init, State.Started)) {
            log = new BalanceControllerLogger(storeClient.clusterId(), localStoreId, "balancer.logger");

            Map<String, IStoreBalancerFactory> balancerFactoryMap = BaseHookLoader.load(IStoreBalancerFactory.class);
            for (String factoryName : options.getBalancers()) {
                if (!balancerFactoryMap.containsKey(factoryName)) {
                    log.warn("Balancer factory[{}] not found", factoryName);
                    continue;
                }
                StoreBalancer balancer = balancerFactoryMap.get(factoryName).newBalancer(localStoreId);
                log.info("Balancer factory[{}] enabled for store[{}]", factoryName, localStoreId);
                balancers.add(balancer);
            }
            this.metricsManager = new MetricManager(localStoreId, storeClient.clusterId());
            log.info("Balancer start");
            descriptorSub = this.storeClient.describe()
                .distinctUntilChanged()
                .subscribe(sds -> executor.execute(() -> updateStoreDescriptors(sds)));
            scheduleLater(randomDelay(), TimeUnit.MILLISECONDS);
        }
    }

    public void stop() {
        if (state.compareAndSet(State.Started, State.Closed)) {
            descriptorSub.dispose();
            if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
            }
            if (executorOwner) {
                MoreExecutors.shutdownAndAwaitTermination(executor, 5, TimeUnit.SECONDS);
            }
        }
    }

    private void updateStoreDescriptors(Set<KVRangeStoreDescriptor> descriptors) {
        for (StoreBalancer balancer : balancers) {
            balancer.update(descriptors);
        }
        scheduleLater(randomDelay(), TimeUnit.MILLISECONDS);
    }

    private void scheduleLater(long delay, TimeUnit timeUnit) {
        if (state.get() == State.Started && scheduling.compareAndSet(false, true)) {
            scheduledFuture = executor.schedule(this::scheduleNow, delay, timeUnit);
        }
    }

    private long randomDelay() {
        return ThreadLocalRandom.current()
            .nextLong(options.getScheduleIntervalInMs(), options.getScheduleIntervalInMs() * 2);
    }

    private void scheduleNow() {
        metricsManager.scheduleCount.increment();
        for (StoreBalancer fromBalancer : balancers) {
            try {
                Optional<BalanceCommand> commandOpt = fromBalancer.balance();
                if (commandOpt.isPresent()) {
                    BalanceCommand commandToRun = commandOpt.get();
                    log.info("Balancer[{}] run command: {}", fromBalancer.getClass().getSimpleName(), commandToRun);
                    String balancerName = fromBalancer.getClass().getSimpleName();
                    String cmdName = commandToRun.getClass().getSimpleName();
                    Sample start = Timer.start();
                    runCommand(commandToRun)
                        .whenCompleteAsync((r, e) -> {
                            scheduling.set(false);
                            CommandMetrics metrics = metricsManager.getCommandMetrics(balancerName, cmdName);
                            if (e != null) {
                                log.error("Should not be here, error when run command", e);
                                metrics.cmdFailedCounter.increment();
                            } else {
                                log.info("Balancer command[{},{}] result: {}", fromBalancer.getClass().getSimpleName(),
                                    commandToRun, r);
                                if (r) {
                                    metrics.cmdSucceedCounter.increment();
                                    start.stop(metrics.cmdRunTimer);
                                } else {
                                    metrics.cmdFailedCounter.increment();
                                }
                            }
                            scheduleLater(randomDelay(), TimeUnit.MILLISECONDS);
                        }, executor);
                    return;
                }
            } catch (Throwable e) {
                log.warn("Run balancer[{}] failed", fromBalancer.getClass().getSimpleName(), e);
            }
        }
        // no command to run
        scheduling.set(false);
    }

    private CompletableFuture<Boolean> runCommand(BalanceCommand command) {
        if (command.getExpectedVer() != null) {
            Long prevCMDVer = historyCommandCache.getIfPresent(command.getKvRangeId());
            if (prevCMDVer != null && prevCMDVer >= command.getExpectedVer()) {
                log.warn("Command version is duplicated with prev one: {}", command);
                return CompletableFuture.completedFuture(false);
            }
        }
        return switch (command.type()) {
            case CHANGE_CONFIG -> {
                ChangeReplicaConfigRequest changeConfigRequest = ChangeReplicaConfigRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setKvRangeId(command.getKvRangeId())
                    .setVer(command.getExpectedVer())
                    .addAllNewVoters(((ChangeConfigCommand) command).getVoters())
                    .addAllNewLearners(((ChangeConfigCommand) command).getLearners())
                    .build();
                yield handleStoreReplyCode(command,
                    storeClient.changeReplicaConfig(command.getToStore(), changeConfigRequest)
                        .thenApply(ChangeReplicaConfigReply::getCode)
                );
            }
            case MERGE -> {
                KVRangeMergeRequest rangeMergeRequest = KVRangeMergeRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setVer(command.getExpectedVer())
                    .setMergerId(command.getKvRangeId())
                    .setMergeeId(((MergeCommand) command).getMergeeId())
                    .build();
                yield handleStoreReplyCode(command,
                    storeClient.mergeRanges(command.getToStore(), rangeMergeRequest)
                        .thenApply(KVRangeMergeReply::getCode));
            }
            case SPLIT -> {
                KVRangeSplitRequest kvRangeSplitRequest = KVRangeSplitRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setKvRangeId(command.getKvRangeId())
                    .setVer(command.getExpectedVer())
                    .setSplitKey(((SplitCommand) command).getSplitKey())
                    .build();
                yield handleStoreReplyCode(command,
                    storeClient.splitRange(command.getToStore(), kvRangeSplitRequest)
                        .thenApply(KVRangeSplitReply::getCode));
            }
            case TRANSFER_LEADERSHIP -> {
                TransferLeadershipRequest transferLeadershipRequest = TransferLeadershipRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setKvRangeId(command.getKvRangeId())
                    .setVer(command.getExpectedVer())
                    .setNewLeaderStore(((TransferLeadershipCommand) command).getNewLeaderStore())
                    .build();
                yield handleStoreReplyCode(command,
                    storeClient.transferLeadership(command.getToStore(), transferLeadershipRequest)
                        .thenApply(TransferLeadershipReply::getCode));
            }
            case RECOVERY -> {
                RecoverRequest recoverRequest = RecoverRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .build();
                yield storeClient.recover(command.getToStore(), recoverRequest)
                    .handle((r, e) -> {
                        if (e != null) {
                            log.error("Unexpected error when recover, req: {}", recoverRequest, e);
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
                        case CHANGE_CONFIG, SPLIT, MERGE ->
                            historyCommandCache.put(command.getKvRangeId(), command.getExpectedVer());
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
                    .register(io.micrometer.core.instrument.Metrics.globalRegistry);
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
