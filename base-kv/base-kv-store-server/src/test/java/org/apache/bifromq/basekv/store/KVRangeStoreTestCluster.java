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

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.apache.bifromq.basekv.localengine.StructUtil.toValue;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_CHECKPOINT_ROOT_DIR;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_ROOT_DIR;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.awaitility.Awaitility.await;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.basekv.TestCoProcFactory;
import org.apache.bifromq.basekv.TestUtil;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.proto.StoreMessage;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.store.exception.KVRangeException;
import org.apache.bifromq.basekv.store.option.KVRangeStoreOptions;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;

@Slf4j
public class KVRangeStoreTestCluster {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR_NAME = "testDB_cp";
    private static final String DB_WAL_NAME = "testWAL";
    private static final String DB_WAL_CHECKPOINT_DIR = "testWAL_cp";
    private static final String CLUSTER = "test_cluster";
    private final String bootstrapStore;
    private final KVRangeId genesisKVRangeId;
    private final Map<String, String> storePathMap = Maps.newConcurrentMap();
    private final Map<String, KVRangeStore> rangeStoreMap = Maps.newConcurrentMap();
    private final Map<String, PublishSubject<StoreMessage>> rangeStoreMsgSourceMap = Maps.newConcurrentMap();
    private final Map<String, KVRangeStoreDescriptor> storeDescriptorMap = Maps.newConcurrentMap();
    private final Map<KVRangeId, KVRangeConfig> rangeConfigMap = Maps.newConcurrentMap();
    private final Map<String, Set<String>> cutMap = Maps.newConcurrentMap();
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final KVRangeStoreOptions optionsTpl;
    private final ExecutorService queryExecutor = new ThreadPoolExecutor(2, 2, 0L,
        TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
        EnvProvider.INSTANCE.newThreadFactory("query-executor"));
    private final ScheduledExecutorService bgTaskExecutor = new ScheduledThreadPoolExecutor(1,
        EnvProvider.INSTANCE.newThreadFactory("bg-task-executor"));

    private final List<NetworkRule> networkRules = Lists.newCopyOnWriteArrayList();
    private final List<InjectRule> injectRules = Lists.newCopyOnWriteArrayList();
    private final List<CaptureRule> captureRules = Lists.newCopyOnWriteArrayList();
    private final List<HoldRule> holdRules = Lists.newCopyOnWriteArrayList();

    private final Path dbRootDir;

    @SneakyThrows
    public KVRangeStoreTestCluster(KVRangeStoreOptions options) {
        this.optionsTpl = options.toBuilder().build();
        dbRootDir = Files.createTempDirectory("");
        bootstrapStore = buildStore(true);
        List<KVRangeDescriptor> list = rangeStoreMap.get(bootstrapStore).describe().blockingFirst().getRangesList();
        genesisKVRangeId = list.get(0).getId();
    }

    public String addStore() {
        return buildStore(false);
    }

    public void startStore(String storeId) {
        Preconditions.checkArgument(storePathMap.containsKey(storeId), "Unknown store %s", storeId);
        loadStore(storeId);
    }

    public void shutdownStore(String storeId) {
        checkStore(storeId);
        rangeStoreMap.remove(storeId).stop();
        rangeStoreMsgSourceMap.remove(storeId).onComplete();
    }

    public boolean isHosting(String storeId, KVRangeId rangeId) {
        checkStore(storeId);
        return rangeStoreMap.get(storeId).isHosting(rangeId);
    }

    public CompletionStage<Void> recover(String storeId, KVRangeId rangeId) {
        checkStore(storeId);
        return rangeStoreMap.get(storeId).recover(rangeId);
    }

    public String bootstrapStore() {
        return bootstrapStore;
    }

    public KVRangeId genesisKVRangeId() {
        return genesisKVRangeId;
    }

    public List<String> allStoreIds() {
        return Lists.newArrayList(rangeStoreMap.keySet());
    }

    public String leaderOf(KVRangeId kvRangeId) {
        checkKVRangeId(kvRangeId);
        return rangeConfigMap.get(kvRangeId).leader;
    }

    public boolean hasKVRange(String storeId, KVRangeId kvRangeId) {
        checkStore(storeId);
        return storeDescriptorMap.getOrDefault(storeId, KVRangeStoreDescriptor.getDefaultInstance())
            .getRangesList()
            .stream()
            .map(r -> r.getId())
            .collect(Collectors.toList())
            .contains(kvRangeId);
    }

    public KVRangeDescriptor getKVRange(String storeId, KVRangeId kvRangeId) {
        checkStore(storeId);
        return storeDescriptorMap
            .getOrDefault(storeId, KVRangeStoreDescriptor.getDefaultInstance())
            .getRangesList()
            .stream()
            .filter(r -> r.getId().equals(kvRangeId))
            .findFirst()
            .orElse(null);
    }

    public List<KVRangeId> allKVRangeIds() {
        return Lists.newArrayList(rangeConfigMap.keySet());
    }

    public KVRangeConfig kvRangeSetting(KVRangeId kvRangeId) {
        checkKVRangeId(kvRangeId);
        return rangeConfigMap.get(kvRangeId);
    }

    public void awaitKVRangeReady(String storeId, KVRangeId kvRangeId) {
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            KVRangeConfig kvRangeSetting = rangeConfigMap.get(kvRangeId);
            return hasKVRange(storeId, kvRangeId) && kvRangeSetting != null;
        });
    }

    public KVRangeConfig awaitKVRangeReady(String storeId, KVRangeId kvRangeId, long atLeastVer) {
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            KVRangeConfig kvRangeSetting = rangeConfigMap.get(kvRangeId);
            return hasKVRange(storeId, kvRangeId) && kvRangeSetting != null && kvRangeSetting.ver >= atLeastVer;
        });
        return rangeConfigMap.get(kvRangeId);
    }

    public KVRangeConfig awaitAllKVRangeReady(KVRangeId kvRangeId, long atLeastVer, long timeoutInSeconds) {
        await().atMost(Duration.ofSeconds(timeoutInSeconds)).until(() -> {
            boolean allReady = true;
            for (KVRangeStoreDescriptor storeDescriptor : storeDescriptorMap.values()) {
                boolean ready = false;
                for (KVRangeDescriptor rangeDescriptor : storeDescriptor.getRangesList()) {
                    if (rangeDescriptor.getId().equals(kvRangeId) && rangeDescriptor.getVer() >= atLeastVer) {
                        ready = true;
                        break;
                    }
                }
                allReady &= ready;
            }
            return allReady;
        });
        return rangeConfigMap.get(kvRangeId);
    }

    public void awaitKVRangeStateOnAllStores(KVRangeId kvRangeId, State.StateType state, long timeoutInSeconds) {
        await().atMost(Duration.ofSeconds(timeoutInSeconds)).until(() -> {
            boolean exists = false;
            for (KVRangeStoreDescriptor sd : storeDescriptorMap.values()) {
                for (KVRangeDescriptor rd : sd.getRangesList()) {
                    if (rd.getId().equals(kvRangeId)) {
                        exists = true;
                        if (rd.getState() != state) {
                            return false;
                        }
                    }
                }
            }
            return exists;
        });
    }

    public void awaitRangeAbsentAcrossStores(KVRangeId kvRangeId, long timeoutInSeconds) {
        await().atMost(Duration.ofSeconds(timeoutInSeconds)).until(() ->
            storeDescriptorMap.values().stream().noneMatch(sd ->
                sd.getRangesList().stream().anyMatch(rd -> rd.getId().equals(kvRangeId))));
    }

    public CompletionStage<Void> transferLeader(String storeId, long ver, KVRangeId kvRangeId, String newLeader) {
        checkStore(storeId);
        return rangeStoreMap.get(storeId).transferLeadership(ver, kvRangeId, newLeader);
    }

    public CompletionStage<Void> changeReplicaConfig(String storeId, long ver, KVRangeId kvRangeId,
                                                     Set<String> newVoters, Set<String> newLearners) {
        checkStore(storeId);
        return rangeStoreMap.get(storeId).changeReplicaConfig(ver, kvRangeId, newVoters, newLearners);
    }

    public CompletionStage<Void> purgeRange(KVRangeId kvRangeId) {
        checkKVRangeId(kvRangeId);
        KVRangeConfig cfg = rangeConfigMap.get(kvRangeId);
        return changeReplicaConfig(cfg.leader, cfg.ver, kvRangeId, emptySet(), emptySet())
            .thenRun(() -> awaitRangeAbsentAcrossStores(kvRangeId, 60));
    }

    public void cut(String fromStoreId, String toStoreId) {
        cutMap.computeIfAbsent(fromStoreId, k -> Sets.newConcurrentHashSet()).add(toStoreId);
    }

    public void uncut(String fromStoreId, String toStoreId) {
        cutMap.computeIfAbsent(fromStoreId, k -> Sets.newConcurrentHashSet()).remove(toStoreId);
    }

    public void isolate(String storeId) {
        Set<String> peers = allStoreIds().stream().filter(s -> !s.equals(storeId)).collect(Collectors.toSet());
        peers.forEach(peer -> {
            cut(storeId, peer);
            cut(peer, storeId);
        });
    }

    public void integrate(String storeId) {
        Set<String> peers = allStoreIds().stream().filter(s -> !s.equals(storeId)).collect(Collectors.toSet());
        peers.forEach(peer -> {
            uncut(storeId, peer);
            uncut(peer, storeId);
        });
    }

    public CompletionStage<Void> split(String storeId, long ver, KVRangeId kvRangeId, ByteString splitKey) {
        checkStore(storeId);
        return rangeStoreMap.get(storeId).split(ver, kvRangeId, splitKey);
    }

    public CompletionStage<Void> merge(String storeId, long ver, KVRangeId mergerId, KVRangeId mergeeId) {
        checkStore(storeId);
        // Prefer voters from cached leader settings; fallback to any descriptor; else empty (absent mergee fast path)
        Set<String> voters;
        KVRangeConfig mergeeCfg = rangeConfigMap.get(mergeeId);
        if (mergeeCfg != null) {
            voters = Sets.newHashSet(mergeeCfg.clusterConfig.getVotersList());
        } else {
            voters = storeDescriptorMap.values().stream()
                .flatMap(sd -> sd.getRangesList().stream())
                .filter(rd -> rd.getId().equals(mergeeId))
                .findFirst()
                .map(rd -> Sets.newHashSet(rd.getConfig().getVotersList()))
                .orElseGet(Sets::newHashSet);
        }
        return rangeStoreMap.get(storeId).merge(ver, mergerId, mergeeId, voters);
    }

    public CompletionStage<Void> mergeWithMergeeVoters(String storeId,
                                                       long ver,
                                                       KVRangeId mergerId,
                                                       KVRangeId mergeeId,
                                                       Set<String> explicitMergeeVoters) {
        checkStore(storeId);
        return rangeStoreMap.get(storeId).merge(ver, mergerId, mergeeId, explicitMergeeVoters);
    }

    public boolean exist(String storeId, KVRangeId kvRangeId, ByteString key) {
        checkStore(storeId);
        CompletableFuture<Boolean> onDone = new CompletableFuture<>();
        exist(storeId, kvRangeId, key, onDone);
        return onDone.join();
    }

    private void exist(String storeId, KVRangeId kvRangeId, ByteString key, CompletableFuture<Boolean> onDone) {
        checkStore(storeId);
        rangeStoreMap.get(storeId).exist(kvRangeSetting(kvRangeId).ver, kvRangeId, key, true)
            .whenComplete((v, e) -> {
                if (e != null) {
                    if (shouldRetry(e)) {
                        exist(storeId, kvRangeId, key, onDone);
                    } else {
                        onDone.completeExceptionally(e);
                    }
                } else {
                    onDone.complete(v);
                }
            });
    }

    public Optional<ByteString> get(String storeId, KVRangeId kvRangeId, ByteString key) {
        checkStore(storeId);
        CompletableFuture<Optional<ByteString>> onDone = new CompletableFuture<>();
        get(storeId, kvRangeId, key, onDone);
        return onDone.join();
    }

    private void get(String storeId, KVRangeId kvRangeId, ByteString key,
                     CompletableFuture<Optional<ByteString>> onDone) {
        checkStore(storeId);
        rangeStoreMap.get(storeId).get(kvRangeSetting(kvRangeId).ver, kvRangeId, key, true)
            .whenComplete((v, e) -> {
                if (e != null) {
                    if (shouldRetry(e)) {
                        get(storeId, kvRangeId, key, onDone);
                    } else {
                        onDone.completeExceptionally(e);
                    }
                } else {
                    onDone.complete(v);
                }
            });
    }

    public ByteString queryCoProc(String storeId, KVRangeId kvRangeId, ByteString key) {
        checkStore(storeId);
        CompletableFuture<ByteString> onDone = new CompletableFuture<>();
        queryCoProc(storeId, kvRangeId, key, onDone);
        return onDone.join();
    }

    private void queryCoProc(String storeId, KVRangeId kvRangeId, ByteString coProc,
                             CompletableFuture<ByteString> onDone) {
        checkStore(storeId);
        rangeStoreMap.get(storeId).queryCoProc(kvRangeSetting(kvRangeId).ver, kvRangeId, ROCoProcInput.newBuilder()
                .setRaw(coProc)
                .build(), true)
            .whenComplete((v, e) -> {
                if (e != null) {
                    if (shouldRetry(e)) {
                        queryCoProc(storeId, kvRangeId, coProc, onDone);
                    } else {
                        onDone.completeExceptionally(e);
                    }
                } else {
                    onDone.complete(v.getRaw());
                }
            });
    }

    public CompletionStage<ByteString> put(String storeId, long ver, KVRangeId kvRangeId, ByteString key,
                                           ByteString value) {
        return rangeStoreMap.get(storeId).put(ver, kvRangeId, key, value);
    }

    public ByteString put(String storeId, KVRangeId kvRangeId, ByteString key, ByteString value) {
        CompletableFuture<ByteString> onDone = new CompletableFuture<>();
        put(storeId, kvRangeId, key, value, onDone);
        return onDone.join();
    }

    private void put(String storeId, KVRangeId kvRangeId, ByteString key, ByteString value,
                     CompletableFuture<ByteString> onDone) {
        checkStore(storeId);
        rangeStoreMap.get(storeId).put(kvRangeSetting(kvRangeId).ver, kvRangeId, key, value)
            .whenComplete((v, e) -> {
                if (e != null) {
                    if (shouldRetry(e)) {
                        put(storeId, kvRangeId, key, value, onDone);
                    } else {
                        onDone.completeExceptionally(e);
                    }
                } else {
                    onDone.complete(v);
                }
            });
    }

    public ByteString delete(String storeId, KVRangeId kvRangeId, ByteString key) {
        CompletableFuture<ByteString> onDone = new CompletableFuture<>();
        delete(storeId, kvRangeId, key, onDone);
        return onDone.join();
    }

    private void delete(String storeId, KVRangeId kvRangeId, ByteString key, CompletableFuture<ByteString> onDone) {
        checkStore(storeId);
        rangeStoreMap.get(storeId).delete(kvRangeSetting(kvRangeId).ver, kvRangeId, key)
            .whenComplete((v, e) -> {
                if (e != null) {
                    if (shouldRetry(e)) {
                        delete(storeId, kvRangeId, key, onDone);
                    } else {
                        onDone.completeExceptionally(e);
                    }
                } else {
                    onDone.complete(v);
                }
            });
    }

    public ByteString mutateCoProc(String storeId, KVRangeId kvRangeId, ByteString key) {
        CompletableFuture<ByteString> onDone = new CompletableFuture<>();
        mutateCoProc(storeId, kvRangeId, key, onDone);
        return onDone.join();
    }

    private void mutateCoProc(String storeId, KVRangeId kvRangeId, ByteString key,
                              CompletableFuture<ByteString> onDone) {
        checkStore(storeId);
        rangeStoreMap.get(storeId)
            .mutateCoProc(kvRangeSetting(kvRangeId).ver, kvRangeId, RWCoProcInput.newBuilder().setRaw(key).build())
            .whenComplete((v, e) -> {
                if (e != null) {
                    if (shouldRetry(e)) {
                        mutateCoProc(storeId, kvRangeId, key, onDone);
                    } else {
                        onDone.completeExceptionally(e);
                    }
                } else {
                    onDone.complete(v.getRaw());
                }
            });
    }

    public void shutdown() {
        disposables.dispose();
        rangeStoreMap.values().forEach(IKVRangeStore::stop);
        TestUtil.deleteDir(dbRootDir.toString());
    }

    private String buildStore(boolean isBootstrap) {
        String uuid = UUID.randomUUID().toString();
        KVRangeStoreOptions options = optionsTpl.toBuilder().build();
        Struct dataConf = options.getDataEngineConf().toBuilder()
            .putFields(DB_ROOT_DIR, toValue(Paths.get(dbRootDir.toString(), DB_NAME, uuid).toString()))
            .putFields(DB_CHECKPOINT_ROOT_DIR,
                toValue(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR_NAME, uuid).toString()))
            .build();
        options.setDataEngineType(options.getDataEngineType());
        options.setDataEngineConf(dataConf);
        Struct walConf = options.getWalEngineConf().toBuilder()
            .putFields(DB_ROOT_DIR, toValue(Paths.get(dbRootDir.toString(), DB_WAL_NAME, uuid).toString()))
            .build();
        options.setWalEngineType(options.getWalEngineType());
        options.setWalEngineConf(walConf);
        KVRangeStore store = initStore(options);
        if (isBootstrap) {
            store.bootstrap(KVRangeIdUtil.generate(), FULL_BOUNDARY).join();
        }
        storePathMap.put(store.id(), uuid);
        return store.id();
    }

    private void loadStore(String storeId) {
        String uuid = storePathMap.get(storeId);
        KVRangeStoreOptions options = optionsTpl.toBuilder().build();
        if ("memory".equalsIgnoreCase(options.getWalEngineType())) {
            options.setOverrideIdentity(storeId);
        }
        Struct dataConf = options.getDataEngineConf().toBuilder()
            .putFields(DB_ROOT_DIR, toValue(Paths.get(dbRootDir.toString(), DB_NAME, uuid).toString()))
            .putFields(DB_CHECKPOINT_ROOT_DIR,
                toValue(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR_NAME, uuid).toString()))
            .build();
        options.setDataEngineType(options.getDataEngineType());
        options.setDataEngineConf(dataConf);
        Struct walConf = options.getWalEngineConf().toBuilder()
            .putFields(DB_ROOT_DIR, toValue(Paths.get(dbRootDir.toString(), DB_WAL_NAME, uuid).toString()))
            .build();
        options.setWalEngineType(options.getWalEngineType());
        options.setWalEngineConf(walConf);
        initStore(options);
    }

    private KVRangeStore initStore(KVRangeStoreOptions options) {
        int tickerThreads = 2;
        KVRangeStore store =
            new KVRangeStore(CLUSTER,
                options,
                new TestCoProcFactory(),
                queryExecutor,
                tickerThreads,
                bgTaskExecutor,
                emptyMap());
        PublishSubject<StoreMessage> storeMsgSource = PublishSubject.create();
        store.start(new IStoreMessenger() {
            @Override
            public void send(StoreMessage message) {
                deliver(message);
            }

            @Override
            public Observable<StoreMessage> receive() {
                return storeMsgSource.subscribeOn(Schedulers.io());
            }

            @Override
            public void close() {

            }
        });
        rangeStoreMap.put(store.id(), store);
        rangeStoreMsgSourceMap.put(store.id(), storeMsgSource);
        disposables.add(store.describe().subscribe(this::handleStoreDescriptor));
        return store;
    }

    private void deliver(StoreMessage message) {
        // hold first for deterministic delayed delivery
        for (HoldRule r : holdRules) {
            if (r.predicate.test(message)) {
                r.buffer.add(message);
                return;
            }
        }
        // capture first
        captureRules.forEach(r -> {
            if (!r.future.isDone() && r.predicate.test(message)) {
                r.future.complete(message);
                if (r.oneShot) {
                    captureRules.remove(r);
                }
            }
        });
        // inject once if triggered
        injectRules.forEach(r -> {
            if (r.predicate.test(message)) {
                injectRules.remove(r);
                StoreMessage injected = r.factory.apply(message);
                if (injected != null) {
                    deliver(injected);
                }
            }
        });
        if (message.getPayload().hasHostStoreId()) {
            if (shouldDrop(message)) {
                return;
            }
            long delayMs = delayMs(message);
            Runnable sendTask = () -> {
                if (rangeStoreMsgSourceMap.containsKey(message.getPayload().getHostStoreId())
                    && !cutMap.getOrDefault(message.getFrom(), emptySet())
                    .contains(message.getPayload().getHostStoreId())) {
                    rangeStoreMsgSourceMap.get(message.getPayload().getHostStoreId()).onNext(message);
                }
            };
            if (delayMs > 0) {
                bgTaskExecutor.schedule(sendTask, delayMs, TimeUnit.MILLISECONDS);
            } else {
                sendTask.run();
            }
        } else {
            rangeStoreMsgSourceMap.forEach((sid, msgSubject) -> {
                StoreMessage targetMsg = message.toBuilder()
                    .setPayload(message.getPayload().toBuilder()
                        .setHostStoreId(sid)
                        .build())
                    .build();
                if (shouldDrop(targetMsg)) {
                    return;
                }
                long delayMs = delayMs(targetMsg);
                Runnable sendTask = () -> msgSubject.onNext(targetMsg);
                if (delayMs > 0) {
                    bgTaskExecutor.schedule(sendTask, delayMs, TimeUnit.MILLISECONDS);
                } else {
                    sendTask.run();
                }
            });
        }
    }

    public AutoCloseable dropIf(Predicate<StoreMessage> predicate) {
        NetworkRule rule = NetworkRule.drop(predicate, false);
        networkRules.add(rule);
        return () -> networkRules.remove(rule);
    }

    public AutoCloseable dropOnceIf(Predicate<StoreMessage> predicate) {
        NetworkRule rule = NetworkRule.drop(predicate, true);
        networkRules.add(rule);
        return () -> networkRules.remove(rule);
    }

    public AutoCloseable delayIf(Predicate<StoreMessage> predicate, long delayMs) {
        NetworkRule rule = NetworkRule.delay(predicate, delayMs, false);
        networkRules.add(rule);
        return () -> networkRules.remove(rule);
    }

    public AutoCloseable delayOnceIf(Predicate<StoreMessage> predicate, long delayMs) {
        NetworkRule rule = NetworkRule.delay(predicate, delayMs, true);
        networkRules.add(rule);
        return () -> networkRules.remove(rule);
    }

    public void clearNetworkRules() {
        networkRules.clear();
    }

    public HoldHandle holdIf(Predicate<StoreMessage> predicate) {
        HoldRule rule = new HoldRule(predicate);
        holdRules.add(rule);
        return new HoldHandle(rule);
    }

    public AutoCloseable injectOnceIf(Predicate<StoreMessage> trigger,
                                      Function<StoreMessage, StoreMessage> factory) {
        InjectRule r = new InjectRule(trigger, factory, true);
        injectRules.add(r);
        return () -> injectRules.remove(r);
    }

    public CompletableFuture<StoreMessage> captureOnce(Predicate<StoreMessage> predicate) {
        CompletableFuture<StoreMessage> fut = new CompletableFuture<>();
        CaptureRule r = new CaptureRule(predicate, fut, true);
        captureRules.add(r);
        return fut;
    }

    private boolean shouldDrop(StoreMessage m) {
        for (NetworkRule r : networkRules) {
            if (r.action == NetworkRule.Action.DROP && r.predicate.test(m)) {
                if (r.oneShot) {
                    networkRules.remove(r);
                }
                return true;
            }
        }
        return false;
    }

    private long delayMs(StoreMessage m) {
        for (NetworkRule r : networkRules) {
            if (r.action == NetworkRule.Action.DELAY && r.predicate.test(m)) {
                if (r.oneShot) {
                    networkRules.remove(r);
                }
                return r.delayMs;
            }
        }
        return 0L;
    }

    private void handleStoreDescriptor(KVRangeStoreDescriptor storeDescriptor) {
        storeDescriptorMap.put(storeDescriptor.getId(), storeDescriptor);
        storeDescriptor.getRangesList().forEach(rangeDescriptor -> {
            if (rangeDescriptor.getRole() == RaftNodeStatus.Leader
                && (rangeDescriptor.getState() == State.StateType.Normal
                || rangeDescriptor.getState() == State.StateType.Merged)
                && rangeDescriptor.getConfig().getNextVotersCount() == 0
                && rangeDescriptor.getConfig().getNextLearnersCount() == 0) {
                KVRangeConfig settings = new KVRangeConfig(CLUSTER, storeDescriptor.getId(), rangeDescriptor);
                rangeConfigMap.compute(rangeDescriptor.getId(), (id, oldSettings) -> {
                    if (oldSettings != null) {
                        if (oldSettings.ver <= rangeDescriptor.getVer()) {
                            return settings;
                        }
                        return oldSettings;
                    }
                    return settings;
                });
            }
        });
    }

    private void checkStore(String storeId) {
        Preconditions.checkArgument(rangeStoreMap.containsKey(storeId));
    }

    private void checkKVRangeId(KVRangeId kvRangeId) {
        Preconditions.checkArgument(rangeConfigMap.containsKey(kvRangeId));
    }

    private boolean shouldRetry(Throwable e) {
        return e instanceof KVRangeException.TryLater ||
            e instanceof KVRangeException.BadVersion ||
            e.getCause() instanceof KVRangeException.TryLater ||
            e.getCause() instanceof KVRangeException.BadVersion;
    }

    private long reqId() {
        return System.nanoTime();
    }

    private static class NetworkRule {
        final Predicate<StoreMessage> predicate;
        final Action action;
        final long delayMs;
        final boolean oneShot;

        private NetworkRule(Predicate<StoreMessage> predicate, Action action, long delayMs,
                            boolean oneShot) {
            this.predicate = predicate;
            this.action = action;
            this.delayMs = delayMs;
            this.oneShot = oneShot;
        }

        static NetworkRule drop(Predicate<StoreMessage> predicate, boolean oneShot) {
            return new NetworkRule(predicate, Action.DROP, 0L, oneShot);
        }

        static NetworkRule delay(Predicate<StoreMessage> predicate, long delayMs, boolean oneShot) {
            return new NetworkRule(predicate, Action.DELAY, delayMs, oneShot);
        }

        enum Action { DROP, DELAY }
    }

    private static class InjectRule {
        final java.util.function.Predicate<StoreMessage> predicate;
        final java.util.function.Function<StoreMessage, StoreMessage> factory;
        final boolean oneShot;

        InjectRule(java.util.function.Predicate<StoreMessage> predicate,
                   java.util.function.Function<StoreMessage, StoreMessage> factory,
                   boolean oneShot) {
            this.predicate = predicate;
            this.factory = factory;
            this.oneShot = oneShot;
        }
    }

    private static class CaptureRule {
        final java.util.function.Predicate<StoreMessage> predicate;
        final java.util.concurrent.CompletableFuture<StoreMessage> future;
        final boolean oneShot;

        CaptureRule(java.util.function.Predicate<StoreMessage> predicate,
                    java.util.concurrent.CompletableFuture<StoreMessage> future,
                    boolean oneShot) {
            this.predicate = predicate;
            this.future = future;
            this.oneShot = oneShot;
        }
    }

    private static class HoldRule {
        final java.util.function.Predicate<StoreMessage> predicate;
        final java.util.concurrent.ConcurrentLinkedQueue<StoreMessage> buffer =
            new java.util.concurrent.ConcurrentLinkedQueue<>();

        HoldRule(java.util.function.Predicate<StoreMessage> predicate) {
            this.predicate = predicate;
        }
    }

    public final class HoldHandle implements AutoCloseable {
        private final HoldRule rule;

        private HoldHandle(HoldRule rule) {
            this.rule = rule;
        }

        public void releaseOne() {
            StoreMessage msg = rule.buffer.poll();
            if (msg != null) {
                // temporarily remove rule to avoid re-hold
                holdRules.remove(rule);
                try {
                    deliver(msg);
                } finally {
                    holdRules.add(rule);
                }
            }
        }

        public void releaseAll() {
            holdRules.remove(rule);
            StoreMessage msg;
            while ((msg = rule.buffer.poll()) != null) {
                deliver(msg);
            }
        }

        @Override
        public void close() {
            releaseAll();
        }
    }
}
