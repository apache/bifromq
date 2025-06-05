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

package org.apache.bifromq.basekv.metaservice;

import static org.apache.bifromq.basekv.metaservice.NameUtil.toLandscapeURI;
import static org.apache.bifromq.basekv.metaservice.NameUtil.toLoadRulesProposalURI;
import static org.apache.bifromq.basekv.metaservice.NameUtil.toLoadRulesURI;
import static org.apache.bifromq.basekv.proto.ProposalResult.ACCEPTED;
import static org.apache.bifromq.basekv.proto.ProposalResult.REJECTED;

import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;
import org.apache.bifromq.basecrdt.core.api.IMVReg;
import org.apache.bifromq.basecrdt.core.api.IORMap;
import org.apache.bifromq.basecrdt.core.api.MVRegOperation;
import org.apache.bifromq.basecrdt.core.api.ORMapOperation;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.proto.DescriptorKey;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.proto.LoadRules;
import org.apache.bifromq.basekv.proto.LoadRulesProposition;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BaseKVClusterMetadataManager implements IBaseKVClusterMetadataManager {
    private static final Scheduler SHARED_SCHEDULER = Schedulers.from(
        ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), EnvProvider.INSTANCE.newThreadFactory("basekv-metadata-manager", true)),
            "basekv-metadata-manager"));
    private final ICRDTService crdtService;
    private final IORMap loadRulesORMap;
    private final IORMap loadRulesProposalORMap;
    private final IORMap landscapeORMap;
    private final Duration proposalTimeout;
    private final Subject<Map<String, Struct>> loadRulesSubject = BehaviorSubject.create();
    private final BehaviorSubject<Map<DescriptorKey, KVRangeStoreDescriptor>> landscapeSubject =
        BehaviorSubject.create();
    private final CompositeDisposable disposable = new CompositeDisposable();
    private volatile LoadRulesProposalHandler handler = null;

    public BaseKVClusterMetadataManager(String clusterId,
                                        ICRDTService crdtService,
                                        Duration proposalTimeout) {
        this.crdtService = crdtService;
        this.landscapeORMap = crdtService.host(toLandscapeURI(clusterId));
        this.loadRulesORMap = crdtService.host(toLoadRulesURI(clusterId));
        this.loadRulesProposalORMap = crdtService.host(toLoadRulesProposalURI(clusterId));
        this.proposalTimeout = proposalTimeout;
        disposable.add(landscapeORMap.inflation()
            .observeOn(SHARED_SCHEDULER)
            .map(this::buildLandscape)
            .subscribe(landscapeSubject::onNext));
        disposable.add(loadRulesORMap.inflation()
            .observeOn(SHARED_SCHEDULER)
            .map(this::buildBalancerLoadRules)
            .subscribe(loadRulesSubject::onNext));
        disposable.add(loadRulesProposalORMap.inflation()
            .observeOn(SHARED_SCHEDULER)
            .map(this::buildLoadRulesProposition)
            .map(m -> {
                Map<String, LoadRulesProposition> result = new HashMap<>();
                m.forEach((k, v) -> {
                    if (v.hasProposal()) {
                        result.put(k, v);
                    }
                });
                return result;
            })
            .scan((prev, curr) -> {
                if (prev == null) {
                    return curr;
                } else {
                    Map<String, LoadRulesProposition> result = new HashMap<>();
                    curr.forEach((k, v) -> {
                        if (!prev.containsKey(k) || !prev.get(k).equals(v)) {
                            result.put(k, v);
                        }
                    });
                    return result;
                }
            })
            .filter(m -> !m.isEmpty())
            .subscribe(this::handleProposals));
        disposable.add(
            Observable.combineLatest(landscapeSubject, crdtService.aliveReplicas(landscapeORMap.id().getUri())
                        .map(replicas -> replicas.stream().map(Replica::getId).collect(Collectors.toSet())),
                    (StoreDescriptorAndReplicas::new))
                .observeOn(SHARED_SCHEDULER)
                .subscribe(this::checkAndHealStoreDescriptorList));
    }

    @Override
    public Observable<Map<String, Struct>> loadRules() {
        return loadRulesSubject.distinctUntilChanged();
    }

    @Override
    public void setLoadRulesProposalHandler(LoadRulesProposalHandler handler) {
        this.handler = handler;
    }

    @Override
    public CompletableFuture<ProposalResult> proposeLoadRules(String balancerFactoryClass, Struct loadRules) {
        IMVReg balancerProposalMVReg = loadRulesProposalORMap.getMVReg(ByteString.copyFromUtf8(balancerFactoryClass));
        CompletableFuture<ProposalResult> resultFuture = new CompletableFuture<>();
        long now = HLC.INST.get();
        balancerProposalMVReg.inflation()
            .timeout(proposalTimeout.toMillis(), TimeUnit.MILLISECONDS)
            .mapOptional(ts -> {
                Optional<LoadRulesProposition> propositionOpt = buildLoadRulesProposition(balancerProposalMVReg);
                if (propositionOpt.isEmpty()) {
                    return Optional.of(ProposalResult.NO_BALANCER);
                }
                LoadRulesProposition loadRulesProposition = propositionOpt.get();
                if (loadRulesProposition.getHlc() == now) {
                    if (loadRulesProposition.hasProposalResult()) {
                        switch (loadRulesProposition.getProposalResult()) {
                            case ACCEPTED:
                                return Optional.of(ProposalResult.ACCEPTED);
                            default:
                                return Optional.of(ProposalResult.REJECTED);
                        }
                    }
                    return Optional.empty();
                } else if (loadRulesProposition.getHlc() > now) {
                    return Optional.of(ProposalResult.OVERRIDDEN);
                } else {
                    return Optional.empty();
                }
            })
            .take(1)
            .subscribe(resultFuture::complete, resultFuture::completeExceptionally);
        balancerProposalMVReg.execute(MVRegOperation.write(LoadRulesProposition.newBuilder()
            .setProposal(loadRules)
            .setHlc(now)
            .build().toByteString()));
        return resultFuture;
    }

    @Override
    public Observable<Map<String, KVRangeStoreDescriptor>> landscape() {
        return landscapeSubject
            .map(descriptorMap -> {
                Map<String, KVRangeStoreDescriptor> descriptorMapByStoreId = new HashMap<>();
                descriptorMap.forEach((key, value) -> descriptorMapByStoreId.compute(key.getStoreId(), (k, v) -> {
                    if (v == null) {
                        return value;
                    }
                    return v.getHlc() > value.getHlc() ? v : value;
                }));
                return descriptorMapByStoreId;
            });
    }

    @Override
    public Optional<KVRangeStoreDescriptor> getStoreDescriptor(String storeId) {
        for (DescriptorKey key : landscapeSubject.getValue().keySet()) {
            if (key.getStoreId().equals(storeId)) {
                return Optional.of(landscapeSubject.getValue().get(key));
            }
        }
        return Optional.empty();
    }

    @Override
    public CompletableFuture<Void> report(KVRangeStoreDescriptor descriptor) {
        DescriptorKey descriptorKey = toDescriptorKey(descriptor.getId());
        Optional<KVRangeStoreDescriptor> descriptorOnCRDT =
            buildLandscape(landscapeORMap.getMVReg(descriptorKey.toByteString()));
        if (descriptorOnCRDT.isEmpty() || !descriptorOnCRDT.get().equals(descriptor)) {
            return landscapeORMap.execute(ORMapOperation
                .update(descriptorKey.toByteString())
                .with(MVRegOperation.write(descriptor.toByteString())));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> stopReport(String storeId) {
        return removeDescriptorFromCRDT(toDescriptorKey(storeId));
    }

    public void stop() {
        disposable.dispose();
        loadRulesSubject.onComplete();
        landscapeSubject.onComplete();
        crdtService.stopHosting(landscapeORMap.id().getUri()).join();
        crdtService.stopHosting(loadRulesORMap.id().getUri()).join();
        crdtService.stopHosting(loadRulesProposalORMap.id().getUri()).join();
    }

    private void handleProposals(Map<String, LoadRulesProposition> proposals) {
        LoadRulesProposalHandler handler = this.handler;
        if (handler != null) {
            for (String balancerClassFQN : proposals.keySet()) {
                LoadRulesProposition proposition = proposals.get(balancerClassFQN);
                Struct loadRules = proposition.getProposal();
                switch (handler.handle(balancerClassFQN, loadRules)) {
                    case ACCEPTED -> {
                        loadRulesORMap.execute(ORMapOperation
                            .update(ByteString.copyFromUtf8(balancerClassFQN))
                            .with(MVRegOperation.write(LoadRules.newBuilder()
                                .setLoadRules(loadRules)
                                .setHlc(proposition.getHlc()) // keep same HLC
                                .build().toByteString())));
                        loadRulesProposalORMap.execute(ORMapOperation
                            .update(ByteString.copyFromUtf8(balancerClassFQN))
                            .with(MVRegOperation.write(LoadRulesProposition.newBuilder()
                                .setProposalResult(ACCEPTED)
                                .setHlc(proposition.getHlc()) // keep same HLC
                                .build().toByteString())));
                    }
                    case REJECTED -> loadRulesProposalORMap.execute(ORMapOperation
                        .update(ByteString.copyFromUtf8(balancerClassFQN))
                        .with(MVRegOperation.write(LoadRulesProposition.newBuilder()
                            .setProposalResult(REJECTED)
                            .setHlc(proposition.getHlc()) // keep same HLC
                            .build().toByteString())));
                    case NO_BALANCER -> loadRulesProposalORMap.execute(ORMapOperation
                        .remove(ByteString.copyFromUtf8(balancerClassFQN))
                        .of(CausalCRDTType.mvreg));
                }
            }
        }
    }

    private Map<String, Struct> buildBalancerLoadRules(long ts) {
        Map<String, Struct> loadRulesMap = new HashMap<>();
        loadRulesORMap.keys().forEachRemaining(ormapKey -> {
            String balancerClassFQN = ormapKey.key().toStringUtf8();
            Optional<LoadRules> loadRules = buildLoadRules(loadRulesORMap.getMVReg(ormapKey.key()));
            loadRules.ifPresent(rules -> loadRulesMap.put(balancerClassFQN, rules.getLoadRules()));
        });
        return loadRulesMap;
    }

    private Optional<LoadRules> buildLoadRules(IMVReg mvReg) {
        List<LoadRules> l = Lists.newArrayList(Iterators.filter(Iterators.transform(mvReg.read(), b -> {
            try {
                return LoadRules.parseFrom(b);
            } catch (InvalidProtocolBufferException e) {
                log.error("Unable to parse LoadRules", e);
                return null;
            }
        }), Objects::nonNull));
        l.sort((a, b) -> Long.compareUnsigned(b.getHlc(), a.getHlc()));
        return Optional.ofNullable(l.isEmpty() ? null : l.get(0));
    }

    private Map<String, LoadRulesProposition> buildLoadRulesProposition(long ts) {
        Map<String, LoadRulesProposition> loadRulesPropositionMap = new HashMap<>();
        loadRulesProposalORMap.keys().forEachRemaining(ormapKey ->
            buildLoadRulesProposition(loadRulesProposalORMap.getMVReg(ormapKey.key()))
                .ifPresent(proposition -> loadRulesPropositionMap.put(ormapKey.key().toStringUtf8(), proposition)));
        return loadRulesPropositionMap;
    }

    private Optional<LoadRulesProposition> buildLoadRulesProposition(IMVReg mvReg) {
        List<LoadRulesProposition> l = Lists.newArrayList(Iterators.filter(Iterators.transform(mvReg.read(), b -> {
            try {
                return LoadRulesProposition.parseFrom(b);
            } catch (InvalidProtocolBufferException e) {
                log.error("Unable to parse LoadRulesProposal", e);
                return null;
            }
        }), Objects::nonNull));
        l.sort((a, b) -> Long.compareUnsigned(b.getHlc(), a.getHlc()));
        return Optional.ofNullable(l.isEmpty() ? null : l.get(0));
    }

    private Map<DescriptorKey, KVRangeStoreDescriptor> buildLandscape(long ts) {
        Map<DescriptorKey, KVRangeStoreDescriptor> landscape = new HashMap<>();
        landscapeORMap.keys().forEachRemaining(ormapKey ->
            buildLandscape(landscapeORMap.getMVReg(ormapKey.key()))
                .ifPresent(descriptor -> landscape.put(parseDescriptorKey(ormapKey.key()), descriptor)));
        return landscape;
    }

    private Optional<KVRangeStoreDescriptor> buildLandscape(IMVReg mvReg) {
        List<KVRangeStoreDescriptor> l = Lists.newArrayList(Iterators.filter(Iterators.transform(mvReg.read(), b -> {
            try {
                return KVRangeStoreDescriptor.parseFrom(b);
            } catch (InvalidProtocolBufferException e) {
                log.error("Unable to parse KVRangeStoreDescriptor", e);
                return null;
            }
        }), Objects::nonNull));
        l.sort((a, b) -> Long.compareUnsigned(b.getHlc(), a.getHlc()));
        return Optional.ofNullable(l.isEmpty() ? null : l.get(0));
    }

    private void checkAndHealStoreDescriptorList(StoreDescriptorAndReplicas storeDescriptorAndReplicas) {
        Map<DescriptorKey, KVRangeStoreDescriptor> storedDescriptors = storeDescriptorAndReplicas.descriptorMap;
        Set<ByteString> aliveReplicas = storeDescriptorAndReplicas.replicaIds;
        for (DescriptorKey descriptorKey : storedDescriptors.keySet()) {
            if (!aliveReplicas.contains(descriptorKey.getReplicaId())) {
                log.debug("store[{}] is not alive, remove its storeDescriptor", descriptorKey.getStoreId());
                removeDescriptorFromCRDT(descriptorKey);
            }
        }
    }

    private CompletableFuture<Void> removeDescriptorFromCRDT(DescriptorKey key) {
        return landscapeORMap.execute(ORMapOperation.remove(key.toByteString()).of(CausalCRDTType.mvreg));
    }

    private DescriptorKey toDescriptorKey(String storeId) {
        return DescriptorKey.newBuilder()
            .setStoreId(storeId)
            .setReplicaId(landscapeORMap.id().getId())
            .build();
    }

    private DescriptorKey parseDescriptorKey(ByteString key) {
        try {
            return DescriptorKey.parseFrom(key);
        } catch (InvalidProtocolBufferException e) {
            log.error("Unable to parse DescriptorKey", e);
            return null;
        }
    }

    private record StoreDescriptorAndReplicas(Map<DescriptorKey, KVRangeStoreDescriptor> descriptorMap,
                                              Set<ByteString> replicaIds) {
    }
}
