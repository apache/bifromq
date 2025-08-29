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

import static java.util.Collections.emptyMap;
import static org.apache.bifromq.basekv.metaservice.CRDTUtil.parseDescriptorKey;
import static org.apache.bifromq.basekv.metaservice.CRDTUtil.toBalancerStateURI;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.bifromq.base.util.RendezvousHash;
import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;
import org.apache.bifromq.basecrdt.core.api.IMVReg;
import org.apache.bifromq.basecrdt.core.api.IORMap;
import org.apache.bifromq.basecrdt.core.api.MVRegOperation;
import org.apache.bifromq.basecrdt.core.api.ORMapOperation;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.proto.BalancerStateSnapshot;
import org.apache.bifromq.basekv.proto.StoreKey;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

class BaseKVStoreBalancerStatesCRDT implements IBaseKVStoreBalancerStatesCRDT {
    private final String clusterId;
    private final Logger log;
    private final ICRDTService crdtService;
    // key: storeId, value: Map of balancerClassFQN -> BalancerState
    private final IORMap balancerStatesByStoreORMap;
    private final BehaviorSubject<Map<StoreKey, Map<String, BalancerStateSnapshot>>> balancerStatesSubject =
        BehaviorSubject.createDefault(emptyMap());
    private final CompositeDisposable disposable = new CompositeDisposable();

    BaseKVStoreBalancerStatesCRDT(String clusterId, ICRDTService crdtService) {
        this.clusterId = clusterId;
        this.log = MDCLogger.getLogger(BaseKVStoreBalancerStatesCRDT.class, "clusterId", clusterId);
        this.crdtService = crdtService;
        this.balancerStatesByStoreORMap = crdtService.host(toBalancerStateURI(clusterId));
        disposable.add(balancerStatesByStoreORMap.inflation()
            .observeOn(IBaseKVMetaService.SHARED_SCHEDULER)
            .map(this::buildBalancerStateSnapshots)
            .subscribe(balancerStatesSubject::onNext));
        disposable.add(Observable.combineLatest(
                this.currentBalancerStates(),
                this.aliveReplicas(),
                (StateSnapshotsAndReplicas::new))
            .observeOn(IBaseKVMetaService.SHARED_SCHEDULER)
            .subscribe(this::houseKeep));
    }

    @Override
    public String clusterId() {
        return clusterId;
    }

    @Override
    public Observable<Long> refuteSignal() {
        return crdtService.refreshSignal();
    }

    public Observable<Set<ByteString>> aliveReplicas() {
        return crdtService.aliveReplicas(balancerStatesByStoreORMap.id().getUri())
            .map(replicas -> replicas.stream().map(Replica::getId).collect(Collectors.toSet()));
    }

    public Observable<Map<StoreKey, Map<String, BalancerStateSnapshot>>> currentBalancerStates() {
        return balancerStatesSubject.distinctUntilChanged();
    }

    public Map<String, BalancerStateSnapshot> getStoreBalancerStates(String storeId) {
        return balancerStatesSubject.getValue().getOrDefault(toDescriptorKey(storeId), emptyMap());
    }

    public CompletableFuture<Void> setStoreBalancerState(String storeId,
                                                         String balancerClassFQN,
                                                         boolean disable,
                                                         Struct loadRules) {
        StoreKey storeKey = toDescriptorKey(storeId);
        Map<String, BalancerStateSnapshot> snapshotMap = balancerStatesSubject.getValue()
            .getOrDefault(storeKey, emptyMap());
        if (snapshotMap.containsKey(balancerClassFQN)) {
            BalancerStateSnapshot existing = snapshotMap.getOrDefault(balancerClassFQN,
                BalancerStateSnapshot.getDefaultInstance());
            if (existing.getDisable() == disable && existing.getLoadRules().equals(loadRules)) {
                return CompletableFuture.completedFuture(null);
            }
        }
        IORMap balancerStatesORMap = balancerStatesByStoreORMap.getORMap(storeKey.toByteString());
        return balancerStatesORMap.execute(ORMapOperation.update(ByteString.copyFromUtf8(balancerClassFQN))
            .with(MVRegOperation.write(BalancerStateSnapshot.newBuilder()
                .setDisable(disable)
                .setLoadRules(loadRules)
                .setHlc(HLC.INST.get())
                .build().toByteString())));
    }

    public CompletableFuture<Void> removeStore(String storeId) {
        return removeStore(toDescriptorKey(storeId));
    }

    public CompletableFuture<Void> removeStore(StoreKey storeKey) {
        return balancerStatesByStoreORMap.execute(ORMapOperation.remove(storeKey.toByteString())
            .of(CausalCRDTType.ormap));
    }

    public StoreKey toDescriptorKey(String storeId) {
        return StoreKey.newBuilder()
            .setStoreId(storeId)
            .setReplicaId(balancerStatesByStoreORMap.id().getId())
            .build();
    }

    public void stop() {
        disposable.dispose();
        crdtService.stopHosting(balancerStatesByStoreORMap.id().getUri()).join();
    }

    private Optional<BalancerStateSnapshot> buildBalancerStateSnapshot(IMVReg mvReg) {
        List<BalancerStateSnapshot> l = Lists.newArrayList(
            Iterators.filter(Iterators.transform(mvReg.read(), b -> {
                try {
                    return BalancerStateSnapshot.parseFrom(b);
                } catch (InvalidProtocolBufferException e) {
                    log.error("Unable to parse BalancerState", e);
                    return null;
                }
            }), Objects::nonNull));
        l.sort((a, b) -> Long.compareUnsigned(b.getHlc(), a.getHlc()));
        return Optional.ofNullable(l.isEmpty() ? null : l.get(0));
    }

    private Map<StoreKey, Map<String, BalancerStateSnapshot>> buildBalancerStateSnapshots(long ts) {
        Map<StoreKey, Map<String, BalancerStateSnapshot>> currentBalancerStates = new HashMap<>();
        balancerStatesByStoreORMap.keys().forEachRemaining(storeIdKey ->
            balancerStatesByStoreORMap.getORMap(storeIdKey.key()).keys()
                .forEachRemaining(balancerClassKey -> {
                    String balancerClassFQN = balancerClassKey.key().toStringUtf8();
                    IMVReg mvReg = balancerStatesByStoreORMap.getORMap(storeIdKey.key())
                        .getMVReg(balancerClassKey.key());
                    Optional<BalancerStateSnapshot> snapshotOpt = buildBalancerStateSnapshot(mvReg);
                    snapshotOpt.ifPresent(state ->
                        currentBalancerStates.computeIfAbsent(parseDescriptorKey(storeIdKey.key()),
                                k -> new HashMap<>())
                            .put(balancerClassFQN, state));
                }));
        return currentBalancerStates;
    }

    private void houseKeep(StateSnapshotsAndReplicas stateSnapshotsAndReplicas) {
        Map<StoreKey, Map<String, BalancerStateSnapshot>> observed = stateSnapshotsAndReplicas.observed;
        Set<ByteString> aliveReplicas = stateSnapshotsAndReplicas.replicaIds;
        for (StoreKey storeKey : observed.keySet()) {
            if (!aliveReplicas.contains(storeKey.getReplicaId())
                && shouldClean(aliveReplicas, storeKey.getReplicaId())) {
                log.debug("store[{}] is not alive, remove its balancer states", storeKey.getStoreId());
                this.removeStore(storeKey);
            }
        }
    }

    private boolean shouldClean(Set<ByteString> aliveReplicas, ByteString failedReplicas) {
        // Choose cleaner deterministically from the identical aliveReplicas set across nodes.
        RendezvousHash<ByteString, ByteString> hash = RendezvousHash.<ByteString, ByteString>builder()
            .keyFunnel((from, into) -> into.putBytes(from.asReadOnlyByteBuffer()))
            .nodeFunnel((from, into) -> into.putBytes(from.asReadOnlyByteBuffer()))
            .nodes(aliveReplicas)
            .build();
        ByteString cleaner = hash.get(failedReplicas);
        return cleaner != null && cleaner.equals(balancerStatesByStoreORMap.id().getId());
    }

    private record StateSnapshotsAndReplicas(Map<StoreKey, Map<String, BalancerStateSnapshot>> observed,
                                             Set<ByteString> replicaIds) {
    }
}
