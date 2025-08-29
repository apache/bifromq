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
import static org.apache.bifromq.basekv.metaservice.CRDTUtil.toBalancerStateProposalURI;

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
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;
import org.apache.bifromq.basecrdt.core.api.IMVReg;
import org.apache.bifromq.basecrdt.core.api.IORMap;
import org.apache.bifromq.basecrdt.core.api.MVRegOperation;
import org.apache.bifromq.basecrdt.core.api.ORMapOperation;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.proto.BalancerStateSnapshot;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

class BaseKVStoreBalancerStatesProposalCRDT implements IBaseKVStoreBalancerStatesProposalCRDT {
    private final String clusterId;
    private final Logger log;
    private final ICRDTService crdtService;
    // key: balancerClassFQN, value: BalancerState
    private final IORMap expectedBalancerStatesORMap;
    private final BehaviorSubject<Map<String, BalancerStateSnapshot>> expectedBalancerStatesSubject =
        BehaviorSubject.createDefault(emptyMap());
    private final CompositeDisposable disposable = new CompositeDisposable();

    BaseKVStoreBalancerStatesProposalCRDT(String clusterId, ICRDTService crdtService) {
        this.clusterId = clusterId;
        this.log = MDCLogger.getLogger(BaseKVStoreBalancerStatesProposalCRDT.class, "clusterId", clusterId);
        this.crdtService = crdtService;
        this.expectedBalancerStatesORMap = crdtService.host(toBalancerStateProposalURI(clusterId));
        disposable.add(expectedBalancerStatesORMap.inflation()
            .observeOn(IBaseKVMetaService.SHARED_SCHEDULER)
            .map(this::buildExpectedBalancerStateSnapshots)
            .subscribe(expectedBalancerStatesSubject::onNext));
    }

    @Override
    public String clusterId() {
        return clusterId;
    }

    public Observable<Map<String, BalancerStateSnapshot>> expectedBalancerStates() {
        return expectedBalancerStatesSubject.distinctUntilChanged();
    }

    @Override
    public Optional<BalancerStateSnapshot> expectedBalancerState(String balancerFactoryClassFQN) {
        Map<String, BalancerStateSnapshot> snapshotMap = expectedBalancerStatesSubject.getValue();
        if (snapshotMap == null || snapshotMap.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(snapshotMap.get(balancerFactoryClassFQN));
    }

    public CompletableFuture<Void> setBalancerState(String balancerFactoryClassFQN, boolean disable, Struct loadRules) {
        Map<String, BalancerStateSnapshot> snapshotMap = expectedBalancerStatesSubject.getValue();
        if (snapshotMap.containsKey(balancerFactoryClassFQN)) {
            BalancerStateSnapshot existing = snapshotMap.getOrDefault(balancerFactoryClassFQN,
                BalancerStateSnapshot.getDefaultInstance());
            if (existing.getDisable() == disable && existing.getLoadRules().equals(loadRules)) {
                return CompletableFuture.completedFuture(null);
            }
        }
        return expectedBalancerStatesORMap.execute(ORMapOperation.update(ByteString.copyFromUtf8(
                balancerFactoryClassFQN))
            .with(MVRegOperation.write(BalancerStateSnapshot.newBuilder()
                .setDisable(disable)
                .setLoadRules(loadRules)
                .setHlc(HLC.INST.get())
                .build().toByteString())));
    }

    public CompletableFuture<Void> removeBalancerState(String balancerFactoryClassFQN) {
        return expectedBalancerStatesORMap.execute(ORMapOperation.remove(ByteString.copyFromUtf8(
                balancerFactoryClassFQN))
            .of(CausalCRDTType.mvreg));
    }

    public void stop() {
        disposable.dispose();
        crdtService.stopHosting(expectedBalancerStatesORMap.id().getUri()).join();
    }

    private Map<String, BalancerStateSnapshot> buildExpectedBalancerStateSnapshots(long ts) {
        Map<String, BalancerStateSnapshot> balancerStatesMap = new HashMap<>();
        expectedBalancerStatesORMap.keys().forEachRemaining(ormapKey -> {
            String balancerClassFQN = ormapKey.key().toStringUtf8();
            Optional<BalancerStateSnapshot> balancerStateOpt = buildBalancerStateSnapshot(
                expectedBalancerStatesORMap.getMVReg(ormapKey.key()));
            balancerStateOpt.ifPresent(stateSnapshot -> balancerStatesMap.put(balancerClassFQN,
                stateSnapshot));
        });
        log.debug("Expected balancer states changed: {}", balancerStatesMap);
        return balancerStatesMap;
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
}
