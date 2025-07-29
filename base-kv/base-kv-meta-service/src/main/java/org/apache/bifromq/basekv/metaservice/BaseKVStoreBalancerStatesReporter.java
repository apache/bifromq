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

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.proto.BalancerStateSnapshot;
import org.apache.bifromq.basekv.proto.StoreKey;

@Slf4j
class BaseKVStoreBalancerStatesReporter implements IBaseKVStoreBalancerStatesReporter {
    private final String storeId;
    private final IBaseKVStoreBalancerStatesCRDT statesCRDT;
    private final CompositeDisposable disposable = new CompositeDisposable();
    private final Map<String, BalancerState> latestState = new ConcurrentHashMap<>();

    BaseKVStoreBalancerStatesReporter(String storeId, IBaseKVStoreBalancerStatesCRDT statesCRDT) {
        this.storeId = storeId;
        this.statesCRDT = statesCRDT;
        disposable.add(Observable.combineLatest(
                statesCRDT.currentBalancerStates(),
                statesCRDT.aliveReplicas(),
                (StateSnapshotsAndReplicas::new))
            .observeOn(IBaseKVMetaService.SHARED_SCHEDULER)
            .subscribe(this::houseKeep));
    }

    @Override
    public CompletableFuture<Void> reportBalancerState(String balancerFactoryClassFQN,
                                                       boolean disable,
                                                       Struct loadRules) {
        Map<String, BalancerStateSnapshot> stateSnapshotMap = statesCRDT.getStoreBalancerStates(storeId);
        BalancerStateSnapshot stateSnapshot = stateSnapshotMap.get(balancerFactoryClassFQN);
        if (stateSnapshot == null
            || stateSnapshot.getDisable() != disable
            || !stateSnapshot.getLoadRules().equals(loadRules)) {
            latestState.put(balancerFactoryClassFQN, new BalancerState(disable, loadRules));
            return statesCRDT.setStoreBalancerState(storeId, balancerFactoryClassFQN, disable, loadRules);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void stop() {
        statesCRDT.removeStore(storeId).join();
        disposable.dispose();
    }

    private void houseKeep(StateSnapshotsAndReplicas stateSnapshotsAndReplicas) {
        Map<StoreKey, Map<String, BalancerStateSnapshot>> observed = stateSnapshotsAndReplicas.observed;
        Set<ByteString> aliveReplicas = stateSnapshotsAndReplicas.replicaIds;
        for (StoreKey storeKey : observed.keySet()) {
            if (!aliveReplicas.contains(storeKey.getReplicaId())) {
                log.debug("store[{}] is not alive, remove its balancer states", storeKey.getStoreId());
                statesCRDT.removeStore(storeKey);
            }
        }
        if (!observed.containsKey(statesCRDT.toDescriptorKey(storeId))) {
            latestState.forEach((balancerClassFQN, balancerState) ->
                statesCRDT.setStoreBalancerState(storeId, balancerClassFQN,
                    balancerState.enable(), balancerState.loadRules()));
        }
    }

    private record StateSnapshotsAndReplicas(Map<StoreKey, Map<String, BalancerStateSnapshot>> observed,
                                             Set<ByteString> replicaIds) {
    }

    private record BalancerState(boolean enable, Struct loadRules) {

    }
}
