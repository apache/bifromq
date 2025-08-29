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

import com.google.protobuf.Struct;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.bifromq.basekv.proto.BalancerStateSnapshot;
import org.apache.bifromq.basekv.proto.StoreKey;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

class BaseKVStoreBalancerStatesReporter implements IBaseKVStoreBalancerStatesReporter {
    private final Logger log;
    private final String storeId;
    private final IBaseKVStoreBalancerStatesCRDT statesCRDT;
    private final CompositeDisposable disposable = new CompositeDisposable();
    private final Map<String, BalancerState> latestState = new ConcurrentHashMap<>();

    BaseKVStoreBalancerStatesReporter(String storeId, IBaseKVStoreBalancerStatesCRDT statesCRDT) {
        this.log = MDCLogger.getLogger(BaseKVStoreBalancerStatesReporter.class, "clusterId", statesCRDT.clusterId(),
            "storeId", storeId);
        this.storeId = storeId;
        this.statesCRDT = statesCRDT;
        disposable.add(statesCRDT.currentBalancerStates()
            .observeOn(IBaseKVMetaService.SHARED_SCHEDULER)
            .subscribe(this::afterInflation));
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
    public Observable<Long> refreshSignal() {
        return statesCRDT.refuteSignal();
    }

    @Override
    public void stop() {
        statesCRDT.removeStore(storeId).join();
        disposable.dispose();
    }

    private void afterInflation(Map<StoreKey, Map<String, BalancerStateSnapshot>> observed) {
        if (!observed.containsKey(statesCRDT.toDescriptorKey(storeId))) {
            log.debug("Rectify missing store balancer states");
            latestState.forEach((balancerClassFQN, balancerState) ->
                statesCRDT.setStoreBalancerState(storeId, balancerClassFQN,
                    balancerState.enable(), balancerState.loadRules()));
        }
    }

    private record BalancerState(boolean enable, Struct loadRules) {

    }
}
