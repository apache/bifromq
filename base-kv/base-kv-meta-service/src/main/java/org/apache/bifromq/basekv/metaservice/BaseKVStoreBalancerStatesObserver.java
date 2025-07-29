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

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.util.HashMap;
import java.util.Map;
import org.apache.bifromq.basekv.proto.BalancerStateSnapshot;

class BaseKVStoreBalancerStatesObserver implements IBaseKVStoreBalancerStatesObserver {
    private final BehaviorSubject<Map<String, Map<String, BalancerStateSnapshot>>> currentBalancerStatesSubject =
        BehaviorSubject.createDefault(emptyMap());
    private final CompositeDisposable disposable = new CompositeDisposable();

    BaseKVStoreBalancerStatesObserver(IBaseKVStoreBalancerStatesCRDT statesCRDT) {
        disposable.add(statesCRDT.currentBalancerStates()
            .observeOn(IBaseKVMetaService.SHARED_SCHEDULER)
            .map(statesMap -> {
                Map<String, Map<String, BalancerStateSnapshot>> currentStates = new HashMap<>();
                statesMap.forEach((storeKey, balancerStates) ->
                    currentStates.compute(storeKey.getStoreId(), (k, v) -> {
                        if (v == null) {
                            return balancerStates;
                        }
                        // Merge balancer states, prefer the one with higher HLC
                        if (greatestHLC(v) >= greatestHLC(balancerStates)) {
                            return v;
                        }
                        return balancerStates;
                    }));
                return currentStates;
            })
            .subscribe(currentBalancerStatesSubject::onNext));
    }

    private long greatestHLC(Map<String, BalancerStateSnapshot> balancerStates) {
        return balancerStates.values().stream()
            .mapToLong(BalancerStateSnapshot::getHlc)
            .max()
            .orElse(0L);
    }

    @Override
    public Observable<Map<String, Map<String, BalancerStateSnapshot>>> currentBalancerStates() {
        return currentBalancerStatesSubject.distinctUntilChanged();
    }

    @Override
    public void stop() {
        disposable.dispose();
        currentBalancerStatesSubject.onComplete();
    }
}
