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

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;

class BaseKVLandscapeObserver implements IBaseKVLandscapeObserver {
    private final BehaviorSubject<Map<String, KVRangeStoreDescriptor>> landscapeSubject =
        BehaviorSubject.create();
    private final CompositeDisposable disposable = new CompositeDisposable();

    BaseKVLandscapeObserver(IBaseKVLandscapeCRDT landscapeCRDT) {
        disposable.add(landscapeCRDT.landscape()
            .map(descriptorMap -> {
                Map<String, KVRangeStoreDescriptor> descriptorMapByStoreId = new HashMap<>();
                descriptorMap.forEach((key, value) -> descriptorMapByStoreId.compute(key.getStoreId(), (k, v) -> {
                    if (v == null) {
                        return value;
                    }
                    return v.getHlc() > value.getHlc() ? v : value;
                }));
                return descriptorMapByStoreId;
            })
            .subscribe(landscapeSubject::onNext));
    }

    @Override
    public final Observable<Map<String, KVRangeStoreDescriptor>> landscape() {
        return landscapeSubject.distinctUntilChanged();
    }

    @Override
    public Optional<KVRangeStoreDescriptor> getStoreDescriptor(String storeId) {
        for (String key : landscapeSubject.getValue().keySet()) {
            if (key.equals(storeId)) {
                return Optional.of(landscapeSubject.getValue().get(key));
            }
        }
        return Optional.empty();
    }

    @Override
    public void stop() {
        disposable.dispose();
        landscapeSubject.onComplete();
    }
}
