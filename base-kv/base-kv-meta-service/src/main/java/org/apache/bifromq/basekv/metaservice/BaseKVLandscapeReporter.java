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
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.proto.StoreKey;

@Slf4j
class BaseKVLandscapeReporter implements IBaseKVLandscapeReporter {
    private final String storeId;
    private final IBaseKVLandscapeCRDT landscapeCRDT;
    private final CompositeDisposable disposable = new CompositeDisposable();
    private volatile KVRangeStoreDescriptor latestDescriptor;

    BaseKVLandscapeReporter(String storeId, IBaseKVLandscapeCRDT landscapeCRDT) {
        this.storeId = storeId;
        this.landscapeCRDT = landscapeCRDT;
        disposable.add(Observable.combineLatest(
                landscapeCRDT.landscape(),
                landscapeCRDT.aliveReplicas(),
                (StoreDescriptorAndReplicas::new))
            .observeOn(IBaseKVMetaService.SHARED_SCHEDULER)
            .subscribe(this::houseKeep));
    }

    @Override
    public CompletableFuture<Void> report(KVRangeStoreDescriptor descriptor) {
        Optional<KVRangeStoreDescriptor> descriptorOnCRDT = landscapeCRDT.getStoreDescriptor(descriptor.getId());
        if (descriptorOnCRDT.isEmpty() || !descriptorOnCRDT.get().equals(descriptor)) {
            return landscapeCRDT.setStoreDescriptor(descriptor);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void stop() {
        landscapeCRDT.removeDescriptor(storeId).join();
        disposable.dispose();
    }

    private void houseKeep(StoreDescriptorAndReplicas storeDescriptorAndReplicas) {
        Map<StoreKey, KVRangeStoreDescriptor> storedDescriptors = storeDescriptorAndReplicas.descriptorMap;
        Set<ByteString> aliveReplicas = storeDescriptorAndReplicas.replicaIds;
        for (StoreKey storeKey : storedDescriptors.keySet()) {
            if (!aliveReplicas.contains(storeKey.getReplicaId())) {
                log.debug("store[{}] is not alive, remove its descriptor", storeKey.getStoreId());
                landscapeCRDT.removeDescriptor(storeKey);
            }
        }
        if (!storedDescriptors.containsKey(landscapeCRDT.toDescriptorKey(storeId))) {
            KVRangeStoreDescriptor latestDescriptor = this.latestDescriptor;
            if (latestDescriptor != null) {
                landscapeCRDT.setStoreDescriptor(latestDescriptor);
            }
        }
    }

    private record StoreDescriptorAndReplicas(Map<StoreKey, KVRangeStoreDescriptor> descriptorMap,
                                              Set<ByteString> replicaIds) {
    }
}
