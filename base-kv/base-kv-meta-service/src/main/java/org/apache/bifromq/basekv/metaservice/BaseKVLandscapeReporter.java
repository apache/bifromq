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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.proto.StoreKey;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

class BaseKVLandscapeReporter implements IBaseKVLandscapeReporter {
    private final Logger log;
    private final String storeId;
    private final IBaseKVLandscapeCRDT landscapeCRDT;
    private final CompositeDisposable disposable = new CompositeDisposable();
    private volatile KVRangeStoreDescriptor latestDescriptor;

    BaseKVLandscapeReporter(String storeId, IBaseKVLandscapeCRDT landscapeCRDT) {
        this.log = MDCLogger.getLogger(BaseKVLandscapeReporter.class, "clusterId", landscapeCRDT.clusterId(),
            "storeId", storeId);
        this.storeId = storeId;
        this.landscapeCRDT = landscapeCRDT;
        disposable.add(landscapeCRDT.landscape()
            .observeOn(IBaseKVMetaService.SHARED_SCHEDULER)
            .subscribe(this::afterInflation));
    }

    @Override
    public CompletableFuture<Void> report(KVRangeStoreDescriptor descriptor) {
        Optional<KVRangeStoreDescriptor> descriptorOnCRDT = landscapeCRDT.getStoreDescriptor(descriptor.getId());
        if (descriptorOnCRDT.isEmpty() || !descriptorOnCRDT.get().equals(descriptor)) {
            this.latestDescriptor = descriptor;
            return landscapeCRDT.setStoreDescriptor(descriptor);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Observable<Long> refreshSignal() {
        return landscapeCRDT.refreshSignal();
    }

    @Override
    public void stop() {
        landscapeCRDT.removeDescriptor(storeId).join();
        disposable.dispose();
    }

    private void afterInflation(Map<StoreKey, KVRangeStoreDescriptor> storedDescriptors) {
        if (!storedDescriptors.containsKey(landscapeCRDT.toDescriptorKey(storeId))) {
            KVRangeStoreDescriptor latestDescriptor = this.latestDescriptor;
            if (latestDescriptor != null) {
                log.debug("Rectify missing store descriptor");
                landscapeCRDT.setStoreDescriptor(latestDescriptor);
            }
        }
    }
}
