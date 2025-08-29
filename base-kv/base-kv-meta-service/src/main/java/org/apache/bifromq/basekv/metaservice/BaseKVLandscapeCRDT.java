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

import static org.apache.bifromq.basekv.metaservice.CRDTUtil.parseDescriptorKey;
import static org.apache.bifromq.basekv.metaservice.CRDTUtil.toLandscapeURI;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
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
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.proto.StoreKey;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

class BaseKVLandscapeCRDT implements IBaseKVLandscapeCRDT {
    private final String clusterId;
    private final Logger log;
    private final ICRDTService crdtService;
    private final IORMap landscapeORMap;
    private final BehaviorSubject<Map<StoreKey, KVRangeStoreDescriptor>> landscapeSubject = BehaviorSubject.create();
    private final CompositeDisposable disposable = new CompositeDisposable();

    BaseKVLandscapeCRDT(String clusterId, ICRDTService crdtService) {
        this.clusterId = clusterId;
        this.log = MDCLogger.getLogger(BaseKVLandscapeCRDT.class, "clusterId", clusterId);
        this.crdtService = crdtService;
        this.landscapeORMap = crdtService.host(toLandscapeURI(clusterId));
        disposable.add(landscapeORMap.inflation()
            .observeOn(IBaseKVMetaService.SHARED_SCHEDULER)
            .map(this::buildLandscape)
            .subscribe(landscapeSubject::onNext));
        disposable.add(Observable.combineLatest(landscape(), aliveReplicas(), (StoreDescriptorAndReplicas::new))
            .observeOn(IBaseKVMetaService.SHARED_SCHEDULER)
            .subscribe(this::houseKeep));
    }

    @Override
    public String clusterId() {
        return clusterId;
    }

    @Override
    public Observable<Long> refreshSignal() {
        return crdtService.refreshSignal();
    }

    public Observable<Set<ByteString>> aliveReplicas() {
        return crdtService.aliveReplicas(landscapeORMap.id().getUri())
            .map(replicas -> replicas.stream().map(Replica::getId).collect(Collectors.toSet()));
    }

    public Observable<Map<StoreKey, KVRangeStoreDescriptor>> landscape() {
        return landscapeSubject.distinctUntilChanged();
    }

    public Optional<KVRangeStoreDescriptor> getStoreDescriptor(String storeId) {
        StoreKey storeKey = toDescriptorKey(storeId);
        return buildLandscape(landscapeORMap.getMVReg(storeKey.toByteString()));
    }

    public CompletableFuture<Void> setStoreDescriptor(KVRangeStoreDescriptor descriptor) {
        StoreKey storeKey = toDescriptorKey(descriptor.getId());
        return landscapeORMap.execute(ORMapOperation.update(storeKey.toByteString())
            .with(MVRegOperation.write(descriptor.toByteString())));
    }

    public CompletableFuture<Void> removeDescriptor(StoreKey key) {
        return landscapeORMap.execute(ORMapOperation.remove(key.toByteString()).of(CausalCRDTType.mvreg));
    }

    public CompletableFuture<Void> removeDescriptor(String storeId) {
        StoreKey storeKey = toDescriptorKey(storeId);
        return landscapeORMap.execute(ORMapOperation.remove(storeKey.toByteString()).of(CausalCRDTType.mvreg));
    }

    public StoreKey toDescriptorKey(String storeId) {
        return StoreKey.newBuilder()
            .setStoreId(storeId)
            .setReplicaId(landscapeORMap.id().getId())
            .build();
    }

    public void stop() {
        disposable.dispose();
        crdtService.stopHosting(landscapeORMap.id().getUri()).join();
    }

    private Map<StoreKey, KVRangeStoreDescriptor> buildLandscape(long ts) {
        Map<StoreKey, KVRangeStoreDescriptor> landscape = new HashMap<>();
        landscapeORMap.keys().forEachRemaining(ormapKey -> buildLandscape(landscapeORMap.getMVReg(ormapKey.key()))
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

    private void houseKeep(StoreDescriptorAndReplicas storeDescriptorAndReplicas) {
        Map<StoreKey, KVRangeStoreDescriptor> storedDescriptors = storeDescriptorAndReplicas.descriptorMap;
        Set<ByteString> aliveReplicas = storeDescriptorAndReplicas.replicaIds;
        for (StoreKey storeKey : storedDescriptors.keySet()) {
            if (!aliveReplicas.contains(storeKey.getReplicaId())
                && shouldClean(aliveReplicas, storeKey.getReplicaId())) {
                log.debug("store[{}] is not alive, remove its descriptor", storeKey.getStoreId());
                removeDescriptor(storeKey);
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
        return cleaner != null && cleaner.equals(landscapeORMap.id().getId());
    }

    private record StoreDescriptorAndReplicas(Map<StoreKey, KVRangeStoreDescriptor> descriptorMap,
                                              Set<ByteString> replicaIds) {
    }
}
