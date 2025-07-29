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
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basecrdt.service.ICRDTService;

@Slf4j
class BaseKVMetaService implements IBaseKVMetaService {
    private final ICRDTService crdtService;
    private final Map<String, IBaseKVLandscapeCRDT> landscapeCRDTs = new ConcurrentHashMap<>();
    private final Map<String, IBaseKVStoreBalancerStatesCRDT> balancerStatesCRDTs = new ConcurrentHashMap<>();
    private final Map<String, IBaseKVStoreBalancerStatesProposalCRDT> balancerStatesProposalCRDTs = new ConcurrentHashMap<>();

    BaseKVMetaService(ICRDTService crdtService) {
        this.crdtService = crdtService;
    }

    @Override
    public Observable<Set<String>> clusterIds() {
        return crdtService.aliveCRDTs().map(crdtUris -> crdtUris.stream()
                .filter(CRDTUtil::isLandscapeURI)
                .map(CRDTUtil::parseClusterId)
                .collect(Collectors.toSet()))
            .distinctUntilChanged()
            .observeOn(Schedulers.single());
    }

    @Override
    public IBaseKVStoreBalancerStatesProposer balancerStatesProposer(String clusterId) {
        IBaseKVStoreBalancerStatesProposalCRDT statesCRDT = balancerStatesProposalCRDTs.computeIfAbsent(clusterId,
            k -> new BaseKVStoreBalancerStatesProposalCRDT(clusterId, crdtService));
        return new BaseKVStoreBalancerStatesProposer(statesCRDT);
    }

    @Override
    public IBaseKVStoreBalancerStatesProposal balancerStatesProposal(String clusterId) {
        IBaseKVStoreBalancerStatesProposalCRDT statesCRDT = balancerStatesProposalCRDTs.computeIfAbsent(clusterId,
            k -> new BaseKVStoreBalancerStatesProposalCRDT(clusterId, crdtService));
        return new BaseKVStoreBalancerStatesProposal(statesCRDT);
    }

    @Override
    public IBaseKVLandscapeObserver landscapeObserver(String clusterId) {
        IBaseKVLandscapeCRDT landscapeCRDT = landscapeCRDTs.computeIfAbsent(clusterId,
            k -> new BaseKVLandscapeCRDT(clusterId, crdtService));
        return new BaseKVLandscapeObserver(landscapeCRDT);
    }

    @Override
    public IBaseKVLandscapeReporter landscapeReporter(String clusterId, String storeId) {
        IBaseKVLandscapeCRDT landscapeCRDT = landscapeCRDTs.computeIfAbsent(clusterId,
            k -> new BaseKVLandscapeCRDT(clusterId, crdtService));
        return new BaseKVLandscapeReporter(storeId, landscapeCRDT);
    }

    @Override
    public IBaseKVStoreBalancerStatesObserver balancerStatesObserver(String clusterId) {
        IBaseKVStoreBalancerStatesCRDT balancerStatesCRDT = balancerStatesCRDTs.computeIfAbsent(clusterId,
            k -> new BaseKVStoreBalancerStatesCRDT(clusterId, crdtService));
        return new BaseKVStoreBalancerStatesObserver(balancerStatesCRDT);
    }

    @Override
    public IBaseKVStoreBalancerStatesReporter balancerStatesReporter(String clusterId, String storeId) {
        IBaseKVStoreBalancerStatesCRDT balancerStatesCRDT = balancerStatesCRDTs.computeIfAbsent(clusterId,
            k -> new BaseKVStoreBalancerStatesCRDT(clusterId, crdtService));
        return new BaseKVStoreBalancerStatesReporter(storeId, balancerStatesCRDT);
    }

    @Override
    public void close() {
        landscapeCRDTs.values().forEach(IBaseKVLandscapeCRDT::stop);
        balancerStatesCRDTs.values().forEach(IBaseKVStoreBalancerStatesCRDT::stop);
        balancerStatesProposalCRDTs.values().forEach(IBaseKVStoreBalancerStatesProposalCRDT::stop);
    }
}
