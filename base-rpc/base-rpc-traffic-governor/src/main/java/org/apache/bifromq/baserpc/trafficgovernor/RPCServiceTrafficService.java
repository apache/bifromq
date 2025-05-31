/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.baserpc.trafficgovernor;

import org.apache.bifromq.basecrdt.service.ICRDTService;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

class RPCServiceTrafficService implements IRPCServiceTrafficService {
    private final ICRDTService crdtService;
    private Map<String, RPCServiceTrafficManager> trafficManagerMap = new ConcurrentHashMap<>();

    public RPCServiceTrafficService(ICRDTService crdtService) {
        this.crdtService = crdtService;
    }

    @Override
    public Observable<Set<String>> services() {
        return crdtService.aliveCRDTs()
            .map(crdtUris -> crdtUris.stream()
                .filter(NameUtil::isLandscapeURI)
                .map(NameUtil::parseServiceUniqueName)
                .collect(Collectors.toSet()))
            .distinctUntilChanged()
            .observeOn(Schedulers.single());
    }

    @Override
    public IRPCServiceLandscape getServiceLandscape(String serviceUniqueName) {
        return getTrafficManager(serviceUniqueName);
    }

    @Override
    public IRPCServiceServerRegister getServerRegister(String serviceUniqueName) {
        return getTrafficManager(serviceUniqueName);
    }

    @Override
    public IRPCServiceTrafficGovernor getTrafficGovernor(String serviceUniqueName) {
        return getTrafficManager(serviceUniqueName);
    }

    private RPCServiceTrafficManager getTrafficManager(String serviceUniqueName) {
        return trafficManagerMap.computeIfAbsent(serviceUniqueName,
            k -> new RPCServiceTrafficManager(serviceUniqueName, crdtService));
    }

    @Override
    public void close() {
        trafficManagerMap.values().forEach(RPCServiceTrafficManager::close);
    }
}
