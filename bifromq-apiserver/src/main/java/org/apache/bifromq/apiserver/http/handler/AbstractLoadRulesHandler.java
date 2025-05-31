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

package org.apache.bifromq.apiserver.http.handler;

import org.apache.bifromq.apiserver.http.IHTTPRequestHandler;
import org.apache.bifromq.basekv.metaservice.IBaseKVClusterMetadataManager;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

abstract class AbstractLoadRulesHandler implements IHTTPRequestHandler {
    protected final Map<String, IBaseKVClusterMetadataManager> metadataManagers = new ConcurrentHashMap<>();
    private final IBaseKVMetaService metaService;
    private final CompositeDisposable disposable = new CompositeDisposable();


    protected AbstractLoadRulesHandler(IBaseKVMetaService metaService) {
        this.metaService = metaService;
    }

    @Override
    public void start() {
        disposable.add(metaService.clusterIds().subscribe(clusterIds -> {
            metadataManagers.keySet().removeIf(clusterId -> !clusterIds.contains(clusterId));
            for (String clusterId : clusterIds) {
                metadataManagers.computeIfAbsent(clusterId, metaService::metadataManager);
            }
        }));
    }

    @Override
    public void close() {
        disposable.dispose();
    }
}
