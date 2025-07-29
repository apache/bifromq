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

package org.apache.bifromq.apiserver.http.handler;

import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.bifromq.apiserver.http.IHTTPRequestHandler;
import org.apache.bifromq.basekv.metaservice.IBaseKVLandscapeObserver;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;

abstract class AbstractLandscapeHandler implements IHTTPRequestHandler {
    protected final Map<String, IBaseKVLandscapeObserver> landscapeObservers = new ConcurrentHashMap<>();
    private final IBaseKVMetaService metaService;
    private final CompositeDisposable disposable = new CompositeDisposable();

    protected AbstractLandscapeHandler(IBaseKVMetaService metaService) {
        this.metaService = metaService;
    }

    @Override
    public void start() {
        disposable.add(metaService.clusterIds().subscribe(clusterIds -> {
            landscapeObservers.keySet().removeIf(clusterId -> !clusterIds.contains(clusterId));
            for (String clusterId : clusterIds) {
                landscapeObservers.computeIfAbsent(clusterId, metaService::landscapeObserver);
            }
        }));
    }

    @Override
    public void close() {
        disposable.dispose();
        landscapeObservers.values().forEach(IBaseKVLandscapeObserver::stop);
    }
}
