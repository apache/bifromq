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
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficGovernor;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

abstract class AbstractTrafficRulesHandler implements IHTTPRequestHandler {
    protected final Map<String, IRPCServiceTrafficGovernor> governorMap = new ConcurrentHashMap<>();
    private final IRPCServiceTrafficService trafficService;
    private final CompositeDisposable disposable = new CompositeDisposable();

    public AbstractTrafficRulesHandler(IRPCServiceTrafficService trafficService) {
        this.trafficService = trafficService;
    }

    @Override
    public void start() {
        disposable.add(trafficService.services().subscribe(serviceUniqueNames -> {
            governorMap.keySet().removeIf(serviceUniqueName -> !serviceUniqueNames.contains(serviceUniqueName));
            for (String serviceUniqueName : serviceUniqueNames) {
                if (isTrafficGovernable(serviceUniqueName)) {
                    governorMap.computeIfAbsent(tryShorten(serviceUniqueName),
                        k -> trafficService.getTrafficGovernor(serviceUniqueName));
                }
            }
        }));
    }

    @Override
    public void close() {
        disposable.dispose();
    }

    private boolean isTrafficGovernable(String serviceUniqueName) {
        return switch (serviceUniqueName) {
            case "distservice.DistService",
                 "inboxservice.InboxService",
                 "sessiondict.SessionDictService",
                 "retainservice.RetainService" -> true;
            case "mqttbroker.OnlineInboxBroker" -> false;
            default -> !serviceUniqueName.endsWith("@basekv.BaseKVStoreService");
        };
    }

    private String tryShorten(String serviceUniqueName) {
        return switch (serviceUniqueName) {
            case "distservice.DistService" -> "dist.service";
            case "inboxservice.InboxService" -> "inbox.service";
            case "sessiondict.SessionDictService" -> "sessiondict.service";
            case "retainservice.RetainService" -> "retain.service";
            default -> serviceUniqueName;
        };
    }
}
