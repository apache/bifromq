/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.retain.client.IRetainClient;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.bifromq.apiserver.http.IHTTPRequestHandler;
import org.apache.bifromq.apiserver.http.IHTTPRequestHandlersFactory;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;

public final class RequestHandlersFactory implements IHTTPRequestHandlersFactory {
    private final Map<Class<? extends IHTTPRequestHandler>, IHTTPRequestHandler> handlers = new HashMap<>();

    public RequestHandlersFactory(IAgentHost agentHost,
                                  IRPCServiceTrafficService trafficService,
                                  IBaseKVMetaService metaService,
                                  ISessionDictClient sessionDictClient,
                                  IDistClient distClient,
                                  IInboxClient inboxClient,
                                  IRetainClient retainClient,
                                  ISettingProvider settingProvider) {
        register(new ListAllStoreHandler(metaService));
        register(new GetStoreLandscapeHandler(metaService, trafficService));
        register(new GetStoreRangesHandler(metaService));
        register(new GetLoadRulesHandler(metaService));
        register(new SetLoadRulesHandler(metaService));

        register(new GetTrafficRulesHandler(trafficService));
        register(new SetTrafficRulesHandler(trafficService));
        register(new UnsetTrafficRulesHandler(trafficService));

        register(new GetClusterHandler(agentHost));
        register(new ListAllServicesHandler(trafficService));
        register(new GetServiceLandscapeHandler(trafficService));
        register(new SetServerGroupTagsHandler(trafficService));

        register(new GetSessionInfoHandler(settingProvider, sessionDictClient));
        register(new KillHandler(settingProvider, sessionDictClient));
        register(new RetainHandler(settingProvider, retainClient));
        register(new ExpireRetainHandler(settingProvider, retainClient));
        register(new PubHandler(settingProvider, distClient));
        register(new SubHandler(settingProvider, sessionDictClient));
        register(new UnsubHandler(settingProvider, sessionDictClient));
        register(new ExpireSessionHandler(settingProvider, inboxClient));
    }

    @Override
    public Collection<IHTTPRequestHandler> build() {
        return handlers.values();
    }

    private void register(IHTTPRequestHandler handler) {
        handlers.put(handler.getClass(), handler);
    }
}
