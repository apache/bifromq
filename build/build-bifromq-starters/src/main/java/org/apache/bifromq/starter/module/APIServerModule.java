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

package org.apache.bifromq.starter.module;

import static org.apache.bifromq.starter.module.SSLUtil.buildServerSslContext;

import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import org.apache.bifromq.plugin.settingprovider.SettingProviderManager;
import org.apache.bifromq.retain.client.IRetainClient;
import org.apache.bifromq.starter.config.StandaloneConfig;
import org.apache.bifromq.starter.config.model.api.APIServerConfig;
import com.google.common.base.Strings;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import io.netty.handler.ssl.SslContext;
import java.util.Optional;
import org.apache.bifromq.apiserver.APIServer;
import org.apache.bifromq.apiserver.IAPIServer;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;

public class APIServerModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(new TypeLiteral<Optional<IAPIServer>>() {
        }).toProvider(APIServerProvider.class)
            .asEagerSingleton();
    }

    private static class APIServerProvider implements Provider<Optional<IAPIServer>> {
        private final StandaloneConfig config;
        private final ServiceInjector injector;

        @Inject
        private APIServerProvider(StandaloneConfig config, ServiceInjector injector) {
            this.config = config;
            this.injector = injector;
        }

        @Override
        public Optional<IAPIServer> get() {
            APIServerConfig serverConfig = config.getApiServerConfig();
            if (!serverConfig.isEnable()) {
                return Optional.empty();
            }

            String apiHost = Strings.isNullOrEmpty(serverConfig.getHost()) ? "0.0.0.0" : serverConfig.getHost();
            SslContext sslContext = null;
            if (serverConfig.isEnableSSL()) {
                sslContext = buildServerSslContext(serverConfig.getSslConfig());
            }
            return Optional.of(APIServer.builder()
                .host(apiHost)
                .port(serverConfig.getHttpPort())
                .maxContentLength(serverConfig.getMaxContentLength())
                .workerThreads(serverConfig.getWorkerThreads())
                .sslContext(sslContext)
                .agentHost(injector.getInstance(IAgentHost.class))
                .trafficService(injector.getInstance(IRPCServiceTrafficService.class))
                .metaService(injector.getInstance(IBaseKVMetaService.class))
                .distClient(injector.getInstance(IDistClient.class))
                .inboxClient(injector.getInstance(IInboxClient.class))
                .sessionDictClient(injector.getInstance(ISessionDictClient.class))
                .retainClient(injector.getInstance(IRetainClient.class))
                .settingProvider(injector.getInstance(SettingProviderManager.class))
                .build());
        }
    }
}
