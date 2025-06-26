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

package org.apache.bifromq.starter.module;

import static org.apache.bifromq.starter.module.SSLUtil.buildServerSslContext;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import jakarta.inject.Singleton;
import java.util.Optional;
import org.apache.bifromq.baserpc.server.RPCServerBuilder;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.mqtt.IMQTTBroker;
import org.apache.bifromq.mqtt.IMQTTBrokerBuilder;
import org.apache.bifromq.plugin.authprovider.AuthProviderManager;
import org.apache.bifromq.plugin.clientbalancer.ClientBalancerManager;
import org.apache.bifromq.plugin.eventcollector.EventCollectorManager;
import org.apache.bifromq.plugin.resourcethrottler.ResourceThrottlerManager;
import org.apache.bifromq.plugin.settingprovider.SettingProviderManager;
import org.apache.bifromq.retain.client.IRetainClient;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;
import org.apache.bifromq.starter.config.StandaloneConfig;
import org.apache.bifromq.starter.config.model.mqtt.MQTTServerConfig;

public class MQTTServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(new TypeLiteral<Optional<IMQTTBroker>>() {
        }).toProvider(MQTTBrokerServerProvider.class).in(Singleton.class);
    }

    private static class MQTTBrokerServerProvider implements Provider<Optional<IMQTTBroker>> {
        private final StandaloneConfig config;
        private final ServiceInjector injector;

        @Inject
        private MQTTBrokerServerProvider(StandaloneConfig config, ServiceInjector injector) {
            this.config = config;
            this.injector = injector;
        }

        @Override
        public Optional<IMQTTBroker> get() {
            MQTTServerConfig serverConfig = config.getMqttServiceConfig().getServer();
            if (!serverConfig.isEnable()) {
                return Optional.empty();
            }
            IMQTTBrokerBuilder brokerBuilder = IMQTTBroker.builder()
                .rpcServerBuilder(injector.getInstance(RPCServerBuilder.class))
                .mqttBossELGThreads(serverConfig.getBossELGThreads())
                .mqttWorkerELGThreads(serverConfig.getWorkerELGThreads())
                .authProvider(injector.getInstance(AuthProviderManager.class))
                .clientBalancer(injector.getInstance(ClientBalancerManager.class))
                .eventCollector(injector.getInstance(EventCollectorManager.class))
                .resourceThrottler(injector.getInstance(ResourceThrottlerManager.class))
                .settingProvider(injector.getInstance(SettingProviderManager.class))
                .distClient(injector.getInstance(IDistClient.class))
                .inboxClient(injector.getInstance(IInboxClient.class))
                .sessionDictClient(injector.getInstance(ISessionDictClient.class))
                .retainClient(injector.getInstance(IRetainClient.class))
                .connectTimeoutSeconds(serverConfig.getConnTimeoutSec())
                .connectRateLimit(serverConfig.getMaxConnPerSec())
                .disconnectRate(serverConfig.getMaxDisconnPerSec())
                .readLimit(serverConfig.getMaxConnBandwidth())
                .writeLimit(serverConfig.getMaxConnBandwidth())
                .maxBytesInMessage(serverConfig.getMaxMsgByteSize());
            if (serverConfig.getTcpListener().isEnable()) {
                brokerBuilder.buildTcpConnListener()
                    .host(serverConfig.getTcpListener().getHost())
                    .port(serverConfig.getTcpListener().getPort())
                    .buildListener();
            }
            if (serverConfig.getTlsListener().isEnable()) {
                brokerBuilder.buildTLSConnListener()
                    .host(serverConfig.getTlsListener().getHost())
                    .port(serverConfig.getTlsListener().getPort())
                    .sslContext(buildServerSslContext(serverConfig.getTlsListener().getSslConfig()))
                    .buildListener();
            }
            if (serverConfig.getWsListener().isEnable()) {
                brokerBuilder.buildWSConnListener()
                    .host(serverConfig.getWsListener().getHost())
                    .port(serverConfig.getWsListener().getPort())
                    .path(serverConfig.getWsListener().getWsPath())
                    .buildListener();
            }
            if (serverConfig.getWssListener().isEnable()) {
                brokerBuilder.buildWSSConnListener()
                    .host(serverConfig.getWssListener().getHost())
                    .port(serverConfig.getWssListener().getPort())
                    .path(serverConfig.getWssListener().getWsPath())
                    .sslContext(buildServerSslContext(serverConfig.getWssListener().getSslConfig()))
                    .buildListener();
            }
            return Optional.of(brokerBuilder.build());
        }

    }
}
