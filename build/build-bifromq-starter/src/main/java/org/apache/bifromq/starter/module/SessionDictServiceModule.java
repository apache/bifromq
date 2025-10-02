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

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import jakarta.inject.Singleton;
import java.util.Optional;
import org.apache.bifromq.baserpc.server.RPCServerBuilder;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.mqtt.inbox.IMqttBrokerClient;
import org.apache.bifromq.sessiondict.server.ISessionDictServer;
import org.apache.bifromq.starter.config.StandaloneConfig;
import org.apache.bifromq.starter.config.model.dict.SessionDictServerConfig;

public class SessionDictServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(new TypeLiteral<Optional<ISessionDictServer>>() {
        }).toProvider(SessionDictServerProvider.class)
            .in(Singleton.class);
    }

    private static class SessionDictServerProvider implements Provider<Optional<ISessionDictServer>> {
        private final StandaloneConfig config;
        private final ServiceInjector injector;

        @Inject
        private SessionDictServerProvider(StandaloneConfig config, ServiceInjector injector) {
            this.config = config;
            this.injector = injector;
        }

        @Override
        public Optional<ISessionDictServer> get() {
            SessionDictServerConfig serverConfig = config.getSessionDictServiceConfig().getServer();
            if (!serverConfig.isEnable()) {
                return Optional.empty();
            }
            return Optional.of(ISessionDictServer.builder()
                .rpcServerBuilder(injector.getInstance(RPCServerBuilder.class))
                .mqttBrokerClient(injector.getInstance(IMqttBrokerClient.class))
                .inboxClient(injector.getInstance(IInboxClient.class))
                .workerThreads(serverConfig.getWorkerThreads())
                .attributes(serverConfig.getAttributes())
                .defaultGroupTags(serverConfig.getDefaultGroups())
                .build());
        }
    }
}
