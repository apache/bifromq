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
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.basekv.store.option.KVRangeOptions;
import org.apache.bifromq.basekv.store.option.KVRangeStoreOptions;
import org.apache.bifromq.baserpc.server.RPCServerBuilder;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.inbox.server.IInboxServer;
import org.apache.bifromq.inbox.store.IInboxStore;
import org.apache.bifromq.plugin.eventcollector.EventCollectorManager;
import org.apache.bifromq.plugin.resourcethrottler.ResourceThrottlerManager;
import org.apache.bifromq.plugin.settingprovider.SettingProviderManager;
import org.apache.bifromq.retain.client.IRetainClient;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;
import org.apache.bifromq.starter.config.StandaloneConfig;
import org.apache.bifromq.starter.config.model.inbox.InboxServerConfig;
import org.apache.bifromq.starter.config.model.inbox.InboxStoreConfig;

public class InboxServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(new TypeLiteral<Optional<IInboxServer>>() {
        }).toProvider(InboxServerProvider.class).in(Singleton.class);
        bind(new TypeLiteral<Optional<IInboxStore>>() {
        }).toProvider(InboxStoreProvider.class).in(Singleton.class);
    }

    private static class InboxServerProvider implements Provider<Optional<IInboxServer>> {
        private final StandaloneConfig config;
        private final ServiceInjector injector;

        @Inject
        private InboxServerProvider(StandaloneConfig config, ServiceInjector injector) {
            this.config = config;
            this.injector = injector;
        }

        @Override
        public Optional<IInboxServer> get() {
            InboxServerConfig serverConfig = config.getInboxServiceConfig().getServer();
            if (!serverConfig.isEnable()) {
                return Optional.empty();
            }
            return Optional.of(IInboxServer.builder()
                .rpcServerBuilder(injector.getInstance(RPCServerBuilder.class))
                .inboxClient(injector.getInstance(IInboxClient.class))
                .distClient(injector.getInstance(IDistClient.class))
                .inboxStoreClient(
                    injector.getInstance(Key.get(IBaseKVStoreClient.class, Names.named("inboxStoreClient"))))
                .workerThreads(serverConfig.getWorkerThreads())
                .attributes(serverConfig.getAttributes())
                .defaultGroupTags(serverConfig.getDefaultGroups())
                .build());
        }
    }

    private static class InboxStoreProvider implements Provider<Optional<IInboxStore>> {
        private final StandaloneConfig config;
        private final ServiceInjector injector;

        @Inject
        private InboxStoreProvider(StandaloneConfig config, ServiceInjector injector) {
            this.config = config;
            this.injector = injector;
        }

        @Override
        public Optional<IInboxStore> get() {
            InboxStoreConfig storeConfig = config.getInboxServiceConfig().getStore();
            if (!storeConfig.isEnable()) {
                return Optional.empty();
            }
            return Optional.of(IInboxStore.builder()
                .rpcServerBuilder(injector.getInstance(RPCServerBuilder.class))
                .agentHost(injector.getInstance(IAgentHost.class))
                .metaService(injector.getInstance(IBaseKVMetaService.class))
                .distClient(injector.getInstance(IDistClient.class))
                .inboxClient(injector.getInstance(IInboxClient.class))
                .retainClient(injector.getInstance(IRetainClient.class))
                .sessionDictClient(injector.getInstance(ISessionDictClient.class))
                .inboxStoreClient(
                    injector.getInstance(Key.get(IBaseKVStoreClient.class, Names.named("inboxStoreClient"))))
                .settingProvider(injector.getInstance(SettingProviderManager.class))
                .eventCollector(injector.getInstance(EventCollectorManager.class))
                .resourceThrottler(injector.getInstance(ResourceThrottlerManager.class))
                .tickerThreads(storeConfig.getTickerThreads())
                .workerThreads(storeConfig.getWorkerThreads())
                .bgTaskExecutor(
                    injector.getInstance(Key.get(ScheduledExecutorService.class, Names.named("bgTaskScheduler"))))
                .expireRateLimit(storeConfig.getExpireRateLimit())
                .minGCInterval(Duration.ofSeconds(storeConfig.getMinGCIntervalSeconds()))
                .maxGCInterval(Duration.ofSeconds(storeConfig.getMaxGCIntervalSeconds()))
                .bootstrapDelay(Duration.ofMillis(storeConfig.getBalanceConfig().getBootstrapDelayInMS()))
                .zombieProbeDelay(Duration.ofMillis(storeConfig.getBalanceConfig().getZombieProbeDelayInMS()))
                .balancerRetryDelay(Duration.ofMillis(
                    storeConfig.getBalanceConfig().getRetryDelayInMS()))
                .balancerFactoryConfig(
                    storeConfig.getBalanceConfig().getBalancers())
                .storeOptions(new KVRangeStoreOptions()
                    .setKvRangeOptions(new KVRangeOptions()
                        .setMaxWALFatchBatchSize(storeConfig.getMaxWALFetchSize())
                        .setCompactWALThreshold(storeConfig.getCompactWALThreshold())
                        .setEnableLoadEstimation(true))
                    .setSplitHinterFactoryConfig(storeConfig.getSplitHinterConfig().getHinters())
                    .setDataEngineType(storeConfig.getDataEngineConfig().getType())
                    .setDataEngineConf(storeConfig.getDataEngineConfig().toStruct())
                    .setWalEngineType(storeConfig.getWalEngineConfig().getType())
                    .setWalEngineConf(storeConfig.getWalEngineConfig().toStruct()))
                .attributes(storeConfig.getAttributes())
                .build());
        }
    }
}
