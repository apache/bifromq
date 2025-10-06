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

import static org.apache.bifromq.starter.module.EngineConfUtil.buildDataEngineConf;
import static org.apache.bifromq.starter.module.EngineConfUtil.buildWALEngineConf;

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
import org.apache.bifromq.dist.server.IDistServer;
import org.apache.bifromq.dist.worker.IDistWorker;
import org.apache.bifromq.plugin.eventcollector.EventCollectorManager;
import org.apache.bifromq.plugin.resourcethrottler.ResourceThrottlerManager;
import org.apache.bifromq.plugin.settingprovider.SettingProviderManager;
import org.apache.bifromq.plugin.subbroker.ISubBrokerManager;
import org.apache.bifromq.starter.config.StandaloneConfig;
import org.apache.bifromq.starter.config.model.dist.DistServerConfig;
import org.apache.bifromq.starter.config.model.dist.DistWorkerConfig;
import org.apache.bifromq.sysprops.props.DistWorkerLoadEstimationWindowSeconds;

public class DistServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(new TypeLiteral<Optional<IDistServer>>() {
        }).toProvider(DistServerProvider.class).in(Singleton.class);
        bind(new TypeLiteral<Optional<IDistWorker>>() {
        }).toProvider(DistWorkerProvider.class).in(Singleton.class);
    }

    private static class DistServerProvider implements Provider<Optional<IDistServer>> {
        private final StandaloneConfig config;
        private final ServiceInjector injector;

        @Inject
        private DistServerProvider(StandaloneConfig config, ServiceInjector injector) {
            this.config = config;
            this.injector = injector;
        }

        @Override
        public Optional<IDistServer> get() {
            DistServerConfig serverConfig = config.getDistServiceConfig().getServer();
            if (!serverConfig.isEnable()) {
                return Optional.empty();
            }
            return Optional.of(IDistServer.builder()
                .rpcServerBuilder(injector.getInstance(RPCServerBuilder.class))
                .distWorkerClient(
                    injector.getInstance(Key.get(IBaseKVStoreClient.class, Names.named("distWorkerClient"))))
                .settingProvider(injector.getInstance(SettingProviderManager.class))
                .eventCollector(injector.getInstance(EventCollectorManager.class))
                .workerThreads(serverConfig.getWorkerThreads())
                .attributes(serverConfig.getAttributes())
                .defaultGroupTags(serverConfig.getDefaultGroups())
                .build());
        }
    }

    private static class DistWorkerProvider implements Provider<Optional<IDistWorker>> {
        private final StandaloneConfig config;
        private final ServiceInjector injector;

        @Inject
        private DistWorkerProvider(StandaloneConfig config, ServiceInjector injector) {
            this.config = config;
            this.injector = injector;
        }

        @Override
        public Optional<IDistWorker> get() {
            DistWorkerConfig workerConfig = config.getDistServiceConfig().getWorker();
            if (!workerConfig.isEnable()) {
                return Optional.empty();
            }

            return Optional.of(IDistWorker.builder()
                .rpcServerBuilder(injector.getInstance(RPCServerBuilder.class))
                .agentHost(injector.getInstance(IAgentHost.class))
                .metaService(injector.getInstance(IBaseKVMetaService.class))
                .eventCollector(injector.getInstance(EventCollectorManager.class))
                .resourceThrottler(injector.getInstance(ResourceThrottlerManager.class))
                .distClient(injector.getInstance(IDistClient.class))
                .distWorkerClient(
                    injector.getInstance(Key.get(IBaseKVStoreClient.class, Names.named("distWorkerClient"))))
                .workerThreads(workerConfig.getWorkerThreads())
                .tickerThreads(workerConfig.getTickerThreads())
                .bgTaskExecutor(
                    injector.getInstance(Key.get(ScheduledExecutorService.class, Names.named("bgTaskScheduler"))))
                .storeOptions(new KVRangeStoreOptions()
                    .setKvRangeOptions(new KVRangeOptions()
                        .setMaxWALFatchBatchSize(workerConfig.getMaxWALFetchSize())
                        .setCompactWALThreshold(workerConfig.getCompactWALThreshold()))
                    .setDataEngineConfigurator(buildDataEngineConf(workerConfig
                        .getDataEngineConfig(), "dist_data"))
                    .setWalEngineConfigurator(buildWALEngineConf(workerConfig
                        .getWalEngineConfig(), "dist_wal")))
                .minGCInterval(Duration.ofSeconds(workerConfig.getMinGCIntervalSeconds()))
                .maxGCInterval(Duration.ofSeconds(workerConfig.getMaxGCIntervalSeconds()))
                .bootstrapDelay(Duration.ofMillis(workerConfig.getBalanceConfig().getBootstrapDelayInMS()))
                .zombieProbeDelay(Duration.ofMillis(workerConfig.getBalanceConfig().getZombieProbeDelayInMS()))
                .balancerRetryDelay(Duration.ofMillis(workerConfig.getBalanceConfig().getRetryDelayInMS()))
                .balancerFactoryConfig(workerConfig.getBalanceConfig().getBalancers())
                .subBrokerManager(injector.getInstance(ISubBrokerManager.class))
                .settingProvider(injector.getInstance(SettingProviderManager.class))
                .loadEstimateWindow(Duration.ofSeconds(DistWorkerLoadEstimationWindowSeconds.INSTANCE.get()))
                .attributes(workerConfig.getAttributes())
                .build());
        }
    }
}
