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

import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import org.apache.bifromq.retain.client.IRetainClient;
import org.apache.bifromq.retain.store.IRetainStore;
import org.apache.bifromq.starter.config.StandaloneConfig;
import org.apache.bifromq.starter.config.model.dist.DistWorkerClientConfig;
import org.apache.bifromq.starter.config.model.inbox.InboxStoreConfig;
import org.apache.bifromq.starter.config.model.retain.RetainStoreClientConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Names;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.Optional;
import org.apache.bifromq.basecluster.AgentHostOptions;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basecrdt.service.CRDTServiceOptions;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.dist.worker.IDistWorker;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.inbox.store.IInboxStore;
import org.apache.bifromq.mqtt.inbox.IMqttBrokerClient;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;

public class CoreServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(SharedResourcesHolder.class).toProvider(SharedCloseableResourcesProvider.class).in(Singleton.class);
        bind(IAgentHost.class).toProvider(AgentHostProvider.class).in(Singleton.class);
        bind(ICRDTService.class).toProvider(CRDTServiceProvider.class).in(Singleton.class);
        bind(IBaseKVMetaService.class).toProvider(BaseKVMetaServiceProvider.class).in(Singleton.class);
        bind(IRPCServiceTrafficService.class).toProvider(RPCServiceTrafficServiceProvider.class).in(Singleton.class);

        bind(IDistClient.class).toProvider(DistClientProvider.class).in(Singleton.class);
        bind(IBaseKVStoreClient.class)
            .annotatedWith(Names.named("distWorkerClient"))
            .toProvider(DistWorkerClientProvider.class)
            .in(Singleton.class);

        bind(IInboxClient.class).toProvider(InboxClientProvider.class).in(Singleton.class);
        bind(IBaseKVStoreClient.class)
            .annotatedWith(Names.named("inboxStoreClient"))
            .toProvider(InboxStoreClientProvider.class)
            .in(Singleton.class);

        bind(IRetainClient.class).toProvider(RetainClientProvider.class).in(Singleton.class);
        bind(IBaseKVStoreClient.class)
            .annotatedWith(Names.named("retainStoreClient"))
            .toProvider(RetainStoreClientProvider.class)
            .in(Singleton.class);

        bind(ISessionDictClient.class).toProvider(SessionDictClientProvider.class).in(Singleton.class);

        bind(IMqttBrokerClient.class).toProvider(MQTTBrokerClientProvider.class).in(Singleton.class);

    }

    private static class SharedCloseableResourcesProvider implements Provider<SharedResourcesHolder> {

        @Override
        public SharedResourcesHolder get() {
            return new SharedResourcesHolder();
        }
    }

    private static class AgentHostProvider extends SharedResourceProvider<IAgentHost> {
        private final StandaloneConfig config;

        @Inject
        private AgentHostProvider(StandaloneConfig config, SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.config = config;
        }

        @Override
        public IAgentHost share() {
            AgentHostOptions agentHostOptions = AgentHostOptions.builder()
                .env(config.getClusterConfig().getEnv())
                .addr(config.getClusterConfig().getHost())
                .port(config.getClusterConfig().getPort())
                .build();
            return IAgentHost.newInstance(agentHostOptions);
        }
    }

    private static class CRDTServiceProvider extends SharedResourceProvider<ICRDTService> {
        private final StandaloneConfig config;
        private final IAgentHost agentHost;

        @Inject
        private CRDTServiceProvider(StandaloneConfig config,
                                    IAgentHost agentHost,
                                    SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.config = config;
            this.agentHost = agentHost;
        }

        @Override
        public ICRDTService share() {
            return ICRDTService.newInstance(agentHost, CRDTServiceOptions.builder().build());
        }
    }

    private static class BaseKVMetaServiceProvider extends SharedResourceProvider<IBaseKVMetaService> {
        private final ICRDTService crdtService;

        @Inject
        private BaseKVMetaServiceProvider(ICRDTService crdtService, SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.crdtService = crdtService;
        }

        @Override
        public IBaseKVMetaService share() {
            return IBaseKVMetaService.newInstance(crdtService);
        }
    }

    private static class RPCServiceTrafficServiceProvider extends SharedResourceProvider<IRPCServiceTrafficService> {
        private final ICRDTService crdtService;

        @Inject
        private RPCServiceTrafficServiceProvider(ICRDTService crdtService,
                                                 SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.crdtService = crdtService;
        }

        @Override
        public IRPCServiceTrafficService share() {
            return IRPCServiceTrafficService.newInstance(crdtService);
        }
    }

    private static class DistClientProvider extends SharedResourceProvider<IDistClient> {
        private final StandaloneConfig config;
        private final EventLoopGroup eventLoopGroup;
        private final SslContext sslContext;
        private final IRPCServiceTrafficService trafficService;

        @Inject
        private DistClientProvider(StandaloneConfig config,
                                   @Named("rpcClientEventLoop") EventLoopGroup eventLoopGroup,
                                   @Named("rpcClientSSLContext") Optional<SslContext> sslContext,
                                   IRPCServiceTrafficService trafficService,
                                   SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.config = config;
            this.eventLoopGroup = eventLoopGroup;
            this.sslContext = sslContext.orElse(null);
            this.trafficService = trafficService;
        }

        @Override
        public IDistClient share() {
            return IDistClient.newBuilder()
                .workerThreads(config.getDistServiceConfig().getClient().getWorkerThreads())
                .trafficService(trafficService)
                .eventLoopGroup(eventLoopGroup)
                .sslContext(sslContext)
                .build();
        }
    }

    private static class DistWorkerClientProvider extends SharedResourceProvider<IBaseKVStoreClient> {
        private final StandaloneConfig config;
        private final EventLoopGroup eventLoopGroup;
        private final IRPCServiceTrafficService trafficService;
        private final IBaseKVMetaService metaService;
        private final SslContext rpcClientSSLContext;

        @Inject
        private DistWorkerClientProvider(StandaloneConfig config,
                                         IRPCServiceTrafficService trafficService,
                                         IBaseKVMetaService metaService,
                                         @Named("rpcClientSSLContext") Optional<SslContext> rpcClientSSLContext,
                                         @Named("rpcClientEventLoop") EventLoopGroup eventLoopGroup,
                                         SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.config = config;
            this.trafficService = trafficService;
            this.metaService = metaService;
            this.rpcClientSSLContext = rpcClientSSLContext.orElse(null);
            this.eventLoopGroup = eventLoopGroup;
        }

        @Override
        public IBaseKVStoreClient share() {
            DistWorkerClientConfig workerConfig = config.getDistServiceConfig().getWorkerClient();
            return IBaseKVStoreClient.newBuilder()
                .clusterId(IDistWorker.CLUSTER_NAME)
                .trafficService(trafficService)
                .metaService(metaService)
                .workerThreads(workerConfig.getWorkerThreads())
                .eventLoopGroup(eventLoopGroup)
                .sslContext(rpcClientSSLContext)
                .queryPipelinesPerStore(workerConfig.getQueryPipelinePerStore())
                .build();
        }
    }

    private static class InboxClientProvider extends SharedResourceProvider<IInboxClient> {
        private final EventLoopGroup eventLoopGroup;
        private final SslContext sslContext;
        private final IRPCServiceTrafficService trafficService;

        @Inject
        private InboxClientProvider(@Named("rpcClientEventLoop") EventLoopGroup eventLoopGroup,
                                    @Named("rpcClientSSLContext") Optional<SslContext> sslContext,
                                    IRPCServiceTrafficService trafficService,
                                    SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.eventLoopGroup = eventLoopGroup;
            this.sslContext = sslContext.orElse(null);
            this.trafficService = trafficService;
        }

        @Override
        public IInboxClient share() {
            return IInboxClient.newBuilder()
                .trafficService(trafficService)
                .eventLoopGroup(eventLoopGroup)
                .sslContext(sslContext)
                .build();
        }
    }

    private static class InboxStoreClientProvider extends SharedResourceProvider<IBaseKVStoreClient> {
        private final StandaloneConfig config;
        private final EventLoopGroup eventLoopGroup;
        private final IRPCServiceTrafficService trafficService;
        private final IBaseKVMetaService metaService;
        private final SslContext rpcClientSSLContext;

        @Inject
        private InboxStoreClientProvider(StandaloneConfig config,
                                         IRPCServiceTrafficService trafficService,
                                         IBaseKVMetaService metaService,
                                         @Named("rpcClientSSLContext") Optional<SslContext> rpcClientSSLContext,
                                         @Named("rpcClientEventLoop") EventLoopGroup eventLoopGroup,
                                         SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.config = config;
            this.trafficService = trafficService;
            this.metaService = metaService;
            this.rpcClientSSLContext = rpcClientSSLContext.orElse(null);
            this.eventLoopGroup = eventLoopGroup;
        }


        @Override
        public IBaseKVStoreClient share() {
            InboxStoreConfig storeConfig = config.getInboxServiceConfig().getStore();
            return IBaseKVStoreClient.newBuilder()
                .clusterId(IInboxStore.CLUSTER_NAME)
                .trafficService(trafficService)
                .metaService(metaService)
                .workerThreads(storeConfig.getWorkerThreads())
                .eventLoopGroup(eventLoopGroup)
                .sslContext(rpcClientSSLContext)
                .queryPipelinesPerStore(storeConfig.getQueryPipelinePerStore())
                .build();
        }
    }

    private static class RetainClientProvider extends SharedResourceProvider<IRetainClient> {
        private final EventLoopGroup eventLoopGroup;
        private final SslContext sslContext;
        private final IRPCServiceTrafficService trafficService;

        @Inject
        private RetainClientProvider(@Named("rpcClientEventLoop") EventLoopGroup eventLoopGroup,
                                     @Named("rpcClientSSLContext") Optional<SslContext> sslContext,
                                     IRPCServiceTrafficService trafficService,
                                     SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.eventLoopGroup = eventLoopGroup;
            this.sslContext = sslContext.orElse(null);
            this.trafficService = trafficService;
        }

        @Override
        public IRetainClient share() {
            return IRetainClient.newBuilder()
                .trafficService(trafficService)
                .eventLoopGroup(eventLoopGroup)
                .sslContext(sslContext)
                .build();
        }
    }

    private static class RetainStoreClientProvider extends SharedResourceProvider<IBaseKVStoreClient> {
        private final StandaloneConfig config;
        private final EventLoopGroup eventLoopGroup;
        private final IRPCServiceTrafficService trafficService;
        private final IBaseKVMetaService metaService;
        private final SslContext rpcClientSSLContext;

        @Inject
        private RetainStoreClientProvider(StandaloneConfig config,
                                          @Named("rpcClientEventLoop") EventLoopGroup eventLoopGroup,
                                          IRPCServiceTrafficService trafficService,
                                          IBaseKVMetaService metaService,
                                          @Named("rpcClientSSLContext") Optional<SslContext> rpcClientSSLContext,
                                          SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.config = config;
            this.eventLoopGroup = eventLoopGroup;
            this.trafficService = trafficService;
            this.metaService = metaService;
            this.rpcClientSSLContext = rpcClientSSLContext.orElse(null);
        }


        @Override
        public IBaseKVStoreClient share() {
            RetainStoreClientConfig clientConfig = config.getRetainServiceConfig().getStoreClient();
            return IBaseKVStoreClient.newBuilder()
                .clusterId(IRetainStore.CLUSTER_NAME)
                .trafficService(trafficService)
                .metaService(metaService)
                .eventLoopGroup(eventLoopGroup)
                .workerThreads(clientConfig.getWorkerThreads())
                .sslContext(rpcClientSSLContext)
                .queryPipelinesPerStore(clientConfig.getQueryPipelinePerStore())
                .build();
        }
    }

    private static class SessionDictClientProvider extends SharedResourceProvider<ISessionDictClient> {
        private final StandaloneConfig config;
        private final EventLoopGroup eventLoopGroup;
        private final SslContext sslContext;
        private final IRPCServiceTrafficService trafficService;

        @Inject
        private SessionDictClientProvider(StandaloneConfig config,
                                          @Named("rpcClientEventLoop") EventLoopGroup eventLoopGroup,
                                          @Named("rpcClientSSLContext") Optional<SslContext> sslContext,
                                          IRPCServiceTrafficService trafficService,
                                          SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.config = config;
            this.eventLoopGroup = eventLoopGroup;
            this.sslContext = sslContext.orElse(null);
            this.trafficService = trafficService;
        }

        @Override
        public ISessionDictClient share() {
            return ISessionDictClient.newBuilder()
                .workerThreads(config.getSessionDictServiceConfig().getClient().getWorkerThreads())
                .trafficService(trafficService)
                .eventLoopGroup(eventLoopGroup)
                .sslContext(sslContext)
                .build();
        }
    }

    private static class MQTTBrokerClientProvider extends SharedResourceProvider<IMqttBrokerClient> {
        private final StandaloneConfig config;
        private final EventLoopGroup eventLoopGroup;
        private final SslContext sslContext;
        private final IRPCServiceTrafficService trafficService;

        @Inject
        private MQTTBrokerClientProvider(StandaloneConfig config,
                                         @Named("rpcClientEventLoop") EventLoopGroup eventLoopGroup,
                                         @Named("rpcClientSSLContext") Optional<SslContext> sslContext,
                                         IRPCServiceTrafficService trafficService,
                                         SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.config = config;
            this.eventLoopGroup = eventLoopGroup;
            this.sslContext = sslContext.orElse(null);
            this.trafficService = trafficService;
        }

        @Override
        public IMqttBrokerClient share() {
            return IMqttBrokerClient.newBuilder()
                .workerThreads(config.getMqttServiceConfig().getClient().getWorkerThreads())
                .trafficService(trafficService)
                .eventLoopGroup(eventLoopGroup)
                .sslContext(sslContext)
                .build();
        }
    }
}
