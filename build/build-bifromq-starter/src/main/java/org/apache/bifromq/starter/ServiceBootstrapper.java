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

package org.apache.bifromq.starter;

import com.google.inject.Inject;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.apiserver.IAPIServer;
import org.apache.bifromq.baserpc.server.IRPCServer;
import org.apache.bifromq.baserpc.server.RPCServerBuilder;
import org.apache.bifromq.dist.server.IDistServer;
import org.apache.bifromq.dist.worker.IDistWorker;
import org.apache.bifromq.inbox.server.IInboxServer;
import org.apache.bifromq.inbox.store.IInboxStore;
import org.apache.bifromq.mqtt.IMQTTBroker;
import org.apache.bifromq.retain.server.IRetainServer;
import org.apache.bifromq.retain.store.IRetainStore;
import org.apache.bifromq.sessiondict.server.ISessionDictServer;
import org.apache.bifromq.starter.module.ServiceInjector;

@Slf4j
class ServiceBootstrapper {
    private final Optional<IAPIServer> apiServerOpt;
    private final Optional<IMQTTBroker> mqttBrokerOpt;

    private final Optional<IDistServer> distServerOpt;
    private final Optional<IDistWorker> distWorkerOpt;
    private final Optional<IInboxServer> inboxServerOpt;
    private final Optional<IInboxStore> inboxStoreOpt;
    private final Optional<IRetainServer> retainServerOpt;
    private final Optional<IRetainStore> retainStoreOpt;
    private final Optional<ISessionDictServer> sessionDictServerOpt;
    private final ServiceInjector serviceInjector;


    @Inject
    ServiceBootstrapper(Optional<IAPIServer> apiServerOpt,
                        Optional<IMQTTBroker> mqttBrokerOpt,
                        Optional<IDistServer> distServerOpt,
                        Optional<IDistWorker> distWorkerOpt,
                        Optional<IInboxServer> inboxServerOpt,
                        Optional<IInboxStore> inboxStoreOpt,
                        Optional<IRetainServer> retainServerOpt,
                        Optional<IRetainStore> retainStoreOpt,
                        Optional<ISessionDictServer> sessionDictServerOpt,
                        ServiceInjector serviceInjector) {
        this.apiServerOpt = apiServerOpt;
        this.mqttBrokerOpt = mqttBrokerOpt;
        this.distServerOpt = distServerOpt;
        this.distWorkerOpt = distWorkerOpt;
        this.inboxServerOpt = inboxServerOpt;
        this.inboxStoreOpt = inboxStoreOpt;
        this.retainServerOpt = retainServerOpt;
        this.retainStoreOpt = retainStoreOpt;
        this.sessionDictServerOpt = sessionDictServerOpt;
        this.serviceInjector = serviceInjector;
    }

    public BootstrappedServices bootstrap() {
        // If any of the services is present, we need to start the RPC server
        if (distServerOpt.isPresent()
            || distWorkerOpt.isPresent()
            || inboxServerOpt.isPresent()
            || inboxStoreOpt.isPresent()
            || retainServerOpt.isPresent()
            || retainStoreOpt.isPresent()
            || sessionDictServerOpt.isPresent()) {
            return new BootstrappedServices(
                serviceInjector.getInstance(RPCServerBuilder.class).build(),
                apiServerOpt,
                mqttBrokerOpt,
                distServerOpt,
                distWorkerOpt,
                inboxServerOpt,
                inboxStoreOpt,
                retainServerOpt,
                retainStoreOpt,
                sessionDictServerOpt);
        } else {
            return new BootstrappedServices(
                null,
                apiServerOpt,
                mqttBrokerOpt,
                distServerOpt,
                distWorkerOpt,
                inboxServerOpt,
                inboxStoreOpt,
                retainServerOpt,
                retainStoreOpt,
                sessionDictServerOpt);
        }
    }


    static class BootstrappedServices {
        private final IRPCServer rpcServer;
        private final Optional<IAPIServer> apiServerOpt;
        private final Optional<IMQTTBroker> mqttBrokerOpt;

        private final Optional<IDistServer> distServerOpt;
        private final Optional<IDistWorker> distWorkerOpt;
        private final Optional<IInboxServer> inboxServerOpt;
        private final Optional<IInboxStore> inboxStoreOpt;
        private final Optional<IRetainServer> retainServerOpt;
        private final Optional<IRetainStore> retainStoreOpt;
        private final Optional<ISessionDictServer> sessionDictServerOpt;

        private BootstrappedServices(IRPCServer rpcServer,
                                     Optional<IAPIServer> apiServerOpt,
                                     Optional<IMQTTBroker> mqttBrokerOpt,
                                     Optional<IDistServer> distServerOpt,
                                     Optional<IDistWorker> distWorkerOpt,
                                     Optional<IInboxServer> inboxServerOpt,
                                     Optional<IInboxStore> inboxStoreOpt,
                                     Optional<IRetainServer> retainServerOpt,
                                     Optional<IRetainStore> retainStoreOpt,
                                     Optional<ISessionDictServer> sessionDictServerOpt) {
            this.rpcServer = rpcServer;
            this.apiServerOpt = apiServerOpt;
            this.mqttBrokerOpt = mqttBrokerOpt;
            this.distServerOpt = distServerOpt;
            this.distWorkerOpt = distWorkerOpt;
            this.inboxServerOpt = inboxServerOpt;
            this.inboxStoreOpt = inboxStoreOpt;
            this.retainServerOpt = retainServerOpt;
            this.retainStoreOpt = retainStoreOpt;
            this.sessionDictServerOpt = sessionDictServerOpt;
        }

        void start() {
            if (rpcServer != null) {
                log.info("Start RPC server");
                rpcServer.start();
            }
            apiServerOpt.ifPresent(IAPIServer::start);
            mqttBrokerOpt.ifPresent(IMQTTBroker::start);
        }

        void stop() {
            mqttBrokerOpt.ifPresent(IMQTTBroker::close);
            apiServerOpt.ifPresent(IAPIServer::close);
            if (rpcServer != null) {
                log.info("Stop RPC server");
                rpcServer.shutdown();
            }
            distServerOpt.ifPresent(IDistServer::close);
            distWorkerOpt.ifPresent(IDistWorker::close);
            inboxServerOpt.ifPresent(IInboxServer::close);
            inboxStoreOpt.ifPresent(IInboxStore::close);
            retainServerOpt.ifPresent(IRetainServer::close);
            retainStoreOpt.ifPresent(IRetainStore::close);
            sessionDictServerOpt.ifPresent(ISessionDictServer::close);
        }
    }
}
