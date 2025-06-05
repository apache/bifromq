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

package org.apache.bifromq.basekv.server;

import static org.apache.bifromq.basekv.RPCServerMetadataUtil.RPC_METADATA_STORE_ID;
import static java.util.Collections.singletonMap;

import org.apache.bifromq.basekv.RPCBluePrint;
import org.apache.bifromq.baserpc.BluePrint;
import org.apache.bifromq.baserpc.server.RPCServerBuilder;
import com.google.common.base.Preconditions;
import io.grpc.ServerServiceDefinition;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BaseKVStoreServer implements IBaseKVStoreServer {
    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private final Map<String, BaseKVStoreService> storeServiceMap = new HashMap<>();
    protected final Set<BindableStoreService> bindableStoreServices = new HashSet<>();

    BaseKVStoreServer(BaseKVStoreServerBuilder builder) {
        for (BaseKVStoreServiceBuilder serviceBuilder : builder.serviceBuilders.values()) {
            BaseKVStoreService storeService = new BaseKVStoreService(serviceBuilder);
            bindableStoreServices.add(new BindableStoreService(storeService, serviceBuilder.rpcExecutor));
            storeServiceMap.put(storeService.clusterId(), storeService);
        }

        RPCServerBuilder rpcServerBuilder = builder.rpcServerBuilder;
        for (BindableStoreService bindable : bindableStoreServices) {
            rpcServerBuilder.bindService(bindable.serviceDefinition,
                bindable.bluePrint,
                bindable.metadata,
                Collections.emptySet(),
                bindable.executor);
        }

    }

    public final String storeId(String clusterId) {
        Preconditions.checkState(state.get() == State.STARTED);
        return storeServiceMap.get(clusterId).storeId();
    }


    @Override
    public final void start() {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            try {
                log.debug("Starting BaseKVStore server for '{}'", storeServiceMap.keySet());
                storeServiceMap.values().forEach(BaseKVStoreService::start);
                log.debug("BaseKVStore server for '{}' started", storeServiceMap.keySet());
                state.set(State.STARTED);
            } catch (Throwable e) {
                state.set(State.FATAL_FAILURE);
                throw e;
            }
        }
    }

    @Override
    public void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            try {
                log.debug("Stopping BaseKVStore server[{}]", storeServiceMap.keySet());
                storeServiceMap.values().forEach(BaseKVStoreService::stop);
            } catch (Throwable e) {
                log.error("Failed to stop BaseKVStore server[{}]", storeServiceMap.keySet(), e);
            } finally {
                log.debug("BaseKVStore server[{}] stopped", storeServiceMap.keySet());
                state.set(State.STOPPED);
            }
        }
    }

    protected static class BindableStoreService {
        final ServerServiceDefinition serviceDefinition;
        final BluePrint bluePrint;
        final Map<String, String> metadata;
        final Executor executor;

        BindableStoreService(BaseKVStoreService storeService, Executor executor) {
            serviceDefinition = RPCBluePrint.scope(storeService.bindService(), storeService.clusterId());
            bluePrint = RPCBluePrint.build(storeService.clusterId());
            metadata = singletonMap(RPC_METADATA_STORE_ID, storeService.storeId());
            this.executor = executor;
        }
    }

    private enum State {
        INIT, STARTING, STARTED, FATAL_FAILURE, STOPPING, STOPPED
    }
}
