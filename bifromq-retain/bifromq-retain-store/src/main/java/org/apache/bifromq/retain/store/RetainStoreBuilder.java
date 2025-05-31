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

package org.apache.bifromq.retain.store;

import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.basekv.store.option.KVRangeStoreOptions;
import org.apache.bifromq.baserpc.server.RPCServerBuilder;
import com.google.protobuf.Struct;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * The builder for building Retain Store.
 */
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Accessors(fluent = true)
@Setter
public class RetainStoreBuilder {
    String clusterId = IRetainStore.CLUSTER_NAME;
    RPCServerBuilder rpcServerBuilder;
    IAgentHost agentHost;
    IBaseKVMetaService metaService;
    IBaseKVStoreClient retainStoreClient;
    KVRangeStoreOptions storeOptions;
    int workerThreads;
    int tickerThreads;
    ScheduledExecutorService bgTaskExecutor;
    Duration bootstrapDelay = Duration.ofSeconds(15);
    Duration zombieProbeDelay = Duration.ofSeconds(15);
    Duration balancerRetryDelay = Duration.ofSeconds(5);
    Map<String, Struct> balancerFactoryConfig = new HashMap<>();
    Duration loadEstimateWindow = Duration.ofSeconds(5);
    Duration gcInterval = Duration.ofMinutes(60);
    Map<String, String> attributes = new HashMap<>();

    public IRetainStore build() {
        return new RetainStore(this);
    }
}
