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

package org.apache.bifromq.dist.worker;

import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.basekv.store.option.KVRangeStoreOptions;
import org.apache.bifromq.baserpc.server.RPCServerBuilder;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.subbroker.ISubBrokerManager;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
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
 * The builder for building Dist Worker.
 */
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Accessors(fluent = true)
@Setter
public class DistWorkerBuilder {
    String clusterId = IDistWorker.CLUSTER_NAME;
    RPCServerBuilder rpcServerBuilder;
    IAgentHost agentHost;
    IBaseKVMetaService metaService;
    int workerThreads;
    int tickerThreads;
    ScheduledExecutorService bgTaskExecutor;
    IEventCollector eventCollector;
    IResourceThrottler resourceThrottler;
    IDistClient distClient;
    IBaseKVStoreClient distWorkerClient;
    ISubBrokerManager subBrokerManager;
    KVRangeStoreOptions storeOptions;
    Duration gcInterval = Duration.ofMinutes(5);
    Duration bootstrapDelay = Duration.ofSeconds(15);
    Duration zombieProbeDelay = Duration.ofSeconds(15);
    Duration balancerRetryDelay = Duration.ofSeconds(5);
    Map<String, Struct> balancerFactoryConfig = new HashMap<>();
    Duration loadEstimateWindow = Duration.ofSeconds(5);
    Map<String, String> attributes = new HashMap<>();

    public IDistWorker build() {
        return new DistWorker(this);
    }
}
