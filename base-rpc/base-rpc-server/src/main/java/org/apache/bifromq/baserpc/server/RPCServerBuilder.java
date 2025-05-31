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

package org.apache.bifromq.baserpc.server;


import org.apache.bifromq.baserpc.BluePrint;
import org.apache.bifromq.baserpc.trafficgovernor.GlobalProcessId;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.google.common.base.Preconditions;
import io.grpc.ServerServiceDefinition;
import io.netty.handler.ssl.SslContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Accessors(fluent = true)
@Setter
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public final class RPCServerBuilder {
    record ServiceDefinition(ServerServiceDefinition definition,
                             BluePrint bluePrint,
                             Map<String, String> attributes,
                             Set<String> defaultGroupTags,
                             Executor executor) {
    }

    @Getter
    String id = GlobalProcessId.ID + "/" + hashCode();
    String host;
    int port = 0;
    int workerThreads = 0;
    IRPCServiceTrafficService trafficService;
    SslContext sslContext;
    final Map<String, ServiceDefinition> serviceDefinitions = new HashMap<>();

    public RPCServerBuilder sslContext(SslContext sslContext) {
        if (sslContext != null) {
            Preconditions.checkArgument(sslContext.isServer(), "Server auth must be enabled");
        }
        this.sslContext = sslContext;
        return this;
    }

    public RPCServerBuilder bindService(@NonNull ServerServiceDefinition serviceDefinition,
                                        @NonNull BluePrint bluePrint,
                                        @NonNull Map<String, String> attributes,
                                        @NonNull Set<String> defaultGroupTags,
                                        @NonNull Executor executor) {
        this.serviceDefinitions.put(serviceDefinition.getServiceDescriptor().getName(),
            new ServiceDefinition(serviceDefinition, bluePrint, attributes, defaultGroupTags, executor));
        return this;
    }

    public IRPCServer build() {
        Preconditions.checkArgument(!serviceDefinitions.isEmpty());
        return new RPCServer(this);
    }
}