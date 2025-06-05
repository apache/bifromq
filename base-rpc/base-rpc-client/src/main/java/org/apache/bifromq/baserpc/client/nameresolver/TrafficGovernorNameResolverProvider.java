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

package org.apache.bifromq.baserpc.client.nameresolver;

import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceLandscape;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.NameResolverRegistry;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TrafficGovernorNameResolverProvider extends NameResolverProvider {
    public static final String SCHEME = "tgov";
    public static final TrafficGovernorNameResolverProvider INSTANCE = new TrafficGovernorNameResolverProvider();
    private static final Map<String, TrafficGovernorNameResolver> RESOLVERS = new ConcurrentHashMap<>();

    static {
        NameResolverRegistry.getDefaultRegistry().register(INSTANCE);
    }

    public static void register(String serviceUniqueName, IRPCServiceLandscape trafficDirector) {
        RESOLVERS.put(serviceUniqueName, new TrafficGovernorNameResolver(serviceUniqueName, trafficDirector));
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
        if (SCHEME.equals(targetUri.getScheme())) {
            return RESOLVERS.get(targetUri.getAuthority());
        }
        return null;
    }

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return Integer.MAX_VALUE;
    }

    @Override
    public String getDefaultScheme() {
        return SCHEME;
    }

    @Override
    public Collection<Class<? extends SocketAddress>> getProducedSocketAddressTypes() {
        return Collections.singleton(SocketAddress.class);
    }
}
