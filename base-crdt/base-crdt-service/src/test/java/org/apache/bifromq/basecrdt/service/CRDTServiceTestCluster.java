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

package org.apache.bifromq.basecrdt.service;

import org.apache.bifromq.basecluster.AgentHostOptions;
import org.apache.bifromq.basecluster.IAgentHost;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.net.InetSocketAddress;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CRDTServiceTestCluster {
    @AllArgsConstructor
    private static class CRDTServiceMeta {
        final AgentHostOptions hostOptions;
        final CRDTServiceOptions serviceOption;
    }

    private final Map<String, CRDTServiceMeta> serviceMetaMap = Maps.newConcurrentMap();
    private final Map<String, IAgentHost> serviceHostMap = Maps.newConcurrentMap();
    private final Map<String, String> serviceIdMap = Maps.newConcurrentMap();
    private final Map<String, ICRDTService> serviceMap = Maps.newConcurrentMap();

    public void newService(String serviceId, AgentHostOptions hostOptions, CRDTServiceOptions serviceOptions) {
        serviceMetaMap.computeIfAbsent(serviceId, k -> {
            loadService(serviceId, hostOptions, serviceOptions);
            return new CRDTServiceMeta(hostOptions, serviceOptions);
        });
    }

    public void stopService(String serviceId) {
        checkService(serviceId);
        serviceMap.remove(serviceIdMap.remove(serviceId)).close();
        serviceHostMap.remove(serviceId).close();
    }

    public ICRDTService getService(String serviceId) {
        checkService(serviceId);
        return serviceMap.get(serviceIdMap.get(serviceId));
    }

    public void join(String joinerId, String joineeId) {
        checkService(joinerId);
        checkService(joineeId);
        serviceHostMap.get(joinerId)
            .join(Sets.newHashSet(
                new InetSocketAddress(
                    serviceHostMap.get(joineeId).local().getAddress(),
                    serviceHostMap.get(joineeId).local().getPort()))
            )
            .join();
    }

    private void checkService(String serviceId) {
        Preconditions.checkArgument(serviceIdMap.containsKey(serviceId));
    }

    private String loadService(String serviceId, AgentHostOptions hostOptions, CRDTServiceOptions serviceOptions) {
        log.info("Load service {}", serviceId);
        IAgentHost host = serviceHostMap.computeIfAbsent(serviceId, id -> IAgentHost.newInstance(hostOptions));
        ICRDTService service = ICRDTService.newInstance(host, serviceOptions);
        serviceIdMap.put(serviceId, service.id());
        serviceMap.put(service.id(), service);
        return service.id();
    }

    public void shutdown() {
        serviceIdMap.keySet().forEach(this::stopService);
    }
}
