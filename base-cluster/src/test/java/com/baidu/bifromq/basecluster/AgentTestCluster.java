/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.basecluster;

import com.baidu.bifromq.basecluster.agent.proto.AgentMemberAddr;
import com.baidu.bifromq.basecluster.agent.proto.AgentMemberMetadata;
import com.baidu.bifromq.basecluster.memberlist.agent.IAgent;
import com.baidu.bifromq.basecluster.membership.proto.HostEndpoint;
import com.baidu.bifromq.basecluster.transport.ITransport;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AgentTestCluster {
    @AllArgsConstructor
    private static class AgentHostMeta {
        final AgentHostOptions options;
    }

    private final MockNetwork network = new MockNetwork();
    private final Map<String, AgentHostMeta> hostMetaMap = Maps.newConcurrentMap();
    private final Map<String, HostEndpoint> hostEndpointMap = Maps.newConcurrentMap();
    private final Map<String, ITransport> hostTransportMap = Maps.newConcurrentMap();
    private final Map<HostEndpoint, IAgentHost> hostMap = Maps.newConcurrentMap();
    private final Map<String, List<ByteString>> inflationLogs = Maps.newConcurrentMap();
    private final CompositeDisposable disposables = new CompositeDisposable();

    public AgentTestCluster() {
    }

    public String newHost(String hostId, AgentHostOptions options) {
        hostMetaMap.computeIfAbsent(hostId, k -> {
            loadStore(hostId, options);
            return new AgentHostMeta(options);
        });
        return hostId;
    }

    public void startHost(String hostId) {
        Preconditions.checkArgument(hostMetaMap.containsKey(hostId), "Unknown store %s", hostId);
        if (hostMetaMap.containsKey(hostId)) {
            AgentHostMeta meta = hostMetaMap.get(hostId);
            loadStore(hostId, meta.options);
        }
    }

    public void join(String joinerId, String joineeId) {
        checkHost(joinerId);
        checkHost(joineeId);
        hostMap.get(hostEndpointMap.get(joinerId))
            .join(Sets.newHashSet(new InetSocketAddress(hostEndpointMap.get(joineeId).getAddress(),
                hostEndpointMap.get(joineeId).getPort())));
    }

    public void stopHost(String hostId) {
        checkHost(hostId);
        inflationLogs.remove(hostId);
        hostMap.remove(hostEndpointMap.get(hostId)).shutdown();
    }

    public void isolate(String hostId) {
        checkHost(hostId);
        network.isolate(hostTransportMap.get(hostId));
    }

    public void integrate(String hostId) {
        network.integrate(hostTransportMap.get(hostId));
    }


    public HostEndpoint endpoint(String hostId) {
        checkHost(hostId);
        return getHost(hostId).local();
    }

    public IAgent host(String hostId, String agentId) {
        checkHost(hostId);
        return getHost(hostId).host(agentId);
    }

    public Observable<Map<AgentMemberAddr, AgentMemberMetadata>> agent(String hostId, String agentId) {
        checkHost(hostId);
        return getHost(hostId).host(agentId).membership();
    }

    public Set<HostEndpoint> membership(String hostId) {
        checkHost(hostId);
        return getHost(hostId).membership().blockingFirst();
    }

    public List<ByteString> inflationLog(String storeId) {
        checkHost(storeId);
        return Collections.unmodifiableList(inflationLogs.get(storeId));
    }

    private HostEndpoint loadStore(String storeId, AgentHostOptions options) {
        inflationLogs.putIfAbsent(storeId, new LinkedList<>());
        ITransport transport = network.create();
        options.addr("127.0.0.1");
        options.port(transport.bindAddress().getPort());
        IAgentHost host = new AgentHost(transport, options);
        host.start();
        hostEndpointMap.put(storeId, host.local());
        hostMap.put(host.local(), host);
        hostTransportMap.put(storeId, transport);
        return host.local();
    }

    public void shutdown() {
        disposables.dispose();
        hostEndpointMap.keySet().forEach(this::stopHost);
    }

    public IAgentHost getHost(String hostId) {
        checkHost(hostId);
        return hostMap.get(hostEndpointMap.get(hostId));
    }

    private void checkHost(String hostId) {
        Preconditions.checkArgument(hostEndpointMap.containsKey(hostId));
    }
}
