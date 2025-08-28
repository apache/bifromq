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

package org.apache.bifromq.basecluster;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberAddr;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberMetadata;
import org.apache.bifromq.basecluster.memberlist.HostAddressResolver;
import org.apache.bifromq.basecluster.memberlist.agent.IAgent;
import org.apache.bifromq.basecluster.membership.proto.HostEndpoint;
import org.apache.bifromq.basecluster.transport.ITransport;

@Slf4j
public class AgentTestCluster {
    private final MockNetwork network = new MockNetwork();
    private final Map<String, AgentHostMeta> hostMetaMap = Maps.newConcurrentMap();
    private final Map<String, HostEndpoint> hostEndpointMap = Maps.newConcurrentMap();
    private final Map<String, ITransport> hostTransportMap = Maps.newConcurrentMap();
    private final Map<HostEndpoint, IAgentHost> hostMap = Maps.newConcurrentMap();
    private final Map<String, List<ByteString>> inflationLogs = Maps.newConcurrentMap();
    private final Map<String, HostEndpoint> crashedHostEndpointMap = Maps.newConcurrentMap();
    private final Map<String, ITransport> crashedHostTransportMap = Maps.newConcurrentMap();
    private final Map<HostEndpoint, IAgentHost> crashedHostMap = Maps.newConcurrentMap();
    private final CompositeDisposable disposables = new CompositeDisposable();

    public AgentTestCluster() {
    }

    public String registerHost(String hostId, AgentHostOptions options) {
        hostMetaMap.computeIfAbsent(hostId, k -> new AgentHostMeta(options));
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
        hostMap.remove(hostEndpointMap.get(hostId)).close();
    }

    public void isolate(String hostId) {
        checkHost(hostId);
        network.isolate(hostTransportMap.get(hostId));
    }

    public void crash(String hostId) {
        checkHost(hostId);
        network.isolate(hostTransportMap.get(hostId));
        inflationLogs.remove(hostId);

        HostEndpoint crashedEndpoint = hostEndpointMap.remove(hostId);
        crashedHostEndpointMap.put(hostId, crashedEndpoint);

        IAgentHost crashedAgentHost = hostMap.remove(crashedEndpoint);
        crashedHostMap.put(crashedEndpoint, crashedAgentHost);
        ITransport transport = hostTransportMap.remove(hostId);
        crashedHostTransportMap.put(hostId, transport);
    }

    public void integrate(String hostId) {
        network.integrate(hostTransportMap.get(hostId));
    }

    public HostEndpoint endpoint(String hostId) {
        checkHost(hostId);
        return getHost(hostId).local();
    }

    public IAgent hostAgent(String hostId, String agentId) {
        checkHost(hostId);
        return getHost(hostId).host(agentId);
    }

    public void stopHostAgent(String hostId, String agentId) {
        checkHost(hostId);
        getHost(hostId).stopHosting(agentId).join();
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
        IAgentHost host =
            new AgentHost(transport, new HostAddressResolver(Duration.ofSeconds(1), Duration.ofSeconds(1)), options);
        hostEndpointMap.put(storeId, host.local());
        hostMap.put(host.local(), host);
        hostTransportMap.put(storeId, transport);
        return host.local();
    }

    public void shutdown() {
        disposables.dispose();
        hostEndpointMap.keySet().forEach(this::stopHost);
        crashedHostTransportMap.keySet().forEach(hostId ->
            crashedHostMap.remove(crashedHostEndpointMap.get(hostId)).close());
    }

    public IAgentHost getHost(String hostId) {
        checkHost(hostId);
        return hostMap.get(hostEndpointMap.get(hostId));
    }

    private void checkHost(String hostId) {
        Preconditions.checkArgument(hostEndpointMap.containsKey(hostId));
    }

    @AllArgsConstructor
    private static class AgentHostMeta {
        final AgentHostOptions options;
    }
}
