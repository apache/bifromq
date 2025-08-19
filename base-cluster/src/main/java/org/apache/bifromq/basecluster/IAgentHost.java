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

import io.reactivex.rxjava3.core.Observable;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.basecluster.memberlist.HostAddressResolver;
import org.apache.bifromq.basecluster.memberlist.IHostAddressResolver;
import org.apache.bifromq.basecluster.memberlist.agent.IAgent;
import org.apache.bifromq.basecluster.membership.proto.HostEndpoint;
import org.apache.bifromq.basecluster.transport.ITransport;
import org.apache.bifromq.basecluster.transport.TCPTransport;
import org.apache.bifromq.basecluster.transport.Transport;

/**
 * Agent host defines the interface for hosting agents and joining the cluster.
 */
public interface IAgentHost extends AutoCloseable {
    /**
     * Create a new instance of agent host.
     *
     * @param options the options for creating the agent host
     * @return a new instance of agent host
     */
    static IAgentHost newInstance(AgentHostOptions options) {
        ITransport transport = Transport.builder()
            .env(options.env())
            .bindAddr(new InetSocketAddress(options.addr(), options.port()))
            .serverSslContext(options.serverSslContext())
            .clientSslContext(options.clientSslContext())
            .options(new Transport.TransportOptions()
                .mtu(options.udpPacketLimit())
                .tcpTransportOptions(new TCPTransport.TCPTransportOptions()
                    .maxChannelsPerHost(options.maxChannelsPerHost())
                    .idleTimeoutInSec(options.idleTimeoutInSec())
                    .connTimeoutInMS(options.connTimeoutInMS())))
            .build();
        IHostAddressResolver resolver =
            new HostAddressResolver(options.addressExpiryInterval(), options.addressRefreshInterval());
        return new AgentHost(transport, resolver, options);
    }

    String env();

    HostEndpoint local();

    /**
     * Join the cluster as a running node by communicating with some existing running node of the cluster as the seeds.
     *
     * @param seeds the seeds of the cluster
     * @return a future that completes when the node has successfully joined the cluster
     */
    CompletableFuture<Void> join(Set<InetSocketAddress> seeds);

    /**
     * Host an agent in local agent host.
     *
     * @param agentId the agent id
     * @return the hosted agent
     */
    IAgent host(String agentId);

    /**
     * Stop hosting the agent from local agent host.
     *
     * @param agentId the agent id
     */
    CompletableFuture<Void> stopHosting(String agentId);

    /**
     * An observable of agent host membership.
     *
     * @return an observable of agent host membership
     */
    Observable<Set<HostEndpoint>> membership();

    /**
     * Return the agent landscape of the cluster.
     *
     * @return the map of host id to the set of agent ids hosted by the host
     */
    Observable<Map<HostEndpoint, Set<String>>> landscape();

    /**
     * Emits a signal whenever the local host actively refutes a suspicion of being dead.
     * Each emission carries the timestamp (in millis) when the refutation occurred.
     *
     * @return an observable stream of refutation timestamps
     */
    Observable<Long> refuteSignal();

    /**
     * Shutdown the agent host.
     */
    void close();
}
