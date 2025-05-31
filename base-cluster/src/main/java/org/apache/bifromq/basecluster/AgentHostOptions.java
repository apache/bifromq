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

package org.apache.bifromq.basecluster;

import org.apache.bifromq.basecrdt.store.CRDTStoreOptions;
import io.netty.handler.ssl.SslContext;
import java.time.Duration;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

@Builder(toBuilder = true)
@Accessors(chain = true, fluent = true)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class AgentHostOptions {

    /**
     * Agent host under same env can communicate with each other.
     */
    @Builder.Default
    private String env = "";

    /**
     * The advertised address of the store, must be IP or hostname.
     */
    private String addr;

    /**
     * The advertised port.
     */
    @Builder.Default
    private int port = 0;

    /**
     * The sslContext to build TLS transport.
     */
    private SslContext serverSslContext;

    /**
     * The sslContext to build TLS client.
     */
    private SslContext clientSslContext;

    /**
     * The UDP DatagramPacket size limit in bytes.
     */
    @Builder.Default
    private int udpPacketLimit = 1400;

    /**
     * maxChannelsPerHost is the max TCP channels for one remote host.
     */
    @Builder.Default
    private int maxChannelsPerHost = 5;

    /**
     * TCP channel's idle time before close.
     */
    @Builder.Default
    private int idleTimeoutInSec = 5;

    /**
     * TCP channel's connection timeout.
     */
    @Builder.Default
    private int connTimeoutInMS = 5000;

    /**
     * the number of peers that will be asked to perform indirect probes in the case that a direct probe failed.
     */
    @Builder.Default
    private Integer indirectProbes = 3;

    /**
     * The multiplier for the number of retransmissions that are attempted for messages broadcast over gossip. The
     * actual count of retransmissions is calculated using the formula:
     * <p>
     * retransmits = retransmitMultiplier * log(N+1)
     * <p>
     * This allows the retransmits to scale properly with cluster size. The higher the multiplier, the more likely a
     * failed broadcast is to converge at the expense of increased bandwidth.
     */
    @Builder.Default
    private Integer retransmitMultiplier = 4;

    /**
     * The multiplier for determining the time an inaccessible node is considered suspect before declaring it dead. The
     * actual timeout is calculated using the formula:
     * <p>
     * suspicionTimeout = suspicionMultiplier * log(N+1) * probeInterval
     * <p>
     * This allows the timeout to scale properly with expected propagation delay with a larger cluster size. The higher
     * the multiplier, the longer an inaccessible node is considered part of the cluster before declaring it dead,
     * giving that suspect node more time to refute if it is indeed still alive.
     */
    @Builder.Default
    private Integer suspicionMultiplier = 4;

    /**
     * The multiplier applied to the suspicionTimeout used as an upper bound on detection time. This max timeout is
     * calculated using the formula:
     * <p>
     * suspicionMaxTimeout = suspicionMaxTimeoutMultiplier * suspicionTimeout
     * <p>
     * If everything is working properly, confirmations from other nodes will accelerate suspicion timers in a manner
     * which will cause the timeout to reach the base SuspicionTimeout before that elapses, so this value will typically
     * only come into play if a node is experiencing issues communicating with other nodes. It should be set to a
     * something fairly large so that a node having problems will have a lot of chances to recover before falsely
     * declaring other nodes as failed, but short enough for a legitimately isolated node to still make progress marking
     * nodes failed in a reasonable amount of time.
     */
    @Builder.Default
    private Integer suspicionMaxTimeoutMultiplier = 6;

    /**
     * baseProbeInterval is the interval between random node probes. Setting this lower (more frequent) will cause the
     * cluster to detect failed nodes more quickly at the expense of increased bandwidth usage. The actual interval is
     * calculated using the formula:
     * <p>
     * probeInterval = baseProbeInterval * localHealthMultiplier
     * <p>
     * where localHealthMultiplier is maintained dynamically by LHA module
     */
    @Builder.Default
    private Duration baseProbeInterval = Duration.ofSeconds(2);

    /**
     * awarenessMaxMultiplier will increase the probe interval if the node becomes aware that it might be degraded and
     * not meeting the soft real time requirements to reliably probe other nodes.
     */
    @Builder.Default
    private Integer awarenessMaxMultiplier = 4;


    /**
     * baseProbeTimeout is the timeout to wait for an ack from a probed node before assuming it is unhealthy. This
     * should be set to 99-percentile of RTT (round-trip time) on your network. The actual timeout is calculated using
     * the formula:
     * <p>
     * probeTimeout = baseProbeTimeout * localHealthMultiplier
     * <p>
     * where localHealthMultiplier is maintained dynamically by LHA module
     */
    @Builder.Default
    private Duration baseProbeTimeout = Duration.ofMillis(500);

    /**
     * gossipPeriod is the interval between sending messages that need to be gossiped that haven't been able to
     * piggyback on probing messages. If this is set to zero, non-piggyback gossip is disabled. By lowering this value
     * (more frequent) gossip messages are propagated across the cluster more quickly at the expense of increased
     * bandwidth.
     */
    @Builder.Default
    private Duration gossipPeriod = Duration.ofMillis(200);

    /**
     * gossipFanout is the number of random recipients to send messages to per GossipInterval. Increasing this number
     * causes the gossip messages to propagate across the cluster more quickly at the expense of increased bandwidth.
     */
    @Builder.Default
    private Integer gossipFanout = 4;

    /**
     * gossipFanoutPerPeriod is the number of gossip messages sent in one gossip period. This number should be
     * appropriate or it may cause gossip storm when some slow member keeps many unconfirmed gossips.
     */
    @Builder.Default
    private Integer gossipFanoutPerPeriod = 60;

    /**
     * Retry delay in seconds after join failure
     */
    @Builder.Default
    private int joinRetryInSec = 10;

    @Builder.Default
    private Duration joinTimeout = Duration.ofSeconds(1800);

    @Builder.Default
    private Duration autoHealingTimeout = Duration.ofHours(24);

    @Builder.Default
    private Duration autoHealingInterval = Duration.ofSeconds(10);

    @Builder.Default
    private Duration addressExpiryInterval = Duration.ofMinutes(5);

    @Builder.Default
    private Duration addressRefreshInterval = Duration.ofMinutes(1);

    /**
     * The options for internal used CRDT store.
     */
    @Builder.Default
    private CRDTStoreOptions crdtStoreOptions = new CRDTStoreOptions();
}
