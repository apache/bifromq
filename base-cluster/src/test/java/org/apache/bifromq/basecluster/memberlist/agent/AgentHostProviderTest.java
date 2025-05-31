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

package org.apache.bifromq.basecluster.memberlist.agent;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basecluster.agent.proto.AgentEndpoint;
import org.apache.bifromq.basecluster.membership.proto.HostEndpoint;
import org.apache.bifromq.basecluster.membership.proto.HostMember;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class AgentHostProviderTest {
    private String agentId1 = "agentA";
    private String agentId2 = "agentB";

    private InetSocketAddress hostAddr1 = new InetSocketAddress("localhost", 1111);
    private InetSocketAddress hostAddr2 = new InetSocketAddress("localhost", 2222);
    private AgentEndpoint endpoint1 = AgentEndpoint.newBuilder()
        .setEndpoint(HostEndpoint.newBuilder()
            .setId(ByteString.copyFromUtf8("ep1"))
            .setAddress(hostAddr1.getHostName())
            .setPort(hostAddr1.getPort())
            .build())
        .build();
    private AgentEndpoint endpoint2 = AgentEndpoint.newBuilder()
        .setEndpoint(HostEndpoint.newBuilder()
            .setId(ByteString.copyFromUtf8("ep2"))
            .setAddress(hostAddr2.getHostName())
            .setPort(hostAddr2.getPort())
            .build())
        .build();

    private HostMember hostMember1 = HostMember.newBuilder()
        .setEndpoint(endpoint1.getEndpoint())
        .putAgent(agentId1, 0)
        .putAgent(agentId2, 0)
        .build();

    private HostMember hostMember2 = HostMember.newBuilder()
        .setEndpoint(endpoint2.getEndpoint())
        .putAgent(agentId2, 0)
        .build();

    @Test
    public void agentAddress() {
        BehaviorSubject<Map<HostEndpoint, HostMember>> aliveHostsSubject =
            BehaviorSubject.createDefault(Collections.emptyMap());
        AgentAddressProvider hostProvider1 = new AgentAddressProvider(agentId1, aliveHostsSubject);
        AgentAddressProvider hostProvider2 = new AgentAddressProvider(agentId2, aliveHostsSubject);

        assertTrue(hostProvider1.agentAddress().blockingFirst().isEmpty());
        assertTrue(hostProvider2.agentAddress().blockingFirst().isEmpty());
        aliveHostsSubject.onNext(new HashMap<>() {{
            put(endpoint1.getEndpoint(), hostMember1);
            put(endpoint2.getEndpoint(), hostMember2);
        }});

        assertEquals(hostProvider1.agentAddress().blockingFirst().size(), 1);
        assertTrue(hostProvider1.agentAddress().blockingFirst().contains(endpoint1));

        assertEquals(hostProvider2.agentAddress().blockingFirst().size(), 2);
        assertTrue(hostProvider2.agentAddress().blockingFirst().containsAll(Sets.newHashSet(endpoint1, endpoint2)));
    }
}
