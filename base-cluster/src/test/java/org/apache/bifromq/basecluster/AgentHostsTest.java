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

import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.awaitility.Awaitility.await;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.observers.TestObserver;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberAddr;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberMetadata;
import org.apache.bifromq.basecluster.agent.proto.AgentMessage;
import org.apache.bifromq.basecluster.annotation.StoreCfg;
import org.apache.bifromq.basecluster.annotation.StoreCfgs;
import org.apache.bifromq.basecluster.memberlist.agent.IAgent;
import org.apache.bifromq.basecluster.memberlist.agent.IAgentMember;
import org.apache.bifromq.basecluster.membership.proto.HostEndpoint;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class AgentHostsTest extends AgentTestTemplate {
    @StoreCfgs(stores = {@StoreCfg(id = "s1")})
    @Test
    public void testRegister() {
        IAgent agent = storeMgr.hostAgent("s1", "agent1");
        IAgentMember agentMember1 = agent.register("agentNode1");
        agentMember1.metadata(copyFromUtf8("123"));
        IAgentMember agentMember2 = agent.register("agentNode2");
        agentMember2.metadata(copyFromUtf8("456"));
        Map<AgentMemberAddr, AgentMemberMetadata> expected = new HashMap<>() {{
            put(agentMember1.address(), agentMember1.metadata());
            put(agentMember2.address(), agentMember2.metadata());
        }};
        await().until(() -> {
            Map<AgentMemberAddr, AgentMemberMetadata> agentMembers = agent.membership().blockingFirst();
            return expected.equals(agentMembers);
        });
    }

    @StoreCfgs(stores = {@StoreCfg(id = "s1")})
    @Test
    public void testUnregister() {
        IAgent agent = storeMgr.hostAgent("s1", "agent1");
        IAgentMember agentMember1 = agent.register("agentNode1");
        IAgentMember agentMember2 = agent.register("agentNode2");
        agentMember1.metadata(copyFromUtf8("123"));
        agentMember2.metadata(copyFromUtf8("123"));
        await().until(() -> agent.membership().blockingFirst().size() == 2);

        agent.deregister(agentMember1);
        await().until(() -> {
            Map<AgentMemberAddr, AgentMemberMetadata> agentMembers = agent.membership().blockingFirst();
            if (agentMembers.size() != 1) {
                return false;
            }
            return agentMembers.keySet().stream().findFirst().get().getName().equals("agentNode2");
        });
        agent.deregister(agentMember2);
        await().until(() -> agent.membership().blockingFirst().isEmpty());
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "s1", isSeed = true),
        @StoreCfg(id = "s2"),
        @StoreCfg(id = "s3"),
    })
    @Test
    public void testMultipleAgentHosts() {
        await().forever().until(() -> storeMgr.membership("s1").size() == 3);
        await().forever().until(() -> storeMgr.membership("s2").size() == 3);
        await().forever().until(() -> storeMgr.membership("s3").size() == 3);
    }

    @Test
    @StoreCfgs(stores = {
        @StoreCfg(id = "s1", isSeed = true),
        @StoreCfg(id = "s2"),
    })
    public void testAgentLandscape() {
        await().until(() -> storeMgr.membership("s1").size() == 2);
        await().until(() -> storeMgr.membership("s2").size() == 2);

        storeMgr.hostAgent("s1", "agent1");
        storeMgr.hostAgent("s2", "agent2");

        await().until(() -> {
            Map<HostEndpoint, Set<String>> landscapeOnS1 = storeMgr.getHost("s1").landscape().blockingFirst();
            Map<HostEndpoint, Set<String>> landscapeOnS2 = storeMgr.getHost("s2").landscape().blockingFirst();
            return landscapeOnS1.equals(landscapeOnS2)
                && landscapeOnS1.size() == 2
                && landscapeOnS1.get(storeMgr.endpoint("s1")).equals(Set.of("agent1"))
                && landscapeOnS1.get(storeMgr.endpoint("s2")).equals(Set.of("agent2"));
        });

        storeMgr.stopHostAgent("s1", "agent1");

        await().until(() -> {
            Map<HostEndpoint, Set<String>> landscapeOnS1 = storeMgr.getHost("s1").landscape().blockingFirst();
            Map<HostEndpoint, Set<String>> landscapeOnS2 = storeMgr.getHost("s2").landscape().blockingFirst();
            return landscapeOnS1.equals(landscapeOnS2)
                && landscapeOnS1.size() == 2
                && landscapeOnS1.get(storeMgr.endpoint("s1")).isEmpty()
                && landscapeOnS1.get(storeMgr.endpoint("s2")).equals(Set.of("agent2"));
        });

        storeMgr.stopHostAgent("s2", "agent2");

        await().until(() -> {
            Map<HostEndpoint, Set<String>> landscapeOnS1 = storeMgr.getHost("s1").landscape().blockingFirst();
            Map<HostEndpoint, Set<String>> landscapeOnS2 = storeMgr.getHost("s2").landscape().blockingFirst();
            return landscapeOnS1.equals(landscapeOnS2)
                && landscapeOnS1.size() == 2
                && landscapeOnS1.get(storeMgr.endpoint("s1")).isEmpty()
                && landscapeOnS1.get(storeMgr.endpoint("s2")).isEmpty();
        });

        storeMgr.hostAgent("s1", "agent1");
        await().until(() -> {
            Map<HostEndpoint, Set<String>> landscapeOnS1 = storeMgr.getHost("s1").landscape().blockingFirst();
            Map<HostEndpoint, Set<String>> landscapeOnS2 = storeMgr.getHost("s2").landscape().blockingFirst();
            return landscapeOnS1.equals(landscapeOnS2)
                && landscapeOnS1.size() == 2
                && landscapeOnS1.get(storeMgr.endpoint("s1")).equals(Set.of("agent1"))
                && landscapeOnS1.get(storeMgr.endpoint("s2")).isEmpty();
        });
        storeMgr.hostAgent("s2", "agent2");
        await().until(() -> {
            Map<HostEndpoint, Set<String>> landscapeOnS1 = storeMgr.getHost("s1").landscape().blockingFirst();
            Map<HostEndpoint, Set<String>> landscapeOnS2 = storeMgr.getHost("s2").landscape().blockingFirst();
            return landscapeOnS1.equals(landscapeOnS2)
                && landscapeOnS1.size() == 2
                && landscapeOnS1.get(storeMgr.endpoint("s1")).equals(Set.of("agent1"))
                && landscapeOnS1.get(storeMgr.endpoint("s2")).equals(Set.of("agent2"));
        });
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "s1", isSeed = true),
        @StoreCfg(id = "s2"),
    })
    @Test
    public void testAgentNodesInTwoStores() {
        await().until(() -> storeMgr.membership("s1").size() == 2);
        await().until(() -> storeMgr.membership("s2").size() == 2);

        IAgent agentOnS1 = storeMgr.hostAgent("s1", "agent1");
        IAgent agentOnS2 = storeMgr.hostAgent("s2", "agent1");

        IAgentMember agentMember1 = agentOnS1.register("agentNode1");
        agentMember1.metadata(copyFromUtf8("1"));

        await().until(() -> {
            Map<AgentMemberAddr, AgentMemberMetadata> agentMembers = agentOnS1.membership().blockingFirst();
            return agentMembers.size() == 1;
        });
        await().until(() -> {
            Map<AgentMemberAddr, AgentMemberMetadata> agentMembers = agentOnS2.membership().blockingFirst();
            return agentMembers.size() == 1;
        });

        IAgentMember agentMember2 = agentOnS2.register("agentNode2");
        agentMember2.metadata(copyFromUtf8("2"));

        await().until(() -> agentOnS1.membership().blockingFirst().size() == 2);
        await().until(() -> agentOnS2.membership().blockingFirst().size() == 2);
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "s1", isSeed = true),
        @StoreCfg(id = "s2"),
        @StoreCfg(id = "s3"),
    })
    @Test
    public void testAgentNodesInThreeStores() {
        await().until(() -> {
            Set<HostEndpoint> hosts_s1 = Sets.newHashSet(storeMgr.getHost("s1").membership().blockingFirst());
            return 3 == hosts_s1.size();
        });
        await().until(() -> {
            Set<HostEndpoint> hosts_s2 = Sets.newHashSet(storeMgr.getHost("s2").membership().blockingFirst());
            return 3 == hosts_s2.size();
        });
        await().until(() -> {
            Set<HostEndpoint> hosts_s3 = Sets.newHashSet(storeMgr.getHost("s3").membership().blockingFirst());
            return 3 == hosts_s3.size();
        });
        IAgent agentOnS1 = storeMgr.hostAgent("s1", "agent1");
        IAgent agentOnS2 = storeMgr.hostAgent("s2", "agent1");
        IAgent agentOnS3 = storeMgr.hostAgent("s3", "agent1");
        IAgentMember agentMember1OnS1 = agentOnS1.register("agentNode1");
        agentMember1OnS1.metadata(copyFromUtf8("1"));

        IAgentMember agentMember11OnS1 = agentOnS1.register("agentNode11");
        agentMember11OnS1.metadata(copyFromUtf8("11"));

        IAgentMember agentMember2OnS2 = agentOnS2.register("agentNode2");
        agentMember2OnS2.metadata(copyFromUtf8("2"));

        IAgentMember agentMember3OnS3 = agentOnS3.register("agentNode3");
        agentMember3OnS3.metadata(copyFromUtf8("3"));

        await().until(() -> agentOnS1.membership().blockingFirst().size() == 4);
        await().until(() -> agentOnS2.membership().blockingFirst().size() == 4);
        await().until(() -> agentOnS3.membership().blockingFirst().size() == 4);

        // unhost agentNode2 from s1
        agentOnS1.deregister(agentMember11OnS1);
        await().until(() -> agentOnS1.membership().blockingFirst().size() == 3);
        await().until(() -> agentOnS2.membership().blockingFirst().size() == 3);
        await().until(() -> agentOnS3.membership().blockingFirst().size() == 3);

        // unhost agentNode 1 from s1
        agentOnS1.deregister(agentMember1OnS1);
        await().until(() -> agentOnS2.membership().blockingFirst().size() == 2);
        await().until(() -> agentOnS3.membership().blockingFirst().size() == 2);

        // re-host agentNode1 in s1 with different metadata
        agentOnS1.register("agentNode1");
        agentMember1OnS1 = agentOnS1.register("agentNode1");
        agentMember1OnS1.metadata(copyFromUtf8("abc"));

        await().until(() -> agentOnS1.membership().blockingFirst().size() == 3);
        await().until(() -> agentOnS2.membership().blockingFirst().size() == 3);
        await().until(() -> agentOnS3.membership().blockingFirst().size() == 3);

        // nothing will happen when unregistering agentNode3 from s1, since s3 is registered in s3
        agentOnS1.deregister(agentMember3OnS3);
        await().until(() -> agentOnS1.membership().blockingFirst().size() == 3);
        await().until(() -> agentOnS2.membership().blockingFirst().size() == 3);
        await().until(() -> agentOnS3.membership().blockingFirst().size() == 3);

        // unregister all
        agentOnS1.deregister(agentMember1OnS1);
        agentOnS2.deregister(agentMember2OnS2);
        agentOnS3.deregister(agentMember3OnS3);
        await().until(() -> agentOnS1.membership().blockingFirst().isEmpty());
        await().until(() -> agentOnS2.membership().blockingFirst().isEmpty());
        await().until(() -> agentOnS3.membership().blockingFirst().isEmpty());
    }

    @StoreCfgs(stores = {@StoreCfg(id = "s1", isSeed = true)})
    @Test
    public void testRefreshRoute() {
        IAgent agent = storeMgr.hostAgent("s1", "agent1");
        IAgentMember agentMember1 = agent.register("agentNode1");
        agentMember1.metadata(copyFromUtf8("1"));
        IAgentMember agentMember2 = agent.register("agentNode2");
        agentMember2.metadata(copyFromUtf8("2"));
        await().until(() -> agent.membership().blockingFirst().size() == 2);

        AgentMemberAddr memberAddr = agentMember1.address();
        agent.deregister(agentMember1);
        await().until(() -> {
            try {
                agentMember1.send(AgentMemberAddr.newBuilder()
                        .setName("agentNode2")
                        .setEndpoint(memberAddr.getEndpoint())
                        .setIncarnation(memberAddr.getIncarnation())
                        .build(),
                    ByteString.EMPTY, true).join();
                return false;
            } catch (Exception e) {
                Assert.assertEquals(e.getClass(), IllegalStateException.class);
                return true;
            }
        });
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "s1", isSeed = true),
        @StoreCfg(id = "s2"),
    })
    @Test
    public void testMulticast() {
        String sender = "sender";
        String receiverGroup = "receiverGroup";
        IAgent agentOnS1 = storeMgr.hostAgent("s1", "agent1");
        IAgent agentOnS2 = storeMgr.hostAgent("s2", "agent1");
        IAgentMember agentMember1 = agentOnS1.register(sender);
        agentMember1.metadata(copyFromUtf8("1"));

        IAgentMember receiverOnS1 = agentOnS1.register(receiverGroup);
        receiverOnS1.metadata(copyFromUtf8("2"));

        IAgentMember receiverOnS2 = agentOnS2.register(receiverGroup);
        receiverOnS2.metadata(copyFromUtf8("2"));

        await().until(() -> agentOnS1.membership().blockingFirst().size() == 3);
        await().until(() -> agentOnS2.membership().blockingFirst().size() == 3);

        TestObserver<AgentMessage> testObserver1 = new TestObserver<>();
        TestObserver<AgentMessage> testObserver2 = new TestObserver<>();
        receiverOnS1.receive().subscribe(testObserver1);
        receiverOnS2.receive().subscribe(testObserver2);
        agentMember1.multicast(receiverGroup, ByteString.copyFromUtf8("hello"), true);

        testObserver1.awaitCount(1);
        testObserver2.awaitCount(1);
        testObserver1.assertValue(testObserver2.values().get(0));
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "s1", isSeed = true),
        @StoreCfg(id = "s2"),
        @StoreCfg(id = "s3"),
    })
    @Test
    public void testHostClusterPartitionAndHealing() {
        IAgentHost host1 = storeMgr.getHost("s1");
        IAgentHost host2 = storeMgr.getHost("s2");
        IAgentHost host3 = storeMgr.getHost("s3");

        await().until(() -> host1.membership().blockingFirst().size() == 3);
        await().until(() -> host2.membership().blockingFirst().size() == 3);
        await().until(() -> host3.membership().blockingFirst().size() == 3);

        //  isolate s1 from others
        storeMgr.isolate("s1");
        await().forever().until(() -> host1.membership().blockingFirst().size() == 1);
        await().forever().until(() -> host2.membership().blockingFirst().size() == 2);
        await().forever().until(() -> host3.membership().blockingFirst().size() == 2);
        // integrate s1 into the cluster
        storeMgr.integrate("s1");
        await().forever().until(() -> host1.membership().blockingFirst().size() == 3);
        await().forever().until(() -> host2.membership().blockingFirst().size() == 3);
        await().forever().until(() -> host3.membership().blockingFirst().size() == 3);
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "s1", isSeed = true),
        @StoreCfg(id = "s2"),
        @StoreCfg(id = "s3"),
    })
    @Test
    public void testAgentClusterPartitionAndHealing() {
        IAgent agentOnS1 = storeMgr.hostAgent("s1", "agent");
        IAgent agentOnS2 = storeMgr.hostAgent("s2", "agent");
        IAgent agentOnS3 = storeMgr.hostAgent("s3", "agent");
        IAgentMember agentMember1OnS1 = agentOnS1.register("agentNode1OnS1");
        agentMember1OnS1.metadata(copyFromUtf8("agentNode1OnS1"));
        IAgentMember agentMember2OnS1 = agentOnS1.register("agentNode2OnS1");
        agentMember1OnS1.metadata(copyFromUtf8("agentNode2OnS1"));

        IAgentMember agentMemberOnS2 = agentOnS2.register("agentNodeOnS2");
        agentMember2OnS1.metadata(copyFromUtf8("agentNodeOnS2"));

        IAgentMember agentMemberOnS3 = agentOnS3.register("agentNodeOnS3");
        agentMemberOnS3.metadata(copyFromUtf8("agentNodeOnS3"));

        await().until(() -> agentOnS1.membership().blockingFirst().size() == 4);
        await().until(() -> agentOnS2.membership().blockingFirst().size() == 4);
        await().until(() -> agentOnS3.membership().blockingFirst().size() == 4);

        //  isolate s2 from others
        storeMgr.isolate("s1");
        await().forever().until(() -> agentOnS1.membership().blockingFirst().size() == 2);
        await().forever().until(() -> agentOnS2.membership().blockingFirst().size() == 2);
        await().forever().until(() -> agentOnS3.membership().blockingFirst().size() == 2);

        // integrate s1 into the cluster
        storeMgr.integrate("s1");
        await().forever().until(() -> agentOnS1.membership().blockingFirst().size() == 4);
        await().forever().until(() -> agentOnS2.membership().blockingFirst().size() == 4);
        await().forever().until(() -> agentOnS3.membership().blockingFirst().size() == 4);
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "s1", isSeed = true),
        @StoreCfg(id = "s2"),
    })
    @Test
    public void testCleanStaleAgentMembersAfterHostRestartWithNewEndpoint() {
        // ensure cluster up
        await().until(() -> storeMgr.membership("s1").size() == 2);
        await().until(() -> storeMgr.membership("s2").size() == 2);

        // host same agent on both hosts so CRDT survives while s1 restarts
        IAgent agentOnS1 = storeMgr.hostAgent("s1", "agentX");
        IAgent agentOnS2 = storeMgr.hostAgent("s2", "agentX");

        // register a member only on s1 to create a CRDT entry bound to s1's endpoint
        IAgentMember s1Member = agentOnS1.register("nodeOnS1");
        s1Member.metadata(copyFromUtf8("payload"));

        // both sides should observe exactly 1 member
        await().until(() -> agentOnS1.membership().blockingFirst().size() == 1);
        await().until(() -> agentOnS2.membership().blockingFirst().size() == 1);

        storeMgr.crash("s1");
        // s2 should eventually only see itself
        await().forever().until(() -> storeMgr.membership("s2").size() == 1);

        // start a new s1 instance with a new endpoint (old isolated one still exists but unreachable)
        storeMgr.startHost("s1");
        // rejoin cluster
        storeMgr.join("s1", "s2");
        // re-host the agent on s1 (no members registered now)
        IAgent newAgentOnS1 = storeMgr.hostAgent("s1", "agentX");

        // eventually, the stale member from old s1 endpoint should be cleaned from CRDT
        await().until(() -> newAgentOnS1.membership().blockingFirst().isEmpty());
        await().until(() -> agentOnS2.membership().blockingFirst().isEmpty());
    }
}
