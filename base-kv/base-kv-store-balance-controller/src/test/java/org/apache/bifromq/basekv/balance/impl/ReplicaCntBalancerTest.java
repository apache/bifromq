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

package org.apache.bifromq.basekv.balance.impl;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.DescriptorUtil.getEffectiveEpoch;
import static org.apache.bifromq.basekv.utils.DescriptorUtil.getEffectiveRoute;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.balance.BalanceNow;
import org.apache.bifromq.basekv.balance.BalanceResult;
import org.apache.bifromq.basekv.balance.BalanceResultType;
import org.apache.bifromq.basekv.balance.command.ChangeConfigCommand;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.utils.EffectiveRoute;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ReplicaCntBalancerTest {
    private ReplicaCntBalancer balancer;

    @BeforeMethod
    public void setUp() {
        balancer = new ReplicaCntBalancer("testCluster", "localStore", 3, 2);
    }

    @Test
    public void balanceWithNoLeaderRange() {
        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        balancer.update(storeDescriptors);

        assertSame(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void balanceToAddVoter() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("a")).build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("s1")
                .addVoters("s2")
                .addVoters("s3")
                .build())
            .build();

        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a")).build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("s1")
                .addVoters("s2")
                .build())
            .build();

        KVRangeStoreDescriptor store1Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("s1")
            .addRanges(kvRangeDescriptor1)
            .build();
        KVRangeStoreDescriptor store2Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("s2")
            .addRanges(kvRangeDescriptor2)
            .build();
        KVRangeStoreDescriptor store3Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("s3")
            .addRanges(kvRangeDescriptor1)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(store1Descriptor);
        storeDescriptors.add(store2Descriptor);
        storeDescriptors.add(store3Descriptor);

        balancer = new ReplicaCntBalancer("testCluster", "s2", 3, 0);
        balancer.update(storeDescriptors);

        BalanceResult result = balancer.balance();
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;
        assertEquals(command.getToStore(), "s2");
        assertEquals(command.getKvRangeId(), kvRangeId2);
        assertTrue(command.getVoters().contains("s1"));
        assertTrue(command.getVoters().contains("s2"));
        assertTrue(command.getVoters().contains("s3"));
        assertTrue(command.getLearners().isEmpty());
    }

    @Test
    public void balanceToAddLearner() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("localStore")
                .addVoters("voterStore2")
                .addVoters("voterStore3")
                .build())
            .build();

        KVRangeStoreDescriptor voterStore1Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("localStore")
            .addRanges(kvRangeDescriptor)
            .build();
        KVRangeStoreDescriptor voterStore2Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("voterStore2")
            .build();
        KVRangeStoreDescriptor voterStore3Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("voterStore3")
            .build();
        KVRangeStoreDescriptor learnerStoreDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("learnerStore")
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(voterStore1Descriptor);
        storeDescriptors.add(voterStore2Descriptor);
        storeDescriptors.add(voterStore3Descriptor);
        storeDescriptors.add(learnerStoreDescriptor);

        balancer.update(storeDescriptors);

        BalanceResult result = balancer.balance();
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;
        assertTrue(command.getLearners().contains("learnerStore"));
    }

    @Test
    public void balanceToRemoveVoter() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("localStore")
                .addVoters("remoteStore")
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("localStore")
            .addRanges(kvRangeDescriptor)
            .putStatistics("cpu.usage", 0.5)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);

        balancer.update(storeDescriptors);

        BalanceResult result = balancer.balance();
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;
        assertTrue(command.getVoters().contains("localStore"));
        assertFalse(command.getVoters().contains("remoteStore"));
        assertTrue(command.getLearners().isEmpty());
    }

    @Test
    public void balanceToRemoveLearner() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("localStore")
                .addLearners("learnerStore")
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("localStore")
            .addRanges(kvRangeDescriptor)
            .putStatistics("cpu.usage", 0.5)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);

        balancer.update(storeDescriptors);

        BalanceResult result = balancer.balance();
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;
        assertTrue(command.getVoters().contains("localStore"));
        assertTrue(command.getLearners().isEmpty());
    }

    @Test
    public void nothingChanged() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("localStore")
                .addLearners("learnerStore")
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("localStore")
            .addRanges(kvRangeDescriptor)
            .putStatistics("cpu.usage", 0.5)
            .build();
        KVRangeStoreDescriptor learnerStoreDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("learnerStore")
            .addRanges(kvRangeDescriptor)
            .putStatistics("cpu.usage", 0.5)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);
        storeDescriptors.add(learnerStoreDescriptor);

        balancer.update(storeDescriptors);

        assertSame(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void balanceToAddAllRestLearners() {
        balancer = new ReplicaCntBalancer("testCluster", "localStore", 3, -1);
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("localStore")
                .addVoters("voterStore2")
                .addVoters("voterStore3")
                .build())
            .build();

        KVRangeStoreDescriptor voterStore1Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("localStore")
            .addRanges(kvRangeDescriptor)
            .putStatistics("cpu.usage", 0.5)
            .build();
        KVRangeStoreDescriptor voterStore2Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("voterStore2")
            .putStatistics("cpu.usage", 0.5)
            .build();
        KVRangeStoreDescriptor voterStore3Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("voterStore3")
            .putStatistics("cpu.usage", 0.5)
            .build();
        KVRangeStoreDescriptor learnerStoreDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("learnerStore")
            .putStatistics("cpu.usage", 0.5)
            .build();
        KVRangeStoreDescriptor learnerStore1Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("learnerStore1")
            .putStatistics("cpu.usage", 0.5)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(voterStore1Descriptor);
        storeDescriptors.add(voterStore2Descriptor);
        storeDescriptors.add(voterStore3Descriptor);
        storeDescriptors.add(learnerStoreDescriptor);
        storeDescriptors.add(learnerStore1Descriptor);

        balancer.update(storeDescriptors);

        BalanceResult result = balancer.balance();
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;
        assertTrue(command.getLearners().contains("learnerStore"));
        assertTrue(command.getLearners().contains("learnerStore1"));
    }

    @Test
    public void balanceVoterCount() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("localStore")
                .build())
            .build();
        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setVer(2)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("localStore")
                .build())
            .build();
        KVRangeStoreDescriptor localStoreDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("localStore")
            .addRanges(kvRangeDescriptor1)
            .addRanges(kvRangeDescriptor2)
            .build();
        KVRangeStoreDescriptor underloadedStoreDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("underloadedStore")
            .build();
        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(underloadedStoreDescriptor);
        storeDescriptors.add(localStoreDescriptor);
        balancer = new ReplicaCntBalancer("testCluster", "localStore", 1, 0);
        balancer.update(storeDescriptors);

        BalanceResult result = balancer.balance();
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;
        assertEquals(command.getToStore(), "localStore");
        assertEquals(command.getKvRangeId(), kvRangeId1);
        assertEquals(command.getExpectedVer(), kvRangeDescriptor1.getVer());
        assertTrue(command.getVoters().contains("underloadedStore"));
        assertFalse(command.getVoters().contains("localStore"));
    }

    @Test
    public void balanceLearnerCount() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("a")).build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("s1")
                .addLearners("s2")
                .build())
            .build();
        KVRangeStoreDescriptor store1Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("s1")
            .addRanges(kvRangeDescriptor1)
            .build();

        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setVer(2)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("s2")
                .addLearners("s1")
                .build())
            .build();
        KVRangeStoreDescriptor store2Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("s2")
            .addRanges(kvRangeDescriptor2)
            .build();

        // s3 is underloaded on learner count
        KVRangeId kvRangeId3 = KVRangeId.newBuilder().setEpoch(1).setId(3).build();
        KVRangeDescriptor kvRangeDescriptor3 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId3)
            .setVer(2)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("s3")
                .addLearners("s2")
                .build())
            .build();
        KVRangeStoreDescriptor store3Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("s3")
            .addRanges(kvRangeDescriptor3)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(store1Descriptor);
        storeDescriptors.add(store2Descriptor);
        storeDescriptors.add(store3Descriptor);
        balancer = new ReplicaCntBalancer("testCluster", "s1", 1, 1);
        balancer.update(storeDescriptors);

        BalanceResult result = balancer.balance();
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;
        assertEquals(command.getToStore(), "s1");
        assertEquals(command.getKvRangeId(), kvRangeId1);
        assertEquals(command.getExpectedVer(), kvRangeDescriptor1.getVer());
        assertTrue(command.getLearners().contains("s3"));
        assertTrue(command.getVoters().contains("s1"));
    }

    @Test
    public void generateCorrectClusterConfig() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("localStore")
                .addLearners("learnerStore")
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("localStore")
            .addRanges(kvRangeDescriptor)
            .putStatistics("cpu.usage", 0.5)
            .build();
        KVRangeStoreDescriptor learnerStoreDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("learnerStore")
            .addRanges(kvRangeDescriptor)
            .putStatistics("cpu.usage", 0.5)
            .build();

        Set<KVRangeStoreDescriptor> allStoreDescriptors = new HashSet<>();
        Map<String, KVRangeStoreDescriptor> storeDescriptors = new HashMap<>();
        allStoreDescriptors.add(storeDescriptor);
        allStoreDescriptors.add(learnerStoreDescriptor);
        storeDescriptors.put(storeDescriptor.getId(), storeDescriptor);
        storeDescriptors.put(learnerStoreDescriptor.getId(), learnerStoreDescriptor);

        EffectiveRoute effectiveRoute = getEffectiveRoute(getEffectiveEpoch(allStoreDescriptors).get());

        Map<Boundary, ClusterConfig> layout =
            balancer.doGenerate(balancer.defaultLoadRules(), storeDescriptors, effectiveRoute);

        assertTrue(balancer.verify(layout, allStoreDescriptors));
    }
}
