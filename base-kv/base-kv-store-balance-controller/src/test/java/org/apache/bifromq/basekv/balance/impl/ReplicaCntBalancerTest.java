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

import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.bifromq.basekv.balance.BalanceNow;
import org.apache.bifromq.basekv.balance.BalanceResult;
import org.apache.bifromq.basekv.balance.BalanceResultType;
import org.apache.bifromq.basekv.balance.command.ChangeConfigCommand;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.utils.EffectiveRoute;
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
            .setState(State.StateType.Normal)
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
            .setState(State.StateType.Normal)
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
            .setState(State.StateType.Normal)
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
    public void balanceToRemoveLearner() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setState(State.StateType.Normal)
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
    public void promoteLearnersToVoters() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor leader = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setState(State.StateType.Normal)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("localStore")
                .addLearners("remoteStore")
                .build())
            .build();
        KVRangeDescriptor learner = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Follower)
            .setVer(1)
            .setState(State.StateType.Normal)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("localStore")
                .addLearners("remoteStore")
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("localStore")
            .addRanges(leader)
            .putStatistics("cpu.usage", 0.5)
            .build();
        KVRangeStoreDescriptor learnerStoreDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("remoteStore")
            .addRanges(learner)
            .putStatistics("cpu.usage", 0.5)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);
        storeDescriptors.add(learnerStoreDescriptor);

        balancer.update(storeDescriptors);
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) balancer.balance()).command;
        assertTrue(command.getVoters().contains("localStore"));
        assertTrue(command.getVoters().contains("remoteStore"));
        assertTrue(command.getLearners().isEmpty());
    }

    @Test
    public void balanceToAddAllRestLearners() {
        balancer = new ReplicaCntBalancer("testCluster", "localStore", 3, -1);
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setState(State.StateType.Normal)
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
            .setState(State.StateType.Normal)
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
            .setState(State.StateType.Normal)
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
            .setState(State.StateType.Normal)
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
            .setState(State.StateType.Normal)
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
            .setState(State.StateType.Normal)
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
            .setState(State.StateType.Normal)
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
            balancer.doGenerate(balancer.initialLoadRules(), storeDescriptors, effectiveRoute);

        assertTrue(balancer.verify(layout, allStoreDescriptors));
    }

    @Test
    public void removeDeadVoterAndBackfillEvenIfCountEqualsExpected() {
        // live: s1, s2, s3；expected voters=3
        // range current voters = [s1, ghost, s2]
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor range = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setState(State.StateType.Normal)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("s1")
                .addVoters("ghost")
                .addVoters("s2")
                .build())
            .build();

        KVRangeStoreDescriptor s1 = KVRangeStoreDescriptor.newBuilder().setId("s1").addRanges(range).build();
        KVRangeStoreDescriptor s2 = KVRangeStoreDescriptor.newBuilder().setId("s2").build();
        KVRangeStoreDescriptor s3 = KVRangeStoreDescriptor.newBuilder().setId("s3").build();

        Set<KVRangeStoreDescriptor> stores = new HashSet<>();
        stores.add(s1);
        stores.add(s2);
        stores.add(s3);

        // votersPerRange=3，learnersPerRange=0
        balancer = new ReplicaCntBalancer("testCluster", "s1", 3, 0);
        balancer.update(stores);

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand cmd = (ChangeConfigCommand) ((BalanceNow<?>) result).command;

        // expected：ghost removed s3 added
        assertEquals(cmd.getKvRangeId(), kvRangeId);
        assertTrue(cmd.getVoters().contains("s1"));
        assertTrue(cmd.getVoters().contains("s2"));
        assertTrue(cmd.getVoters().contains("s3"));
        assertFalse(cmd.getVoters().contains("ghost"));
        assertTrue(cmd.getLearners().isEmpty());
    }

    @Test
    public void abortWhenConfigChangeInProgress_nextFieldsPresent() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        ClusterConfig cfgWithNext = ClusterConfig.newBuilder()
            .addVoters("localStore")
            .addNextVoters("someone")
            .build();

        KVRangeDescriptor range = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(cfgWithNext)
            .build();

        KVRangeStoreDescriptor local = KVRangeStoreDescriptor.newBuilder()
            .setId("localStore")
            .addRanges(range)
            .build();

        Set<KVRangeStoreDescriptor> stores = new HashSet<>();
        stores.add(local);

        balancer = new ReplicaCntBalancer("testCluster", "localStore", 1, 0);
        balancer.update(stores);

        assertSame(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void learnersMinusOneUsesLiveMinusVotersAndSanitizes() {
        // expectedLearners = -1 => learners = live - voters；
        balancer = new ReplicaCntBalancer("testCluster", "s1", 1, -1);

        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor range = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setState(State.StateType.Normal)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("s1")
                .addLearners("ghostLearner")
                .build())
            .build();

        KVRangeStoreDescriptor s1 = KVRangeStoreDescriptor.newBuilder().setId("s1").addRanges(range).build();
        KVRangeStoreDescriptor s2 = KVRangeStoreDescriptor.newBuilder().setId("s2").build();
        KVRangeStoreDescriptor s3 = KVRangeStoreDescriptor.newBuilder().setId("s3").build();

        Set<KVRangeStoreDescriptor> stores = new HashSet<>();
        stores.add(s1);
        stores.add(s2);
        stores.add(s3);

        balancer.update(stores);

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand cmd = (ChangeConfigCommand) ((BalanceNow<?>) result).command;

        // expected：learners = live - voters = {s2, s3}；ghostLearner removed
        assertTrue(cmd.getVoters().contains("s1"));
        assertFalse(cmd.getLearners().contains("ghostLearner"));
        assertTrue(cmd.getLearners().contains("s2"));
        assertTrue(cmd.getLearners().contains("s3"));
        assertEquals(cmd.getLearners().size(), 2);
    }

    @Test
    public void skipWhenCapacityInsufficientAndHasDeadVoter() {
        // expected voters=3，live voters=S1,S2, S3(dead)
        ReplicaCntBalancer balancer = new ReplicaCntBalancer("testCluster", "s1", 3, 0);

        KVRangeId rid = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor range = KVRangeDescriptor.newBuilder()
            .setId(rid)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("s1")
                .addVoters("s2")
                .addVoters("deadS3") // dead
                .build())
            .build();

        KVRangeStoreDescriptor s1 = KVRangeStoreDescriptor.newBuilder().setId("s1").addRanges(range).build();
        KVRangeStoreDescriptor s2 = KVRangeStoreDescriptor.newBuilder().setId("s2").build();
        Set<KVRangeStoreDescriptor> landscape = new HashSet<>();
        landscape.add(s1);
        landscape.add(s2);

        balancer.update(landscape);

        assertSame(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void learnersMinusOnePreferPromoteLearnersToFillVoters() {
        // expected: voters=3, learners=-1
        balancer = new ReplicaCntBalancer("testCluster", "s1", 3, -1);

        KVRangeId rid = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor range = KVRangeDescriptor.newBuilder()
            .setId(rid)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setState(State.StateType.Normal)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("s1")
                .addLearners("s2")
                .addLearners("s3")
                .addLearners("s4")
                .build())
            .build();

        KVRangeStoreDescriptor s1 = KVRangeStoreDescriptor.newBuilder().setId("s1").addRanges(range).build();
        KVRangeStoreDescriptor s2 = KVRangeStoreDescriptor.newBuilder().setId("s2").build();
        KVRangeStoreDescriptor s3 = KVRangeStoreDescriptor.newBuilder().setId("s3").build();
        KVRangeStoreDescriptor s4 = KVRangeStoreDescriptor.newBuilder().setId("s4").build();

        balancer.update(Set.of(s1, s2, s3, s4));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand cmd = (ChangeConfigCommand) ((BalanceNow<?>) result).command;

        // voters should be s1 + two from {s2,s3,s4}
        assertTrue(cmd.getVoters().contains("s1"));
        assertEquals(cmd.getVoters().size(), 3);
        // after promotion, learners should be live - voters = the remaining one
        assertEquals(cmd.getLearners().size(), 1);
        Set<String> all = Set.of("s1", "s2", "s3", "s4");
        Set<String> union = new HashSet<>(cmd.getVoters());
        union.addAll(cmd.getLearners());
        assertEquals(union, all);
    }

    @Test
    public void noChangeWhenLiveLessThanExpectedAndNoDeadVoter() {
        // expected voters=3, live={s1,s2}, voters={s1,s2}
        balancer = new ReplicaCntBalancer("testCluster", "s1", 3, 0);

        KVRangeId rid = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor range = KVRangeDescriptor.newBuilder()
            .setId(rid)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setState(State.StateType.Normal)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("s1")
                .addVoters("s2")
                .build())
            .build();

        KVRangeStoreDescriptor s1 = KVRangeStoreDescriptor.newBuilder().setId("s1").addRanges(range).build();
        KVRangeStoreDescriptor s2 = KVRangeStoreDescriptor.newBuilder().setId("s2").build();

        balancer.update(Set.of(s1, s2));
        assertSame(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void neverRemoveLeaderWhenShrinkingVoters() {
        // expected voters=3, voters currently 4 (leader must stay)
        balancer = new ReplicaCntBalancer("testCluster", "leader", 3, 0);

        KVRangeId rid = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor range = KVRangeDescriptor.newBuilder()
            .setId(rid)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setState(State.StateType.Normal)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("leader")
                .addVoters("s2")
                .addVoters("s3")
                .addVoters("s4")
                .build())
            .build();

        KVRangeStoreDescriptor leader = KVRangeStoreDescriptor.newBuilder().setId("leader").addRanges(range).build();
        KVRangeStoreDescriptor s2 = KVRangeStoreDescriptor.newBuilder().setId("s2").build();
        KVRangeStoreDescriptor s3 = KVRangeStoreDescriptor.newBuilder().setId("s3").build();
        KVRangeStoreDescriptor s4 = KVRangeStoreDescriptor.newBuilder().setId("s4").build();

        balancer.update(Set.of(leader, s2, s3, s4));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand cmd = (ChangeConfigCommand) ((BalanceNow<?>) result).command;

        assertTrue(cmd.getVoters().contains("leader"));
        assertEquals(cmd.getVoters().size(), 3);
    }

    @Test
    public void balanceVoterCountNoopWhenSpreadWithinOne() {
        // two stores, two ranges: counts differ by at most 1 -> no rebalance
        balancer = new ReplicaCntBalancer("testCluster", "s1", 1, 0);

        KVRangeId r1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor d1 = KVRangeDescriptor.newBuilder()
            .setId(r1).setVer(1).setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("m")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters("s1").build())
            .build();

        KVRangeId r2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        KVRangeDescriptor d2 = KVRangeDescriptor.newBuilder()
            .setId(r2).setVer(1).setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("m")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters("s2").build())
            .build();

        KVRangeStoreDescriptor s1 = KVRangeStoreDescriptor.newBuilder().setId("s1").addRanges(d1).build();
        KVRangeStoreDescriptor s2 = KVRangeStoreDescriptor.newBuilder().setId("s2").addRanges(d2).build();

        balancer.update(Set.of(s1, s2));
        assertSame(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void fixedLearnerCountRemovesDeadAndBackfills() {
        // expected learners=2; current learners={deadL, s2}; live={s1,s2,s3,s4}
        balancer = new ReplicaCntBalancer("testCluster", "s1", 1, 2);

        KVRangeId rid = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor range = KVRangeDescriptor.newBuilder()
            .setId(rid)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setState(State.StateType.Normal)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("s1")
                .addLearners("deadL")
                .addLearners("s2")
                .build())
            .build();

        KVRangeStoreDescriptor s1 = KVRangeStoreDescriptor.newBuilder().setId("s1").addRanges(range).build();
        KVRangeStoreDescriptor s2 = KVRangeStoreDescriptor.newBuilder().setId("s2").build();
        KVRangeStoreDescriptor s3 = KVRangeStoreDescriptor.newBuilder().setId("s3").build();
        KVRangeStoreDescriptor s4 = KVRangeStoreDescriptor.newBuilder().setId("s4").build();

        balancer.update(Set.of(s1, s2, s3, s4));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand cmd = (ChangeConfigCommand) ((BalanceNow<?>) result).command;

        assertTrue(cmd.getVoters().contains("s1"));
        assertEquals(cmd.getLearners().size(), 2);
        assertTrue(cmd.getLearners().contains("s2"));
        assertFalse(cmd.getLearners().contains("deadL"));
    }

    @Test
    public void zeroLearnersTargetClearsLearners() {
        // expected learners=0
        balancer = new ReplicaCntBalancer("testCluster", "s1", 1, 0);

        KVRangeId rid = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor range = KVRangeDescriptor.newBuilder()
            .setId(rid)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setState(State.StateType.Normal)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("s1")
                .addLearners("s2")
                .addLearners("s3")
                .build())
            .build();

        KVRangeStoreDescriptor s1 = KVRangeStoreDescriptor.newBuilder().setId("s1").addRanges(range).build();
        KVRangeStoreDescriptor s2 = KVRangeStoreDescriptor.newBuilder().setId("s2").build();
        KVRangeStoreDescriptor s3 = KVRangeStoreDescriptor.newBuilder().setId("s3").build();

        balancer.update(Set.of(s1, s2, s3));
        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand cmd = (ChangeConfigCommand) ((BalanceNow<?>) result).command;
        assertTrue(cmd.getLearners().isEmpty());
        assertTrue(cmd.getVoters().contains("s1"));
    }

    @Test
    public void learnersMinusOneWithAllLiveAsVotersMakesLearnersEmpty() {
        balancer = new ReplicaCntBalancer("testCluster", "s1", 3, -1);

        KVRangeId rid = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor range = KVRangeDescriptor.newBuilder()
            .setId(rid)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setState(State.StateType.Normal)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("s1")
                .addVoters("s2")
                .addVoters("s3")
                .addLearners("ghost") // should be sanitized away
                .build())
            .build();

        KVRangeStoreDescriptor s1 = KVRangeStoreDescriptor.newBuilder().setId("s1").addRanges(range).build();
        KVRangeStoreDescriptor s2 = KVRangeStoreDescriptor.newBuilder().setId("s2").build();
        KVRangeStoreDescriptor s3 = KVRangeStoreDescriptor.newBuilder().setId("s3").build();

        balancer.update(Set.of(s1, s2, s3));
        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand cmd = (ChangeConfigCommand) ((BalanceNow<?>) result).command;

        assertEquals(cmd.getVoters(), Set.of("s1", "s2", "s3"));
        assertTrue(cmd.getLearners().isEmpty());
    }

    @Test
    public void balanceVoterCountPrefersZeroCountStoreFirst() {
        KVRangeId r1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeId r2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        KVRangeId r3 = KVRangeId.newBuilder().setEpoch(1).setId(3).build();
        KVRangeDescriptor d1 = KVRangeDescriptor.newBuilder()
            .setId(r1)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("m")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters("sA").build())
            .build();
        KVRangeDescriptor d2 = KVRangeDescriptor.newBuilder()
            .setId(r2)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("m"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .setConfig(ClusterConfig.newBuilder().addVoters("sA").build())
            .build();

        KVRangeStoreDescriptor sA = KVRangeStoreDescriptor.newBuilder().setId("sA").addRanges(d1).addRanges(d2).build();
        KVRangeStoreDescriptor sB = KVRangeStoreDescriptor.newBuilder().setId("sB")
            .addRanges(KVRangeDescriptor.newBuilder()
                .setId(r3)
                .setVer(1)
                .setRole(RaftNodeStatus.Leader)
                .setState(State.StateType.Normal)
                .setBoundary(Boundary.newBuilder()
                    .setStartKey(ByteString.copyFromUtf8("z"))
                    .build())
                .setConfig(ClusterConfig.newBuilder().addVoters("sB").build())
                .build())
            .build();
        KVRangeStoreDescriptor sC = KVRangeStoreDescriptor.newBuilder().setId("sC").build();

        balancer = new ReplicaCntBalancer("testCluster", "sA", 1, 0);
        balancer.update(Set.of(sA, sB, sC));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand cmd = (ChangeConfigCommand) ((BalanceNow<?>) result).command;

        assertTrue(cmd.getVoters().contains("sC"));
        assertFalse(cmd.getVoters().contains("sA"));
    }

    @Test
    public void balanceVoterCountDoesOnlyOneChangePerRound() {
        KVRangeId r1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeId r2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        KVRangeDescriptor d1 = KVRangeDescriptor.newBuilder()
            .setId(r1)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("m")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters("sA").build())
            .build();
        KVRangeDescriptor d2 = KVRangeDescriptor.newBuilder()
            .setId(r2)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("m")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters("sA").build())
            .build();

        KVRangeStoreDescriptor sA = KVRangeStoreDescriptor.newBuilder().setId("sA").addRanges(d1).addRanges(d2).build();
        KVRangeStoreDescriptor sB = KVRangeStoreDescriptor.newBuilder().setId("sB").build();
        KVRangeStoreDescriptor sC = KVRangeStoreDescriptor.newBuilder().setId("sC").build();

        balancer = new ReplicaCntBalancer("testCluster", "sA", 1, 0);
        balancer.update(Set.of(sA, sB, sC));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand cmd = (ChangeConfigCommand) ((BalanceNow<?>) result).command;
        assertTrue(cmd.getKvRangeId().equals(r1) || cmd.getKvRangeId().equals(r2));
    }

    @Test
    public void balanceVoterCountSkipsTargetsAlreadyInVotersOrLearners() {
        KVRangeId r1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor d1 = KVRangeDescriptor.newBuilder()
            .setId(r1)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("sA")
                .addLearners("sB")
                .build())
            .build();

        KVRangeDescriptor d2 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setEpoch(1).setId(2).build())
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("sA")
                .addLearners("sB")
                .build())
            .build();

        KVRangeStoreDescriptor sA = KVRangeStoreDescriptor.newBuilder().setId("sA").addRanges(d1).addRanges(d2).build();
        KVRangeStoreDescriptor sB = KVRangeStoreDescriptor.newBuilder().setId("sB").build();
        KVRangeStoreDescriptor sC = KVRangeStoreDescriptor.newBuilder().setId("sC").build();

        balancer = new ReplicaCntBalancer("testCluster", "sA", 1, 1);
        balancer.update(Set.of(sA, sB, sC));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand cmd = (ChangeConfigCommand) ((BalanceNow<?>) result).command;

        assertTrue(cmd.getVoters().contains("sC"));
        assertFalse(cmd.getVoters().contains("sB"));
    }
}
