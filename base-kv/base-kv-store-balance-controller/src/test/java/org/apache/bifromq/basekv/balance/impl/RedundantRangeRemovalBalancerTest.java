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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bifromq.basekv.balance.BalanceNow;
import org.apache.bifromq.basekv.balance.BalanceResult;
import org.apache.bifromq.basekv.balance.BalanceResultType;
import org.apache.bifromq.basekv.balance.command.ChangeConfigCommand;
import org.apache.bifromq.basekv.balance.command.QuitCommand;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RedundantRangeRemovalBalancerTest {
    private final String clusterId = "testCluster";
    private final String localStoreId = "localStore";
    private RedundantRangeRemovalBalancer balancer;
    private AtomicLong mockTime;

    @BeforeMethod
    public void setUp() {
        mockTime = new AtomicLong(0L); // Start time at 0
        balancer = new RedundantRangeRemovalBalancer(clusterId, localStoreId, Duration.ofSeconds(1), mockTime::get);
    }

    @Test
    public void noRedundantEpoch() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setVer(1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);

        balancer.update(storeDescriptors);

        assertSame(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void removeRangeInRedundantEpoch() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setVer(1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("m"))
                .build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .build())
            .build();

        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(2).setId(2).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setVer(1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("n"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("otherStore")
                .addVoters(localStoreId)
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor1)
            .addRanges(kvRangeDescriptor2)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = Set.of(storeDescriptor1);

        balancer.update(storeDescriptors);

        BalanceResult command = balancer.balance();
        // first returns AwaitBalance due to suspicion delay
        assertEquals(command.type(), BalanceResultType.AwaitBalance);
        // advance mock time beyond the max suspicion window (2s)
        mockTime.set(3000L);
        command = balancer.balance();
        assertEquals(command.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand changeConfigCommand = (ChangeConfigCommand) ((BalanceNow<?>) command).command;

        assertEquals(changeConfigCommand.getToStore(), localStoreId);
        assertEquals(changeConfigCommand.getKvRangeId(), kvRangeId2);
        assertEquals(changeConfigCommand.getVoters(), Set.of(localStoreId));
        assertEquals(changeConfigCommand.getLearners(), Collections.emptySet());
    }

    @Test
    public void noLocalLeaderRangeInRedundantEpoch() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setState(State.StateType.Normal)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("m"))
                .build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .build())
            .build();

        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(2).setId(2).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setRole(RaftNodeStatus.Follower)
            .setState(State.StateType.Normal)
            .setVer(1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("n"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor1)
            .addRanges(kvRangeDescriptor2)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = Set.of(storeDescriptor1);

        balancer.update(storeDescriptors);

        assertSame(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void removeRedundantEffectiveRange() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        Boundary boundary = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("z"))
            .build();
        ClusterConfig config = ClusterConfig.newBuilder().addVoters(localStoreId).build();

        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setBoundary(boundary)
            .setConfig(config)
            .build();
        KVRangeDescriptor range2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setBoundary(boundary)
            .setConfig(config)
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(range1)
            .addRanges(range2)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = Set.of(storeDescriptor);

        balancer.update(storeDescriptors);

        BalanceResult result = balancer.balance();
        // first returns AwaitBalance due to suspicion delay
        assertEquals(result.type(), BalanceResultType.AwaitBalance);
        mockTime.set(3000L);
        result = balancer.balance();
        assertEquals(result.type(), BalanceResultType.BalanceNow);

        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;
        assertEquals(command.getToStore(), localStoreId);
        assertEquals(command.getKvRangeId(), kvRangeId2);
        assertEquals(command.getVoters(), Collections.emptySet());
        assertEquals(command.getLearners(), Collections.emptySet());
    }

    @Test
    public void ignoreNonLocalStore() {
        String nonLocalStoreId = "otherStore";
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setVer(1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .setConfig(ClusterConfig.newBuilder().addVoters(nonLocalStoreId).build())
            .build();

        KVRangeStoreDescriptor nonLocalStoreDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId(nonLocalStoreId)
            .addRanges(kvRangeDescriptor)
            .build();
        Set<KVRangeStoreDescriptor> landscape = Collections.singleton(nonLocalStoreDescriptor);

        balancer.update(landscape);

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void removeIdConflictingRangeWhenLocalStoreIsLoser() {
        String peerStoreId = "aStore";
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        Boundary boundary = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("z"))
            .build();

        // Local store is Leader of the range
        KVRangeDescriptor localRange = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setVer(1)
            .setBoundary(boundary)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .build())
            .build();

        KVRangeDescriptor peerRange = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(boundary)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(peerStoreId)
                .build())
            .build();

        KVRangeStoreDescriptor localStoreDesc = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(localRange)
            .build();

        KVRangeStoreDescriptor peerStoreDesc = KVRangeStoreDescriptor.newBuilder()
            .setId(peerStoreId)
            .addRanges(peerRange)
            .build();

        balancer.update(Set.of(localStoreDesc, peerStoreDesc));

        BalanceResult result = balancer.balance();
        // first returns AwaitBalance due to suspicion delay
        assertEquals(result.type(), BalanceResultType.AwaitBalance);
        mockTime.set(3000L);
        result = balancer.balance();
        assertEquals(result.type(), BalanceResultType.BalanceNow);

        ChangeConfigCommand cmd = (ChangeConfigCommand) ((BalanceNow<?>) result).command;
        assertEquals(cmd.getToStore(), localStoreId);
        assertEquals(cmd.getKvRangeId(), kvRangeId);
        assertEquals(cmd.getVoters(), Collections.emptySet());
        assertEquals(cmd.getLearners(), Collections.emptySet());
    }

    @Test
    public void ignoreIdConflictingRangeWhenLocalStoreIsWinner() {
        String peerStoreId = "zStore";              // larger than "localStore"
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        Boundary boundary = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("z"))
            .build();

        KVRangeDescriptor localRange = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setState(State.StateType.Normal)
            .setBoundary(boundary)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .build())
            .build();

        KVRangeDescriptor peerRange = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setState(State.StateType.Normal)
            .setBoundary(boundary)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(peerStoreId)
                .build())
            .build();

        KVRangeStoreDescriptor localStoreDesc = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(localRange)
            .build();

        KVRangeStoreDescriptor peerStoreDesc = KVRangeStoreDescriptor.newBuilder()
            .setId(peerStoreId)
            .addRanges(peerRange)
            .build();

        balancer.update(Set.of(localStoreDesc, peerStoreDesc));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void idConflictButVotersOverlapShouldNotDelete() {
        String peerStoreId = "peer";
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        Boundary boundary = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("z")).build();

        KVRangeDescriptor localRange = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setState(State.StateType.Normal)
            .setBoundary(boundary)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .addVoters("x").build())
            .build();

        KVRangeDescriptor peerRange = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setState(State.StateType.Normal)
            .setBoundary(boundary)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .addVoters(peerStoreId).build())
            .build();

        KVRangeStoreDescriptor localStoreDesc = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(localRange)
            .build();
        KVRangeStoreDescriptor peerStoreDesc = KVRangeStoreDescriptor.newBuilder()
            .setId(peerStoreId)
            .addRanges(peerRange)
            .build();

        balancer.update(Set.of(localStoreDesc, peerStoreDesc));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void scheduleQuitForZombieCandidateNotInEffectiveMap() {
        // Effective leaders forming a valid split set
        KVRangeId effId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeId effId2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        // Make effective leader boundaries a valid split set: [null, m) and [m, null)
        Boundary b1 = Boundary.newBuilder()
            .setEndKey(ByteString.copyFromUtf8("m")).build();
        Boundary b2 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("m")).build();
        String leaderStore = "leaderStore";
        KVRangeDescriptor effRange1 = KVRangeDescriptor.newBuilder()
            .setId(effId1).setVer(1).setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setBoundary(b1)
            .setConfig(ClusterConfig.newBuilder().addVoters(leaderStore).build())
            .build();
        KVRangeDescriptor effRange2 = KVRangeDescriptor.newBuilder()
            .setId(effId2).setVer(1).setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setBoundary(b2)
            .setConfig(ClusterConfig.newBuilder().addVoters(leaderStore).build())
            .build();
        KVRangeStoreDescriptor leaderDesc = KVRangeStoreDescriptor.newBuilder()
            .setId(leaderStore)
            .addRanges(effRange1)
            .addRanges(effRange2)
            .build();

        // Local zombie candidate with a different id that doesn't exist in effective map
        KVRangeId zombieId = KVRangeId.newBuilder().setEpoch(1).setId(99).build();
        KVRangeDescriptor zombieCandidate = KVRangeDescriptor.newBuilder()
            .setId(zombieId).setVer(1).setRole(RaftNodeStatus.Candidate)
            .setState(State.StateType.Normal)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters("other").build())
            .build();
        KVRangeStoreDescriptor localDesc = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(zombieCandidate)
            .build();

        balancer.update(Set.of(leaderDesc, localDesc));

        BalanceResult result = balancer.balance();
        // first AwaitBalance due to suspicion delay
        assertEquals(result.type(), BalanceResultType.AwaitBalance);
        mockTime.set(3000L);
        result = balancer.balance();
        assertEquals(result.type(), BalanceResultType.BalanceNow);
        QuitCommand cmd = (QuitCommand) ((BalanceNow<?>) result).command;
        assertEquals(cmd.getToStore(), localStoreId);
        assertEquals(cmd.getKvRangeId(), zombieId);
    }

    @Test
    public void doNotQuitCandidateIfLocalIsInEffectiveReplicaSet() {
        // Effective leader contains localStore in replica sets
        KVRangeId id = KVRangeId.newBuilder().setEpoch(1).setId(7).build();
        // Single leader must be FULL_BOUNDARY to be valid split set
        Boundary b = Boundary.newBuilder().build();
        String leaderStore = "leaderStore";
        ClusterConfig effCfg = ClusterConfig.newBuilder()
            .addVoters(leaderStore)
            .addLearners(localStoreId) // local exists in effective replica set
            .build();
        KVRangeDescriptor effLeader = KVRangeDescriptor.newBuilder()
            .setId(id).setVer(1).setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setBoundary(b)
            .setConfig(effCfg)
            .build();
        KVRangeStoreDescriptor leaderDesc = KVRangeStoreDescriptor.newBuilder()
            .setId(leaderStore)
            .addRanges(effLeader)
            .build();

        // Local candidate with the same id
        KVRangeDescriptor localCandidate = KVRangeDescriptor.newBuilder()
            .setId(id).setVer(1).setRole(RaftNodeStatus.Candidate)
            .setState(State.StateType.Normal)
            .setBoundary(b)
            .setConfig(ClusterConfig.newBuilder().addVoters(leaderStore).build())
            .build();
        KVRangeStoreDescriptor localDesc = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(localCandidate)
            .build();

        balancer.update(Set.of(leaderDesc, localDesc));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void quitCandidateIfLocalNotInEffectiveReplicaSet() {
        // Effective leader without local in any replica set
        KVRangeId id = KVRangeId.newBuilder().setEpoch(1).setId(8).build();
        // Single leader must be FULL_BOUNDARY to be valid split set
        Boundary b = Boundary.newBuilder().build();
        String leaderStore = "leaderStore";
        ClusterConfig effCfg = ClusterConfig.newBuilder()
            .addVoters(leaderStore)
            .build();
        KVRangeDescriptor effLeader = KVRangeDescriptor.newBuilder()
            .setId(id).setVer(1).setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setBoundary(b)
            .setConfig(effCfg)
            .build();
        KVRangeStoreDescriptor leaderDesc = KVRangeStoreDescriptor.newBuilder()
            .setId(leaderStore)
            .addRanges(effLeader)
            .build();

        // Local candidate with the same id but local not in effective config
        KVRangeDescriptor localCandidate = KVRangeDescriptor.newBuilder()
            .setId(id).setVer(1).setRole(RaftNodeStatus.Candidate)
            .setState(State.StateType.Normal)
            .setBoundary(b)
            .setConfig(ClusterConfig.newBuilder().addVoters(leaderStore).build())
            .build();
        KVRangeStoreDescriptor localDesc = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(localCandidate)
            .build();

        balancer.update(Set.of(leaderDesc, localDesc));

        BalanceResult result = balancer.balance();
        // first AwaitBalance due to suspicion delay
        assertEquals(result.type(), BalanceResultType.AwaitBalance);
        mockTime.set(3000L);
        result = balancer.balance();
        assertEquals(result.type(), BalanceResultType.BalanceNow);
        assertTrue(((BalanceNow<?>) result).command instanceof QuitCommand);
    }

    @Test
    public void doNotQuitWhenEffectiveSplitSetInvalid() {
        // Two leaders with overlapping boundaries -> invalid split set
        String leaderStore = "leaderStore";
        KVRangeId effId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeId effId2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        Boundary b1 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("n")).build();
        Boundary b2 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("m")) // overlaps with b1
            .setEndKey(ByteString.copyFromUtf8("z")).build();
        KVRangeDescriptor effRange1 = KVRangeDescriptor.newBuilder()
            .setId(effId1).setVer(1).setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setBoundary(b1)
            .setConfig(ClusterConfig.newBuilder().addVoters(leaderStore).build())
            .build();
        KVRangeDescriptor effRange2 = KVRangeDescriptor.newBuilder()
            .setId(effId2).setVer(1).setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setBoundary(b2)
            .setConfig(ClusterConfig.newBuilder().addVoters(leaderStore).build())
            .build();
        KVRangeStoreDescriptor leaderDesc = KVRangeStoreDescriptor.newBuilder()
            .setId(leaderStore)
            .addRanges(effRange1)
            .addRanges(effRange2)
            .build();

        // Local zombie candidate not present in effective map
        KVRangeId zombieId = KVRangeId.newBuilder().setEpoch(1).setId(99).build();
        KVRangeDescriptor zombieCandidate = KVRangeDescriptor.newBuilder()
            .setId(zombieId).setVer(1).setRole(RaftNodeStatus.Candidate)
            .setState(State.StateType.Normal)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters("other").build())
            .build();
        KVRangeStoreDescriptor localDesc = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(zombieCandidate)
            .build();

        balancer.update(Set.of(leaderDesc, localDesc));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void doNotQuitNonCandidate() {
        // Effective leader forming valid split set
        String leaderStore = "leaderStore";
        KVRangeId effId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        Boundary b = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("z")).build();
        KVRangeDescriptor effLeader = KVRangeDescriptor.newBuilder()
            .setId(effId).setVer(1).setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setBoundary(b)
            .setConfig(ClusterConfig.newBuilder().addVoters(leaderStore).build())
            .build();
        KVRangeStoreDescriptor leaderDesc = KVRangeStoreDescriptor.newBuilder()
            .setId(leaderStore)
            .addRanges(effLeader)
            .build();

        // Local follower with an id not in effective map shouldn't be treated as zombie
        KVRangeId followerId = KVRangeId.newBuilder().setEpoch(1).setId(99).build();
        KVRangeDescriptor localFollower = KVRangeDescriptor.newBuilder()
            .setId(followerId).setVer(1).setRole(RaftNodeStatus.Follower)
            .setState(State.StateType.Normal)
            .setBoundary(b)
            .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).build())
            .build();
        KVRangeStoreDescriptor localDesc = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(localFollower)
            .build();

        balancer.update(Set.of(leaderDesc, localDesc));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.NoNeedBalance);
    }
}
