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

package org.apache.bifromq.basekv.raft.functest;


import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.raft.exception.ClusterConfigChangeException;
import org.apache.bifromq.basekv.raft.functest.annotation.Cluster;
import org.apache.bifromq.basekv.raft.functest.annotation.Config;
import org.apache.bifromq.basekv.raft.functest.annotation.Ticker;
import org.apache.bifromq.basekv.raft.functest.template.SharedRaftConfigTestTemplate;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.raft.proto.RaftNodeSyncState;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class ChangeClusterConfigTest extends SharedRaftConfigTestTemplate {
    @Cluster(v = "V1,V2")
    @Test(groups = "integration")
    public void testChangeToDisjointVoterSet() {
        String leader = group.currentLeader().get();
        group.addRaftNode("V3", 0, 0, ClusterConfig.newBuilder().build(), raftConfig());
        group.connect("V3");
        Set<String> newVoters = new HashSet<>() {{
            add("V3");
        }};
        CompletableFuture<Void> done = group.changeClusterConfig(leader, "cId", newVoters, Collections.emptySet());
        await().until(done::isDone);
        await().until(() ->
            group.latestClusterConfig(leader).getVotersList().size() == 1 &&
                group.latestClusterConfig(leader).getNextVotersList().isEmpty() &&
                group.latestClusterConfig(leader).equals(group.latestClusterConfig("V3")) &&
                group.latestClusterConfig("V2").equals(group.latestClusterConfig("V3")));
        log.info("V1 Config: {}", group.latestClusterConfig(leader));
        log.info("V2 Config: {}", group.latestClusterConfig("V2"));
        log.info("V3 Config: {}", group.latestClusterConfig("V3"));
    }

    @Test(groups = "integration")
    public void testChangeClusterConfigByFollower() {
        Set<String> newVoters = new HashSet<>(Arrays.asList("V1", "V2", "V3", "V4"));
        String follower = group.currentFollowers().get(0);
        try {
            group.changeClusterConfig(follower, newVoters, Collections.emptySet()).join();
        } catch (Throwable e) {
            assertEquals(e.getCause().getClass(), ClusterConfigChangeException.NotLeaderException.class);
        }
    }

    @Test(groups = "integration")
    public void testAddSingleVoter() {
        String leader = group.currentLeader().get();

        group.addRaftNode("V4", 0, 0, ClusterConfig.newBuilder().addVoters("V4").build(), raftConfig());
        group.connect("V4");

        Set<String> newVoters = new HashSet<>() {{
            add("V1");
            add("V2");
            add("V3");
            add("V4");
        }};
        CompletableFuture<Void> done = group.changeClusterConfig(leader, "cId", newVoters, Collections.emptySet());
        assertFalse(done.isDone());

        List<RaftNodeSyncState> leaderStatusLog = Arrays.asList(RaftNodeSyncState.Replicating);
        List<RaftNodeSyncState> nonLeaderStatusLog = Arrays.asList(RaftNodeSyncState.Probing,
            RaftNodeSyncState.Replicating);
        for (String peerId : newVoters) {
            assertTrue(group.awaitIndexCommitted(peerId, 3));
            if (peerId.equals(leader)) {
                assertEquals(group.syncStateLogs(peerId), leaderStatusLog);
            } else {
                assertEquals(group.syncStateLogs(peerId), nonLeaderStatusLog);
            }
        }
        assertTrue(done.isDone());
        assertEquals(group.latestClusterConfig("V4"), group.latestClusterConfig(group.currentLeader().get()));
        assertEquals("cId", group.latestClusterConfig(group.currentLeader().get()).getCorrelateId());
    }

    @Cluster(v = "V1")
    @Test(groups = "integration")
    public void testAddSingleVoterAfterLeaderCompact() {
        String leader = group.currentLeader().get();
        group.compact("V1", ByteString.EMPTY, 1).join();

        group.propose("V1", ByteString.copyFromUtf8("Value1")).join();
        group.awaitIndexCommitted("V1", 2);

        group.addRaftNode("V4", 0, 0, ClusterConfig.newBuilder().addVoters("V4").build(), raftConfig());
        group.connect("V4");

        Set<String> newVoters = new HashSet<>() {{
            add("V1");
            add("V4");
        }};

        CompletableFuture<Void> done = group.changeClusterConfig(leader, "cId", newVoters, Collections.emptySet());
        assertFalse(done.isDone());
        await().until(() -> group.syncStateLogs("V4").contains(RaftNodeSyncState.SnapshotSyncing));
        group.compact("V1", ByteString.EMPTY, 2).join();

        for (String peerId : newVoters) {
            assertTrue(group.awaitIndexCommitted(peerId, 4));
            if (peerId.equals(leader)) {
                assertEquals(group.latestReplicationStatus(peerId), RaftNodeSyncState.Replicating);
            } else {
                assertEquals(group.latestReplicationStatus(peerId), RaftNodeSyncState.Replicating);
            }
        }

        assertTrue(done.isDone());
        assertEquals(group.latestClusterConfig("V4"), group.latestClusterConfig(group.currentLeader().get()));
        assertEquals("cId", group.latestClusterConfig(group.currentLeader().get()).getCorrelateId());
    }

    @Test(groups = "integration")
    public void testAddMultipleVoters() {
        String leader = group.currentLeader().get();

        group.addRaftNode("V4", 0, 0, ClusterConfig.newBuilder().addVoters("V4").build(), raftConfig());
        group.connect("V4");
        group.addRaftNode("V5", 0, 0, ClusterConfig.newBuilder().addVoters("V5").build(), raftConfig());
        group.connect("V5");

        Set<String> newVoters = new HashSet<>() {{
            add("V1");
            add("V2");
            add("V3");
            add("V4");
            add("V5");
        }};
        CompletableFuture<Void> done = group.changeClusterConfig(leader, newVoters, Collections.emptySet());

        List<RaftNodeSyncState> leaderStatusLog = List.of(RaftNodeSyncState.Replicating);
        List<RaftNodeSyncState> nonLeaderStatusLog =
            Arrays.asList(RaftNodeSyncState.Probing, RaftNodeSyncState.Replicating);
        for (String peerId : newVoters) {
            assertTrue(group.awaitIndexCommitted(peerId, 3));
            if (peerId.equals(leader)) {
                assertEquals(group.syncStateLogs(peerId), leaderStatusLog);
            } else {
                assertEquals(group.syncStateLogs(peerId), nonLeaderStatusLog);
            }
        }

        assertTrue(done.isDone());
        assertEquals(group.latestClusterConfig("V4"), group.latestClusterConfig(group.currentLeader().get()));
        assertEquals(group.latestClusterConfig("V5"), group.latestClusterConfig(group.currentLeader().get()));
    }

    @Test(groups = "integration")
    public void testAddLearners() {
        String leader = group.currentLeader().get();

        group.addRaftNode("L4", 0, 0, ClusterConfig.newBuilder().addLearners("L4").build(), raftConfig());
        group.connect("L4");
        group.addRaftNode("L5", 0, 0, ClusterConfig.newBuilder().addLearners("L5").build(), raftConfig());
        group.connect("L5");

        Set<String> newLearners = new HashSet<>() {{
            add("L4");
            add("L5");
        }};
        log.info("Change cluster config");
        CompletableFuture<Void> done =
            group.changeClusterConfig(leader, new HashSet<>(clusterConfig().getVotersList()), newLearners);
        assertFalse(done.isDone());

        assertTrue(group.awaitIndexCommitted("V1", 3));
        assertTrue(group.awaitIndexCommitted("V2", 3));
        assertTrue(group.awaitIndexCommitted("V3", 3));
        assertTrue(group.awaitIndexCommitted("L4", 3));
        assertTrue(group.awaitIndexCommitted("L5", 3));
        assertTrue(done.isDone());
    }

    @Test(groups = "integration")
    public void testAddMultipleVotersAndLearners() {
        String leader = group.currentLeader().get();

        group.addRaftNode("V4", 0, 0, ClusterConfig.newBuilder().addLearners("V4").build(), raftConfig());
        group.connect("V4");
        group.addRaftNode("V5", 0, 0, ClusterConfig.newBuilder().addLearners("V5").build(), raftConfig());
        group.connect("V5");
        group.addRaftNode("L4", 0, 0, ClusterConfig.newBuilder().addLearners("L4").build(), raftConfig());
        group.connect("L4");
        group.addRaftNode("L5", 0, 0, ClusterConfig.newBuilder().addLearners("L5").build(), raftConfig());
        group.connect("L5");

        Set<String> newVoters = new HashSet<>() {{
            add("V1");
            add("V2");
            add("V3");
            add("V4");
            add("V5");
        }};
        Set<String> newLearners = new HashSet<>() {{
            add("L4");
            add("L5");
        }};
        CompletableFuture<Void> done = group.changeClusterConfig(leader, newVoters, newLearners);
        assertFalse(done.isDone());

        assertTrue(group.awaitIndexCommitted("V1", 3));
        assertTrue(group.awaitIndexCommitted("V2", 3));
        assertTrue(group.awaitIndexCommitted("V3", 3));
        assertTrue(group.awaitIndexCommitted("V4", 3));
        assertTrue(group.awaitIndexCommitted("V5", 3));
        assertTrue(group.awaitIndexCommitted("L4", 3));
        assertTrue(group.awaitIndexCommitted("L5", 3));
        assertTrue(done.isDone());
    }

    @Test(groups = "integration")
    public void testAddFollowersThatNeedsLogCatchingUp() {
        String leader = group.currentLeader().get();

        group.propose(leader, ByteString.copyFromUtf8("appCommand1"));
        group.propose(leader, ByteString.copyFromUtf8("appCommand2"));
        group.propose(leader, ByteString.copyFromUtf8("appCommand3"));
        group.propose(leader, ByteString.copyFromUtf8("appCommand4"));
        group.propose(leader, ByteString.copyFromUtf8("appCommand5"));
        assertTrue(group.awaitIndexCommitted("V1", 6));
        assertTrue(group.awaitIndexCommitted("V2", 6));
        assertTrue(group.awaitIndexCommitted("V3", 6));

        group.addRaftNode("V4", 0, 0, ClusterConfig.newBuilder().addLearners("V4").build(), raftConfig());
        group.connect("V4");
        group.addRaftNode("V5", 0, 0, ClusterConfig.newBuilder().addLearners("V5").build(), raftConfig());
        group.connect("V5");
        group.addRaftNode("L1", 0, 0, ClusterConfig.newBuilder().addLearners("L1").build(), raftConfig());
        group.connect("L1");
        group.addRaftNode("L2", 0, 0, ClusterConfig.newBuilder().addLearners("L2").build(), raftConfig());
        group.connect("L2");

        Set<String> newVoters = new HashSet<>() {{
            add("V1");
            add("V2");
            add("V3");
            add("V4");
            add("V5");
        }};
        Set<String> newLearners = new HashSet<>() {{
            add("L1");
            add("L2");
        }};

        CompletableFuture<Void> done = group.changeClusterConfig(leader, newVoters, newLearners);
        assertFalse(done.isDone());

        List<RaftNodeSyncState> leaderStatusLog = List.of(RaftNodeSyncState.Replicating);
        List<RaftNodeSyncState> nonLeaderStatusLog =
            Arrays.asList(RaftNodeSyncState.Probing, RaftNodeSyncState.Replicating);
        for (String peerId : newVoters) {
            assertTrue(group.awaitIndexCommitted(peerId, 8));
            if (peerId.equals(leader)) {
                assertEquals(group.syncStateLogs(peerId), leaderStatusLog);
            } else {
                assertEquals(group.syncStateLogs(peerId), nonLeaderStatusLog);
            }
        }
        assertTrue(group.awaitIndexCommitted("L1", 8));
        assertEquals(group.syncStateLogs("L1"), nonLeaderStatusLog);
        assertTrue(group.awaitIndexCommitted("L2", 8));
        assertEquals(group.syncStateLogs("L2"), nonLeaderStatusLog);
        assertTrue(done.isDone());
    }

    @Test(groups = "integration")
    public void testAddFollowersThatNeedsSnapshotInstallation() {
        String leader = group.currentLeader().get();

        group.propose(leader, ByteString.copyFromUtf8("appCommand1"));
        group.propose(leader, ByteString.copyFromUtf8("appCommand2"));
        group.propose(leader, ByteString.copyFromUtf8("appCommand3"));
        group.propose(leader, ByteString.copyFromUtf8("appCommand4"));
        group.propose(leader, ByteString.copyFromUtf8("appCommand5"));
        assertTrue(group.awaitIndexCommitted("V1", 6));
        assertTrue(group.awaitIndexCommitted("V2", 6));
        assertTrue(group.awaitIndexCommitted("V3", 6));

        group.compact(leader, ByteString.copyFromUtf8("sppSMSnapshot"), 5).join();

        group.addRaftNode("V4", 0, 0, ClusterConfig.newBuilder().addLearners("V4").build(), raftConfig());
        group.connect("V4");
        group.addRaftNode("V5", 0, 0, ClusterConfig.newBuilder().addLearners("V5").build(), raftConfig());
        group.connect("V5");
        group.addRaftNode("L1", 0, 0, ClusterConfig.newBuilder().addLearners("L1").build(), raftConfig());
        group.connect("L1");
        group.addRaftNode("L2", 0, 0, ClusterConfig.newBuilder().addLearners("L2").build(), raftConfig());
        group.connect("L2");

        Set<String> newVoters = new HashSet<>() {{
            add("V1");
            add("V2");
            add("V3");
            add("V4");
            add("V5");
        }};
        Set<String> newLearners = new HashSet<>() {{
            add("L1");
            add("L2");
        }};

        // change cluster config with new proposals arrived
        CompletableFuture<Void> done = group.changeClusterConfig(leader, newVoters, newLearners);
        assertFalse(done.isDone());
        group.propose(leader, ByteString.copyFromUtf8("appCommand6"));

        List<RaftNodeSyncState> leaderStatusLog = List.of(RaftNodeSyncState.Replicating);
        List<RaftNodeSyncState> nonLeaderStatusLog =
            Arrays.asList(RaftNodeSyncState.Probing, RaftNodeSyncState.Replicating);

        Set<String> oldVoters = new HashSet<>() {{
            add("V1");
            add("V2");
            add("V3");
        }};
        for (String peerId : oldVoters) {
            assertTrue(group.awaitIndexCommitted(peerId, 9));
            if (peerId.equals(leader)) {
                assertEquals(group.syncStateLogs(peerId), leaderStatusLog);
            } else {
                assertEquals(group.syncStateLogs(peerId), nonLeaderStatusLog);
            }
        }

        assertTrue(group.awaitIndexCommitted("V4", 9));
        assertEquals(group.syncStateLogs("V4"),
            Arrays.asList(RaftNodeSyncState.Probing, RaftNodeSyncState.SnapshotSyncing,
                RaftNodeSyncState.Replicating));
        assertTrue(group.awaitIndexCommitted("V5", 9));
        assertEquals(group.syncStateLogs("V5"),
            Arrays.asList(RaftNodeSyncState.Probing, RaftNodeSyncState.SnapshotSyncing,
                RaftNodeSyncState.Replicating));
        assertTrue(group.awaitIndexCommitted("L1", 9));
        assertEquals(group.syncStateLogs("L1"),
            Arrays.asList(RaftNodeSyncState.Probing, RaftNodeSyncState.SnapshotSyncing,
                RaftNodeSyncState.Replicating));
        assertTrue(group.awaitIndexCommitted("L2", 9));
        assertEquals(group.syncStateLogs("L2"),
            Arrays.asList(RaftNodeSyncState.Probing, RaftNodeSyncState.SnapshotSyncing,
                RaftNodeSyncState.Replicating));
        assertTrue(done.isDone());
    }

    @Test(groups = "integration")
    public void testRemoveSingleFollower() {
        String leader = group.currentLeader().get();
        String toRemovedFollower = group.currentFollowers().get(0);
        String normalFollower = group.currentFollowers().get(1);
        log.info("To be removed: {}", toRemovedFollower);

        Set<String> newVoters = new HashSet<>(clusterConfig().getVotersList());
        newVoters.remove(toRemovedFollower);

        CompletableFuture<Void> done = group.changeClusterConfig(leader, newVoters, Collections.emptySet());
        assertFalse(done.isDone());

        assertTrue(group.awaitIndexCommitted(leader, 3));
        assertEquals(group.syncStateLogs(leader), List.of(RaftNodeSyncState.Replicating));
        assertTrue(group.awaitIndexCommitted(normalFollower, 3));
        assertEquals(group.syncStateLogs(normalFollower),
            Arrays.asList(RaftNodeSyncState.Probing, RaftNodeSyncState.Replicating));
        // the removed follower will receive new JointConfigEntry but it will not be committed
        assertTrue(group.awaitIndexCommitted(toRemovedFollower, 3));
        // stop tracking as well
        assertEquals(group.syncStateLogs(toRemovedFollower),
            Arrays.asList(RaftNodeSyncState.Probing, RaftNodeSyncState.Replicating, null));

        assertTrue(newVoters.containsAll(group.latestClusterConfig(toRemovedFollower).getVotersList()));
        group.await(ticks(5));
        // the removed should always stay in follower state
        assertEquals(group.nodeState(toRemovedFollower), RaftNodeStatus.Candidate);
        assertTrue(done.isDone());
    }

    @Test(groups = "integration")
    public void testAddPreviousRemovedMember() {
        String leader = group.currentLeader().get();
        String toRemovedFollower = group.currentFollowers().get(0);
        log.info("Removed {}", toRemovedFollower);

        Set<String> newVoters = new HashSet<>(clusterConfig().getVotersList());
        newVoters.remove(toRemovedFollower);

        group.changeClusterConfig(leader, newVoters, Collections.emptySet());
        await().until(() -> group.nodeState(toRemovedFollower) == RaftNodeStatus.Candidate);
        await().until(() -> !group.latestClusterConfig(leader).getVotersList().contains(toRemovedFollower));

        group.await(ticks(2));

        log.info("Add {}", toRemovedFollower);
        newVoters.add(toRemovedFollower);
        group.changeClusterConfig(leader, newVoters, Collections.emptySet());
        await().until(() -> group.latestClusterConfig(leader).getVotersList().contains(toRemovedFollower));

        group.await(ticks(2));
    }

    @Config(preVote = false)
    @Test(groups = "integration")
    public void testAddPreviousRemovedMember1() {
        testAddPreviousRemovedMember();
    }

    @Test(groups = "integration")
    public void testRemoveIsolatedMemberAndAddBack() {
        String leader = group.currentLeader().get();
        String toRemovedFollower = group.currentFollowers().get(0);
        log.info("Removed {}", toRemovedFollower);

        Set<String> newVoters = new HashSet<>(clusterConfig().getVotersList());
        newVoters.remove(toRemovedFollower);
        group.isolate(toRemovedFollower);

        group.changeClusterConfig(leader, newVoters, Collections.emptySet());
        await().until(() -> group.nodeState(toRemovedFollower) == RaftNodeStatus.Candidate);
        await().until(() -> !group.latestClusterConfig(leader).getVotersList().contains(toRemovedFollower));
        group.propose(leader, ByteString.EMPTY);

        group.await(ticks(2));
        group.integrate(toRemovedFollower);

        log.info("Add {}", toRemovedFollower);
        newVoters.add(toRemovedFollower);
        group.changeClusterConfig(leader, newVoters, Collections.emptySet());
        await().until(() -> group.latestClusterConfig(leader).getVotersList().contains(toRemovedFollower));

        group.await(ticks(2));
    }

    @Config(preVote = false)
    @Test(groups = "integration")
    public void testRemoveIsolatedMemberAndAddBack1() {
        testRemoveIsolatedMemberAndAddBack();
    }


    @Cluster(v = "V1,V2,V3,V4,V5", l = "L1,L2")
    @Test(groups = "integration")
    public void testRemoveFollowersAndLearners() {
        String leader = group.currentLeader().get();

        assertEquals(group.currentFollowers().size(), 4);
        assertEquals(group.currentLearners().size(), 2);

        String toRemovedVoter1 = group.currentFollowers().get(0);
        String toRemovedVoter2 = group.currentFollowers().get(1);
        String toRemovedLearner1 = group.currentLearners().get(0);
        String toRemovedLearner2 = group.currentLearners().get(1);
        String normalFollower1 = group.currentFollowers().get(2);
        String normalFollower2 = group.currentFollowers().get(3);
        log.info("To be removed : {}, {}, {}, {}", toRemovedVoter1, toRemovedVoter2, toRemovedLearner1,
            toRemovedLearner2);

        Set<String> newVoters = new HashSet<>(clusterConfig().getVotersList());
        newVoters.remove(toRemovedVoter1);
        newVoters.remove(toRemovedVoter2);
        group.changeClusterConfig(leader, newVoters, Collections.emptySet());
        log.info("New config submitted: newVoters={}, newLearners={}", newVoters, Collections.emptySet());
        assertTrue(group.awaitIndexCommitted(leader, 3));
        assertTrue(group.awaitIndexCommitted(normalFollower1, 3));
        assertTrue(group.awaitIndexCommitted(normalFollower2, 3));
        assertTrue(group.awaitIndexCommitted(toRemovedVoter1, 3));
        assertEquals(group.syncStateLogs(toRemovedVoter1),
            Arrays.asList(RaftNodeSyncState.Probing, RaftNodeSyncState.Replicating, null));
        assertTrue(group.awaitIndexCommitted(toRemovedVoter2, 3));
        assertEquals(group.syncStateLogs(toRemovedVoter2),
            Arrays.asList(RaftNodeSyncState.Probing, RaftNodeSyncState.Replicating, null));
        assertTrue(group.awaitIndexCommitted("L1", 3));
        assertEquals(group.syncStateLogs("L1"),
            Arrays.asList(RaftNodeSyncState.Probing, RaftNodeSyncState.Replicating, null));
        assertTrue(group.awaitIndexCommitted("L2", 3));
        assertEquals(group.syncStateLogs("L2"),
            Arrays.asList(RaftNodeSyncState.Probing, RaftNodeSyncState.Replicating, null));
    }

    @Test(groups = "integration")
    public void testRemoveLeader() {
        String leader = group.currentLeader().get();
        Set<String> newVoters = new HashSet<>(clusterConfig().getVotersList());

        // remove leader self
        newVoters.remove(leader);
        log.info("Change cluster config to voters[{}]", newVoters);
        CompletableFuture<Void> done = group.changeClusterConfig(leader, newVoters, Collections.emptySet());
        done.join();
        assertTrue(done.isDone() && !done.isCompletedExceptionally());
        await().until(() -> group.currentLeader().isPresent() && !leader.equals(group.currentLeader().get()));
        assertEquals(group.syncStateLogs(leader), Arrays.asList(RaftNodeSyncState.Replicating, null));
        assertTrue(
            RaftNodeStatus.Follower == group.nodeState(leader) || RaftNodeStatus.Candidate == group.nodeState(leader));
    }

    @Test(groups = "integration")
    public void TestChangeConfigWhenLeaderStepDown() {
        String leader = group.currentLeader().get();
        String follower = group.currentFollowers().get(0);
        log.info("Isolate leader {}", leader);
        group.isolate(leader);

        Set<String> newVoters = new HashSet<>(clusterConfig().getVotersList());
        log.info("Remove follower {}", follower);
        newVoters.remove(follower);
        group.changeClusterConfig(leader, newVoters, Collections.emptySet())
            .handle((r, e) -> {
                assertSame(e.getClass(), ClusterConfigChangeException.LeaderStepDownException.class);
                return CompletableFuture.completedFuture(null);
            }).join();
    }

    @Cluster(v = "")
    @Ticker(disable = true)
    @Test(groups = "integration")
    public void testStartNodeWithEmptyConfig() {
        group.run(10, TimeUnit.MILLISECONDS);
        group.addRaftNode("V1", 0, 0, ClusterConfig.getDefaultInstance(), raftConfig());
        group.connect("V1");
        group.await(ticks(5));
        assertEquals(group.nodeState("V1"), RaftNodeStatus.Candidate);
    }

    @Cluster(v = "")
    @Ticker(disable = true)
    @Test(groups = "integration")
    public void testStartNodeWithDisjointConfig() {
        group.run(10, TimeUnit.MILLISECONDS);
        // add a node with the voters excluding itself
        group.addRaftNode("V1", 0, 0, ClusterConfig.newBuilder()
            .addVoters("V2")
            .addVoters("V3")
            .build(), raftConfig());
        group.connect("V1");
        group.await(ticks(5));
        // the node should be un-promotable
        assertEquals(group.nodeState("V1"), RaftNodeStatus.Candidate);
    }

    @Cluster(v = "V1")
    @Test(groups = "integration")
    public void testAddVotersInitWithEmptyConfig() {
        group.addRaftNode("V2", 0, 0, ClusterConfig.getDefaultInstance(), raftConfig());
        // V2 should stay in follower state
        group.connect("V2");
        group.await(ticks(5));
        assertEquals(group.nodeState("V2"), RaftNodeStatus.Candidate);

        String leader = group.currentLeader().get();
        log.info("Change cluster config to voters=[V1,V2]");
        group.changeClusterConfig(leader, new HashSet<>(Arrays.asList("V1", "V2")), Collections.emptySet()).join();

        assertTrue(group.awaitIndexCommitted("V1", 3));
        assertTrue(group.awaitIndexCommitted("V2", 3));
        assertEquals(group.latestClusterConfig("V2").getVotersCount(), 2);
    }
}
