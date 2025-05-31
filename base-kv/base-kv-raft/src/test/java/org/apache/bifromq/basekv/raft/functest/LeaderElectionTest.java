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

package org.apache.bifromq.basekv.raft.functest;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.raft.event.ElectionEvent;
import org.apache.bifromq.basekv.raft.exception.DropProposalException;
import org.apache.bifromq.basekv.raft.exception.LeaderTransferException;
import org.apache.bifromq.basekv.raft.functest.annotation.Cluster;
import org.apache.bifromq.basekv.raft.functest.annotation.Config;
import org.apache.bifromq.basekv.raft.functest.annotation.Ticker;
import org.apache.bifromq.basekv.raft.functest.template.SharedRaftConfigTestTemplate;
import org.apache.bifromq.basekv.raft.proto.RaftMessage;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class LeaderElectionTest extends SharedRaftConfigTestTemplate {

    @Test(groups = "integration")
    public void testNonJointElectionWithPreVote() {
        group.pause();
        assertEquals(group.latestClusterConfig("V1"), clusterConfig());
        assertEquals(group.latestClusterConfig("V2"), clusterConfig());
        assertEquals(group.latestClusterConfig("V3"), clusterConfig());
        assertEquals(group.latestSnapshot("V1").size(), 0);
        assertEquals(group.latestSnapshot("V2").size(), 0);
        assertEquals(group.latestSnapshot("V3").size(), 0);
        assertTrue(group.currentLeader().isPresent());
        assertTrue(group.currentCandidates().isEmpty());
        assertEquals(group.currentFollowers().size(), 2);
        // leader elected and at least one node got notified
        assertTrue(group.electionLog("V1").size() == 1
            || group.electionLog("V2").size() == 1
            || group.electionLog("V3").size() == 1);
        Set<ElectionEvent> elected = new HashSet<>();
        elected.addAll(group.electionLog("V1"));
        elected.addAll(group.electionLog("V2"));
        elected.addAll(group.electionLog("V3"));
        // only one leader elected
        assertEquals(elected.size(), 3);
    }

    @Test(groups = "integration")
    public void testNonJointElectionWithoutPreVote() {
        group.pause();
        assertTrue(group.currentLeader().isPresent());
        assertTrue(group.currentCandidates().isEmpty());
        assertEquals(group.currentFollowers().size(), 2);
        // leader elected and at least one node got notified
        assertTrue(group.electionLog("V1").size() == 1
            || group.electionLog("V2").size() == 1
            || group.electionLog("V3").size() == 1);
        Set<ElectionEvent> elected = new HashSet<>();
        elected.addAll(group.electionLog("V1"));
        elected.addAll(group.electionLog("V2"));
        elected.addAll(group.electionLog("V3"));
        // only one leader elected
        assertEquals(elected.size(), 3);
    }

    @Cluster(v = "V1")
    @Test(groups = "integration")
    public void testSingleNodeElectionWithPreVote() {
    }

    @Cluster(v = "V1")
    @Config(preVote = false)
    @Test(groups = "integration")
    public void testSingleNodeElectionWithoutPreVote() {
    }

    @Test(groups = "integration")
    public void testLeaderElectionWithOnlyMajorityFollowersVoted() {
        // isolate Leader
        String leader = group.currentLeader().get();
        group.isolate(leader);
        // enough ticks for leader election
        assertTrue(group.awaitIndexCommitted(leader, 1));
        await().until(() -> group.currentLeader().isPresent() && !group.currentLeader().get().equals(leader));
    }

    @Test(groups = "integration")
    public void testLeaderStepDownBeforeAuthorityEstablished() {
        String leader = group.currentLeader().get();

        String follower1 = group.currentFollowers().get(0);
        String follower2 = group.currentFollowers().get(1);
        group.cut(leader, follower1);

        group.propose(leader, ByteString.copyFromUtf8("appCommand"));
        assertTrue(group.awaitIndexCommitted(follower2, 2));

        group.isolate(leader);
        await().until(() -> group.currentLeader().isPresent() && leader.equals(group.currentLeader().get()));
        assertTrue(group.currentLeader().isPresent());
    }

    @Test(groups = "integration")
    public void testLeaderStepDownWhenQuorumActive() {
        // FIXME: @luyong01, what is the expected logic?

        String leader = group.currentLeader().get();
        // wait enough time
        group.await(ticks(5));
        assertEquals(group.nodeState(leader), RaftNodeStatus.Leader);
    }

    @Test(groups = "integration")
    public void testLeaderStepDownWhenQuorumLost() {
        // enough ticks for leader election
        String leader = group.currentLeader().get();

        // isolate leader
        group.isolate(leader);
        group.waitForNextElection();
        group.await(ticks(5));
        assertNotEquals(group.nodeState(leader), RaftNodeStatus.Leader);
    }

    @Test(groups = "integration")
    public void testTransferLeadershipWithoutPreVote() {
        String leader = group.currentLeader().get();

        assertTrue(group.awaitIndexCommitted(leader, 1));

        String transferee = group.currentFollowers().get(0);
        log.info("Transfer leadership to {} from {}", transferee, leader);
        CompletableFuture<Void> done = group.transferLeadership(leader, transferee);
        done.join();
        assertTrue(done.isDone() && !done.isCompletedExceptionally());
        group.waitForNextElection();

        assertTrue(group.currentLeader().isPresent());
        assertEquals(group.currentLeader().get(), transferee);
    }

    @Cluster(l = "L1,L2,L3")
    @Test(groups = "integration")
    public void testLearnerElectionTimeout() {
        group.isolate("V1");
        group.isolate("V2");

        group.await(ticks(5));
        assertFalse(group.currentLeader().isPresent());
    }

    @Cluster(l = "L1,L2,L3,L4,L5")
    @Test(groups = "integration")
    public void testLearnerPromotion() {
        String leader = group.currentLeader().get();

        Set<String> newVoters = new HashSet<String>() {{
            add("L1");
            add("L2");
            add("L3");
            add("L4");
            add("L5");
        }};
        group.changeClusterConfig(leader, newVoters, Collections.emptySet());
        group.waitForNextElection();
        assertTrue(newVoters.contains(group.currentLeader().get()));
    }

    @Test(groups = "integration")
    public void testLeaderElectionOverwriteNewerLogsWithPreVote() {
        testLeaderElectionOverwriteNewerLogs();
    }

    @Config(preVote = false)
    @Test(groups = "integration")
    public void testLeaderElectionOverwriteNewerLogsWithoutPreVote() {
        testLeaderElectionOverwriteNewerLogs();
    }

    private void testLeaderElectionOverwriteNewerLogs() {
        String leader = group.currentLeader().get();

        String follower1 = group.currentFollowers().get(0);
        String follower2 = group.currentFollowers().get(1);
        group.isolate(follower1);
        // log entries won't be committed
        group.ignore(follower1, leader, RaftMessage.MessageTypeCase.APPENDENTRIESREPLY);
        group.ignore(follower2, leader, RaftMessage.MessageTypeCase.APPENDENTRIESREPLY);
        group.propose(leader, ByteString.copyFromUtf8("appCommand1"));
        group.propose(leader, ByteString.copyFromUtf8("appCommand2"));

        group.recoverNetwork();
        group.isolate(follower2);
        group.ignore(follower1, leader, RaftMessage.MessageTypeCase.APPENDENTRIESREPLY);
        group.ignore(follower2, leader, RaftMessage.MessageTypeCase.APPENDENTRIESREPLY);
        group.propose(leader, ByteString.copyFromUtf8("appCommand3"));

        group.recoverNetwork();
        group.isolate(leader);
        group.waitForNextElection();
        leader = group.currentLeader().get();
        log.info("New leader: {}", leader);
        assertTrue(leader.equals(follower1) || leader.equals(follower2));
    }

    @Test(groups = "integration")
    public void testTransferLeadershipWithPreVote() {
        String leader = group.currentLeader().get();

        String transferee = group.currentFollowers().get(0);
        String cId = group.latestClusterConfig(leader).getCorrelateId();
        CompletableFuture<Void> done = group.transferLeadership(leader, transferee);
        done.join();
        assertTrue(done.isDone() && !done.isCompletedExceptionally());
        group.waitForNextElection();

        assertTrue(group.currentLeader().isPresent());
        assertEquals(group.currentLeader().get(), transferee);
        assertEquals(group.latestClusterConfig(group.currentLeader().get()).getCorrelateId(), cId);
    }

    @Ticker(disable = true)
    @Test(groups = "integration")
    public void testTransferLeadershipImmediatelyAfterLeaderElected() {
        group.run(10, TimeUnit.MILLISECONDS);
        group.waitForNextElection();
        String leader = group.currentLeader().get();
        String follower = group.currentFollowers().get(0);
        try {
            log.info("Transfer leader to {}", follower);
            group.transferLeadership(leader, follower).join();
        } catch (Throwable e) {
            assertSame(e.getCause().getClass(), LeaderTransferException.LeaderNotReadyException.class);
        }
    }

    @Test(groups = "integration")
    public void testTransferLeadershipToUpToDateNode() {
        String leader = group.currentLeader().get();

        group.propose(leader, ByteString.copyFromUtf8("appCommand"));
        assertTrue(group.awaitIndexCommitted("V1", 2));
        assertTrue(group.awaitIndexCommitted("V2", 2));
        assertTrue(group.awaitIndexCommitted("V3", 2));

        String transferee = group.currentFollowers().get(0);
        CompletableFuture<Void> done = group.transferLeadership(leader, transferee);
        done.join();
        assertTrue(done.isDone() && !done.isCompletedExceptionally());
        group.waitForNextElection();
        assertTrue(group.currentLeader().isPresent());
        assertEquals(group.currentLeader().get(), transferee);
    }

    @Test(groups = "integration")
    public void testTransferLeadershipToUpToDateNodeFromFollower() {
        String leader = group.currentLeader().get();

        group.propose(leader, ByteString.copyFromUtf8("appCommand"));
        assertTrue(group.awaitIndexCommitted("V1", 2));
        assertTrue(group.awaitIndexCommitted("V2", 2));
        assertTrue(group.awaitIndexCommitted("V3", 2));

        String transferee = group.currentFollowers().get(0);
        String follower = group.currentFollowers().get(1);
        group.transferLeadership(follower, transferee)
            .handle((r, e) -> {
                assertSame(e.getClass(), LeaderTransferException.NotLeaderException.class);
                return CompletableFuture.completedFuture(null);
            }).join();
    }

    @Test(groups = "integration")
    public void testTransferLeadershipToSlowFollower() {
        String leader = group.currentLeader().get();

        String follower = group.currentFollowers().get(0);
        String transferee = group.currentFollowers().get(1);
        log.info("Cut link between leader[{}] and peer[{}]", leader, transferee);
        group.cut(leader, transferee);

        log.info("Propose command1");
        group.propose(leader, ByteString.copyFromUtf8("appCommand1"));
        assertTrue(group.awaitIndexCommitted(leader, 2));
        assertTrue(group.awaitIndexCommitted(follower, 2));
        log.info("Propose command2");
        group.propose(leader, ByteString.copyFromUtf8("appCommand2"));
        assertTrue(group.awaitIndexCommitted(leader, 3));
        assertTrue(group.awaitIndexCommitted(follower, 3));

        log.info("Recover network");
        group.recoverNetwork();
        log.info("Transfer leadership from leader[{}] to peer[{}]", leader, transferee);
        group.transferLeadership(leader, transferee);
        group.waitForNextElection();
        assertTrue(group.currentLeader().isPresent());
        assertEquals(group.currentLeader().get(), transferee);
    }

    @Test(groups = "integration")
    public void testLeaderTransferAfterSnapshot() {
        String leader = group.currentLeader().get();

        String follower = group.currentFollowers().get(0);
        String transferee = group.currentFollowers().get(1);
        group.cut(leader, transferee);

        group.propose(leader, ByteString.copyFromUtf8("appCommand1"));
        assertTrue(group.awaitIndexCommitted(leader, 2));
        assertTrue(group.awaitIndexCommitted(follower, 2));
        group.propose(leader, ByteString.copyFromUtf8("appCommand2"));
        assertTrue(group.awaitIndexCommitted(leader, 3));
        assertTrue(group.awaitIndexCommitted(follower, 3));

        group.compact(leader, ByteString.copyFromUtf8("appSMSnapshot"), 3).join();

        group.recoverNetwork();
        log.info("Network recovered");
        group.transferLeadership(leader, transferee);
        group.waitForNextElection();
        assertTrue(group.currentLeader().isPresent());
        assertEquals(group.currentLeader().get(), transferee);
    }

    @Test(groups = "integration")
    public void testTransferLeadershipToSelf() {
        String leader = group.currentLeader().get();
        assertTrue(group.awaitIndexCommitted(leader, 1));
        log.info("Leader {} elected", leader);

        group.transferLeadership(leader, leader)
            .handle((r, e) -> {
                assertSame(e.getClass(), LeaderTransferException.SelfTransferException.class);
                return CompletableFuture.completedFuture(null);
            }).join();
    }

    @Test(groups = "integration")
    public void testTransferLeadershipToNonExistingNode() {
        String leader = group.currentLeader().get();
        assertTrue(group.awaitIndexCommitted(leader, 1));
        log.info("Leader {} elected", leader);

        group.transferLeadership(leader, "nonExistingNode")
            .handle((r, e) -> {
                assertSame(e.getClass(), LeaderTransferException.NotFoundOrQualifiedException.class);
                return CompletableFuture.completedFuture(null);
            }).join();
    }

    @Cluster(l = "l1")
    @Test(groups = "integration")
    public void testTransferLeadershipToLearner() {
        String leader = group.currentLeader().get();

        group.transferLeadership(leader, "l1")
            .handle((r, e) -> {
                assertSame(e.getClass(), LeaderTransferException.NotFoundOrQualifiedException.class);
                return CompletableFuture.completedFuture(null);
            }).join();
    }

    @Test(groups = "integration")
    public void testTransferLeadershipTimeout() {
        String leader = group.currentLeader().get();

        assertTrue(group.awaitIndexCommitted(group.currentFollowers().get(0), 1));
        assertTrue(group.awaitIndexCommitted(group.currentFollowers().get(1), 1));

        String transferee = group.currentFollowers().get(0);
        log.info("Cut link between {} and {}", transferee, leader);
        group.cut(leader, transferee);
        log.info("Transfer leadership to {} from {}", transferee, leader);
        group.transferLeadership(leader, transferee)
            .handle((v, e) -> {
                assertSame(e.getClass(), LeaderTransferException.TransferTimeoutException.class);
                return CompletableFuture.completedFuture(null);
            }).join();
        group.await(ticks(5));
        assertEquals(group.currentLeader().get(), leader);
    }

    @Test(groups = "integration")
    public void testTransferLeadershipIgnoreProposal() {
        String leader = group.currentLeader().get();

        String transferee = group.currentFollowers().get(0);
        group.transferLeadership(leader, transferee);

        group.propose(leader, ByteString.copyFromUtf8("appCommand"))
            .handle((r, e) -> {
                assertSame(e.getClass(), DropProposalException.TransferringLeaderException.class);
                return CompletableFuture.completedFuture(null);
            }).join();
    }

    @Config(preVote = false)
    @Test(groups = "integration")
    public void testTransferLeadershipReceiveHigherTermVote() {
        String leader = group.currentLeader().get();

        String follower = group.currentFollowers().get(0);
        String transferee = group.currentFollowers().get(1);
        log.info("Transferee is {}", transferee);
        // transferee is isolated and leader is pending
        group.isolate(transferee);
        group.transferLeadership(leader, transferee);

        group.ignore(leader, follower, RaftMessage.MessageTypeCase.APPENDENTRIES);

        group.waitForNextElection();
        assertTrue(group.currentLeader().isPresent());
        assertEquals(group.currentLeader().get(), follower);
    }

    @Test(groups = "integration")
    public void testTransferLeadershipRemoveNode() {
        String leader = group.currentLeader().get();

        String transferee = group.currentFollowers().get(0);
        group.ignore(leader, transferee, RaftMessage.MessageTypeCase.TIMEOUTNOW);
        log.info("Transferee is {}", transferee);

        Set<String> newVoters = new HashSet<String>(clusterConfig().getVotersList());
        // remove transferee
        log.info("Remove transferee {} from cluster", transferee);
        newVoters.remove(transferee);

        group.transferLeadership(leader, transferee);

        group.changeClusterConfig(leader, newVoters, Collections.emptySet());

        group.await(ticks(10));
        assertEquals(group.currentLeader().get(), leader);
    }

    @Test(groups = "integration")
    public void testTransferLeadershipToDemoteNode() {
        String leader = group.currentLeader().get();

        String transferee = group.currentFollowers().get(0);
        group.ignore(leader, transferee, RaftMessage.MessageTypeCase.TIMEOUTNOW);
        log.info("Transferee is {}", transferee);
        // demote transferee to learner
        Set<String> newVoters = new HashSet<String>() {{
            add("V1");
            add("V2");
            add("V3");
        }};
        newVoters.remove(transferee);

        group.transferLeadership(leader, transferee);

        group.changeClusterConfig(leader, newVoters, Collections.singleton(transferee));

        group.await(ticks(5));
        assertEquals(new HashSet<>(group.latestClusterConfig(leader).getVotersList()), newVoters);
        assertEquals(new HashSet<>(group.latestClusterConfig(leader).getLearnersList()),
            Collections.singleton(transferee));
        assertEquals(group.currentLeader().get(), leader);
    }

    @Test(groups = "integration")
    public void testLeaderTransferWhenOriginLeaderLostVote() {
        String leader = group.currentLeader().get();

        String transferee = group.currentFollowers().get(0);
        group.transferLeadership(leader, transferee);
        log.info("Transferee: {}", transferee);
        group.ignore(transferee, leader, RaftMessage.MessageTypeCase.REQUESTVOTE);
        group.waitForNextElection();
        assertEquals(group.currentLeader().get(), transferee);
    }

    @Config(preVote = false)
    @Test(groups = "integration")
    public void testNodeWithSmallerTermCanCompleteElection() {
        String leader = group.currentLeader().get();

        String behindFollower = group.currentFollowers().get(0);
        String normalFollower = group.currentFollowers().get(1);
        group.isolate(behindFollower);

        group.propose(leader, ByteString.copyFromUtf8("appCommand1"));
        group.propose(leader, ByteString.copyFromUtf8("appCommand2"));
        assertTrue(group.awaitIndexCommitted(leader, 3));
        assertTrue(group.awaitIndexCommitted(normalFollower, 3));

        group.recoverNetwork();
        group.isolate(leader);

        group.waitForNextElection();
        assertTrue(group.currentLeader().isPresent());
        assertEquals(group.currentLeader().get(), normalFollower);
    }

    @Config(preVote = true)
    @Test(groups = "integration")
    public void testNodeWithSmallerTermCanCompleteElectionWithPreVote() {
        this.testNodeWithSmallerTermCanCompleteElection();
    }

    @Config(preVote = false)
    @Test(groups = "integration")
    public void testLeaderStepDown() {
        String leader = group.currentLeader().get();
        assertTrue(group.stepDown(leader));
        assertEquals(group.nodeState(leader), RaftNodeStatus.Follower);
        group.waitForNextElection();
    }

    @Config(preVote = true)
    @Test(groups = "integration")
    public void testLeaderStepDownWithPreVote() {
        this.testLeaderStepDown();
    }
}
