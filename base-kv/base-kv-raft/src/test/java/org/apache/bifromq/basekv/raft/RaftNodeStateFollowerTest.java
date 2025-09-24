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

package org.apache.bifromq.basekv.raft;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bifromq.basekv.raft.event.RaftEvent;
import org.apache.bifromq.basekv.raft.event.RaftEventType;
import org.apache.bifromq.basekv.raft.event.SnapshotRestoredEvent;
import org.apache.bifromq.basekv.raft.proto.AppendEntries;
import org.apache.bifromq.basekv.raft.proto.AppendEntriesReply;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.InstallSnapshot;
import org.apache.bifromq.basekv.raft.proto.InstallSnapshotReply;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.raft.proto.Propose;
import org.apache.bifromq.basekv.raft.proto.ProposeReply;
import org.apache.bifromq.basekv.raft.proto.RaftMessage;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.raft.proto.RequestPreVote;
import org.apache.bifromq.basekv.raft.proto.RequestPreVoteReply;
import org.apache.bifromq.basekv.raft.proto.RequestReadIndex;
import org.apache.bifromq.basekv.raft.proto.RequestReadIndexReply;
import org.apache.bifromq.basekv.raft.proto.RequestVote;
import org.apache.bifromq.basekv.raft.proto.RequestVoteReply;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import org.apache.bifromq.basekv.raft.proto.TimeoutNow;
import org.apache.bifromq.basekv.raft.proto.Voting;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RaftNodeStateFollowerTest extends RaftNodeStateTest {
    private static final String leader = "v1";
    private AutoCloseable closeable;

    @BeforeMethod
    public void openMocks() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterMethod
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void testStartUp() {
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower follower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig, stateStorage,
            msgSender,
            eventListener,
            snapshotInstaller,
            onSnapshotInstalled,
            "cluster", "testCluster", "rangeId", "testRange");
        assertEquals(follower.id, stateStorage.local());
        assertEquals(follower.getState(), RaftNodeStatus.Follower);
        assertEquals(follower.latestClusterConfig(), clusterConfig);
        assertEquals(follower.latestSnapshot(), stateStorage.latestSnapshot().getData());
    }

    @Test
    public void testElectionElapsed() {
        RaftConfig raftConfig = new RaftConfig()
            .setPreVote(true)
            .setDisableForwardProposal(false)
            .setElectionTimeoutTick(6)
            .setHeartbeatTimeoutTick(3)
            .setInstallSnapshotTimeoutTick(5)
            .setMaxInflightAppends(3);
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower raftNodeStateFollower = new RaftNodeStateFollower(1, 0, leader, raftConfig,
            stateStorage, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);

        Integer randomElectionTimeoutTick =
            ReflectionUtils.getField(raftNodeStateFollower, "randomElectionTimeoutTick");
        for (int i = 0; i < randomElectionTimeoutTick - 1; ++i) {
            raftNodeStateFollower.tick();
        }
        RaftNodeState raftNodeState = raftNodeStateFollower.tick();
        assertSame(raftNodeState.getState(), RaftNodeStatus.Candidate);


        // change cluster config to be not promotable
        raftNodeStateFollower = new RaftNodeStateFollower(1, 0, leader, raftConfig,
            stateStorage, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);
        stateStorage.append(Collections.singletonList(LogEntry.newBuilder()
            .setConfig(ClusterConfig.newBuilder().addAllVoters(Arrays.asList("v2", "v3")).build())
            .setTerm(2)
            .setIndex(stateStorage.lastIndex() + 1)
            .build()), false);
        randomElectionTimeoutTick = ReflectionUtils.getField(raftNodeStateFollower, "randomElectionTimeoutTick");
        for (int i = 0; i < randomElectionTimeoutTick - 1; ++i) {
            raftNodeStateFollower.tick();
        }
        raftNodeState = raftNodeStateFollower.tick();
        assertSame(raftNodeState.getState(), RaftNodeStatus.Candidate);
    }

    @Test
    public void testPropose() {
        AtomicInteger onMessageReadyIndex = new AtomicInteger();
        CompletableFuture<Long> onDoneOk = new CompletableFuture<>();
        CompletableFuture<Long> onDoneExceptionally = new CompletableFuture<>();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower follower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig,
            stateStorage, messages -> {
            if (onMessageReadyIndex.get() == 0) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                    put(leader, Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setPropose(Propose.newBuilder()
                            .setId(1)
                            .setCommand(command)
                            .build())
                        .build()));
                }});
            } else if (onMessageReadyIndex.get() == 1) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                    put(leader, Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setPropose(Propose.newBuilder()
                            .setId(2)
                            .setCommand(command)
                            .build())
                        .build()));
                }});
            }
        }, eventListener, snapshotInstaller, onSnapshotInstalled);

        follower.propose(command, onDoneOk);
        assertFalse(onDoneOk.isDone());

        Map<Integer, CompletableFuture<Void>> idToForwardedProposeMap =
            ReflectionUtils.getField(follower, "idToForwardedProposeMap");
        assertTrue(Objects.requireNonNull(idToForwardedProposeMap).containsKey(1));
        LinkedHashMap<Long, Set<CompletableFuture<Void>>> tickToForwardedProposesMap =
            ReflectionUtils.getField(follower, "tickToForwardedProposesMap");
        assertTrue(Objects.requireNonNull(tickToForwardedProposesMap).get(0L).contains(1));

        RaftMessage proposeReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setProposeReply(ProposeReply.newBuilder()
                .setId(1)
                .setCode(ProposeReply.Code.Success)
                .build())
            .build();
        follower.receive(leader, proposeReply);
        assertTrue(idToForwardedProposeMap.get(1).isDone());

        follower.propose(command, onDoneExceptionally);
        assertFalse(onDoneExceptionally.isDone());

        RaftMessage proposeReplyError = RaftMessage.newBuilder()
            .setTerm(1)
            .setProposeReply(ProposeReply.newBuilder()
                .setId(2)
                .setCode(ProposeReply.Code.DropByLeaderTransferring)
                .build())
            .build();
        follower.receive(leader, proposeReplyError);
        assertTrue(idToForwardedProposeMap.get(2).isCompletedExceptionally());

        follower.propose(command, new CompletableFuture<>());
        assertFalse(idToForwardedProposeMap.isEmpty());
        follower.tick();
        follower.tick();
        follower.tick();
        follower.tick();
        follower.tick();
        follower.tick();
        follower.tick();
        assertTrue(idToForwardedProposeMap.isEmpty());
    }

    @Test
    public void testProposeExceptionally() {
        when(raftStateStorage.latestClusterConfig()).thenReturn(clusterConfig);

        RaftNodeStateFollower disableForwardProposalFollower = new RaftNodeStateFollower(1, 0, null,
            new RaftConfig().setDisableForwardProposal(true).setElectionTimeoutTick(3), raftStateStorage,
            msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);
        CompletableFuture<Long> disableForwardProposalOnDone = new CompletableFuture<>();
        disableForwardProposalFollower.propose(command, disableForwardProposalOnDone);
        assertTrue(disableForwardProposalOnDone.isCompletedExceptionally());

        RaftNodeStateFollower noLeaderFollower = new RaftNodeStateFollower(1, 0, null, defaultRaftConfig,
            raftStateStorage, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);
        CompletableFuture<Long> nonLeaderOnDone = new CompletableFuture<>();
        noLeaderFollower.propose(command, nonLeaderOnDone);
        assertTrue(nonLeaderOnDone.isCompletedExceptionally());
    }

    @Test
    public void testTransferLeadership() {
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower follower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig,
            stateStorage, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        follower.transferLeadership("v1", onDone);
        assertTrue(onDone.isCompletedExceptionally());
    }

    @Test
    public void testChangeClusterConfig() {
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower follower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig,
            stateStorage, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        follower.changeClusterConfig("cId", Collections.singleton("v3"), Collections.singleton("l4"), onDone);
        assertTrue(onDone.isCompletedExceptionally());
    }

    @Test
    public void testReadIndex() {
        AtomicInteger onMessageReadyIndex = new AtomicInteger();
        CompletableFuture<Long> onDone = new CompletableFuture<>();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower follower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig,
            stateStorage, messages -> {
            if (onMessageReadyIndex.get() == 0) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                    put(leader, Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setRequestReadIndex(RequestReadIndex.newBuilder()
                            .setId(1)
                            .build())
                        .build()));
                }});
            }
        }, eventListener, snapshotInstaller, onSnapshotInstalled);

        follower.readIndex(onDone);
        assertFalse(onDone.isDone());

        Map<Integer, CompletableFuture<Long>> idToReadRequestMap =
            ReflectionUtils.getField(follower, "idToReadRequestMap");
        assertTrue(Objects.requireNonNull(idToReadRequestMap).containsKey(1));
        LinkedHashMap<Long, Set<CompletableFuture<Long>>> tickToReadRequestsMap =
            ReflectionUtils.getField(follower, "tickToReadRequestsMap");
        assertTrue(Objects.requireNonNull(tickToReadRequestsMap).get(0L).contains(1));

        RaftMessage readIndexReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setRequestReadIndexReply(RequestReadIndexReply.newBuilder()
                .setId(1)
                .build())
            .build();
        follower.receive("v1", readIndexReply);

        follower.readIndex(new CompletableFuture<>());
        assertFalse(tickToReadRequestsMap.isEmpty());
        follower.tick();
        follower.tick();
        follower.tick();
        follower.tick();
        follower.tick();
        follower.tick();
        follower.tick();
        assertTrue(tickToReadRequestsMap.isEmpty());
    }

    @Test
    public void testReadIndexExceptionally() {
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower follower = new RaftNodeStateFollower(1, 0, null, defaultRaftConfig,
            stateStorage, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);

        CompletableFuture<Long> onDone = new CompletableFuture<>();
        follower.readIndex(onDone);
        assertTrue(onDone.isCompletedExceptionally());
    }

    @Test
    public void testReceiveTimeoutNow() {
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower follower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig,
            stateStorage, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);

        // change cluster config to be not promotable
        stateStorage.append(Collections.singletonList(LogEntry.newBuilder()
            .setConfig(ClusterConfig.newBuilder().addAllVoters(Arrays.asList("v2", "v3")).build())
            .setTerm(1)
            .setIndex(stateStorage.lastIndex() + 1)
            .build()), false);

        RaftMessage timeoutNow = RaftMessage.newBuilder()
            .setTerm(1)
            .setTimeoutNow(TimeoutNow.newBuilder().build())
            .build();
        RaftNodeState raftNodeState = follower.receive(leader, timeoutNow);
        assertSame(raftNodeState.getState(), RaftNodeStatus.Follower);

        // change cluster config to be promotable
        stateStorage.append(Collections.singletonList(LogEntry.newBuilder()
            .setConfig(ClusterConfig.newBuilder().addAllVoters(Arrays.asList(local, "v2")).build())
            .setTerm(1)
            .setIndex(stateStorage.lastIndex() + 1)
            .build()), false);

        raftNodeState = follower.receive(leader, timeoutNow);
        assertSame(raftNodeState.getState(), RaftNodeStatus.Candidate);
    }

    @Test
    public void testReceivePreVote() {
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        // not in lease will handle preVote
        RaftNodeStateFollower nonInLeaseFollower = new RaftNodeStateFollower(1, 0, null, defaultRaftConfig,
            stateStorage, messages -> {
            assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                put("v2", Collections.singletonList(RaftMessage.newBuilder()
                    .setTerm(2)
                    .setRequestPreVoteReply(RequestPreVoteReply.newBuilder()
                        .setVoteCouldGranted(true)
                        .build())
                    .build()));
            }});
        }, eventListener, snapshotInstaller, onSnapshotInstalled);
        RaftMessage higherTermPreVote = RaftMessage.newBuilder()
            .setTerm(2)
            .setRequestPreVote(RequestPreVote.newBuilder()
                .setCandidateId(testCandidateId)
                .setLastLogTerm(0)
                .setLastLogIndex(0)
                .build())
            .build();
        RaftNodeState raftNodeState = nonInLeaseFollower.receive("v2", higherTermPreVote);
        assertSame(raftNodeState.getState(), RaftNodeStatus.Follower);

        stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        // inLease && higher term will reject pre-vote
        RaftNodeStateFollower inLeaseFollower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig,
            stateStorage, messages -> assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
            put("v2", Collections.singletonList(RaftMessage.newBuilder()
                .setTerm(2)
                .setRequestPreVoteReply(RequestPreVoteReply.newBuilder()
                    .setVoteCouldGranted(false)
                    .build())
                .build()));
        }}), eventListener, snapshotInstaller, onSnapshotInstalled);
        raftNodeState = inLeaseFollower.receive("v2", higherTermPreVote);
        assertSame(raftNodeState.getState(), RaftNodeStatus.Follower);

        RaftMessage matchedTermPreVote = RaftMessage.newBuilder()
            .setTerm(2)
            .setRequestPreVote(RequestPreVote.newBuilder()
                .setCandidateId(testCandidateId)
                .setLastLogTerm(0)
                .setLastLogIndex(0)
                .build())
            .build();
        // request will be ignored
        raftNodeState = inLeaseFollower.receive("v2", matchedTermPreVote);
        assertSame(raftNodeState.getState(), RaftNodeStatus.Follower);

    }

    @Test
    public void testReceiveRequestVoteFacingDisruption() {
        // higherTerm && !leaderTransfer && inLease() && not a member, prevent from being disrupted
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower inLeaseFollower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig,
            stateStorage, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);
        RaftMessage nonLeaderTransferRequest = RaftMessage.newBuilder()
            .setTerm(2)
            .setRequestVote(RequestVote.newBuilder()
                .setCandidateId(testCandidateId)
                .setLastLogTerm(0)
                .setLastLogIndex(0)
                .build())
            .build();
        inLeaseFollower.receive(testCandidateId, nonLeaderTransferRequest);
        assertEquals(inLeaseFollower.currentTerm(), 1);

        stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        // higherTerm && !leaderTransfer && !inLease()
        RaftNodeStateFollower nonInLeaseFollower = new RaftNodeStateFollower(1, 0, null, defaultRaftConfig,
            stateStorage, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);
        nonInLeaseFollower.receive("v2", nonLeaderTransferRequest);
        assertEquals(nonInLeaseFollower.currentTerm(), 2);

        // higherTerm && leaderTransfer && inLease()
        RaftMessage leaderTransferRequest = RaftMessage.newBuilder()
            .setTerm(3)
            .setRequestVote(RequestVote.newBuilder()
                .setCandidateId(testCandidateId)
                .setLastLogTerm(0)
                .setLastLogIndex(0)
                .setLeaderTransfer(true)
                .build())
            .build();
        inLeaseFollower.receive("v2", leaderTransferRequest);
        assertEquals(inLeaseFollower.currentTerm(), 3);
    }

    @Test
    public void testReceiveRequestVoteWeatherGranted() {
        AtomicInteger onMessageReadyIndex = new AtomicInteger();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower inLeaseFollower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig,
            stateStorage, messages -> {
            if (onMessageReadyIndex.get() == 0 || onMessageReadyIndex.get() == 1 || onMessageReadyIndex.get() == 3) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                    put("v2", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(2)
                        .setRequestVoteReply(RequestVoteReply.newBuilder()
                            .setVoteGranted(true)
                            .build())
                        .build()));
                }});
            } else if (onMessageReadyIndex.get() == 2) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                    put("v2", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(2)
                        .setRequestVoteReply(RequestVoteReply.newBuilder()
                            .setVoteGranted(false)
                            .build())
                        .build()));
                }});
            }
        }, eventListener, snapshotInstaller, onSnapshotInstalled);

        RaftMessage transferVote = RaftMessage.newBuilder()
            .setTerm(2)
            .setRequestVote(RequestVote.newBuilder()
                .setCandidateId("v2")
                .setLastLogTerm(2)
                .setLastLogIndex(0)
                .setLeaderTransfer(true)
                .build())
            .build();

        // inLease && !votingPresent
        inLeaseFollower.receive("v2", transferVote);
        assertTrue(stateStorage.currentVoting().isPresent());

        // inLease && votingPresent && votingTerm != askedTerm
        stateStorage.saveVoting(Voting.newBuilder().setTerm(1).setFor("v1").build());
        inLeaseFollower.receive("v2", transferVote);

        // inLease && votingPresent && votingTerm == askedTerm && votingFor != candidateId
        stateStorage.saveVoting(Voting.newBuilder().setTerm(2).setFor("v1").build());
        inLeaseFollower.receive("v2", transferVote);

        // inLease && votingPresent && votingTerm == askedTerm && votingFor == testCandidateId, repeated vote
        stateStorage.saveVoting(Voting.newBuilder().setTerm(2).setFor("v2").build());
        inLeaseFollower.receive("v2", transferVote);

        onMessageReadyIndex.set(0);
        stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower noInLeaseFollower = new RaftNodeStateFollower(1, 0, null, defaultRaftConfig,
            stateStorage, messages -> {
            if (onMessageReadyIndex.get() == 0 || onMessageReadyIndex.get() == 1 || onMessageReadyIndex.get() == 2) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                    put("v2", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(2)
                        .setRequestVoteReply(RequestVoteReply.newBuilder()
                            .setVoteGranted(true)
                            .build())
                        .build()));
                }});
            }
        }, eventListener, snapshotInstaller, onSnapshotInstalled);

        // !inLease && !votingPresent
        noInLeaseFollower.receive("v2", transferVote);
        assertTrue(stateStorage.currentVoting().isPresent());

        // !inLease && votingPresent && votingTerm != currentTerm
        stateStorage.saveVoting(Voting.newBuilder().setTerm(1).setFor("v2").build());
        noInLeaseFollower.receive("v2", transferVote);

        // !inLease && votingPresent && votingTerm == currentTerm
        stateStorage.saveVoting(Voting.newBuilder().setTerm(2).setFor("v2").build());
        noInLeaseFollower.receive("v2", transferVote);
    }

    @Test
    public void testReceiveRequestVoteUptoDate() {
        AtomicInteger onMessageReadyIndex = new AtomicInteger();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower noInLeaseFollower = new RaftNodeStateFollower(1, 0, null, defaultRaftConfig,
            stateStorage, messages -> {
            if (onMessageReadyIndex.get() == 0) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                    put("v2", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setRequestVoteReply(RequestVoteReply.newBuilder()
                            .setVoteGranted(false)
                            .build())
                        .build()));
                }});
            } else if (onMessageReadyIndex.get() == 1) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                    put("v2", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setRequestVoteReply(RequestVoteReply.newBuilder()
                            .setVoteGranted(true)
                            .build())
                        .build()));
                }});

            } else if (onMessageReadyIndex.get() == 2) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                    put("v2", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(2)
                        .setRequestVoteReply(RequestVoteReply.newBuilder()
                            .setVoteGranted(true)
                            .build())
                        .build()));
                }});
            }
        }, eventListener, snapshotInstaller, onSnapshotInstalled);

        // requestLastLogTerm == localLastLogTerm && requestLastLogIndex < localLastLogIndex
        stateStorage.append(Collections.singletonList(LogEntry.newBuilder()
            .setTerm(1)
            .setIndex(stateStorage.lastIndex() + 1)
            .setData(ByteString.EMPTY)
            .build()), false);
        RaftMessage vote = RaftMessage.newBuilder()
            .setTerm(1)
            .setRequestVote(RequestVote.newBuilder()
                .setCandidateId("v2")
                .setLastLogTerm(1)
                .setLastLogIndex(0)
                .build())
            .build();
        noInLeaseFollower.receive("v2", vote);

        // requestLastLogTerm == localLastLogTerm && requestLastLogIndex > localLastLogIndex
        vote = RaftMessage.newBuilder()
            .setTerm(1)
            .setRequestVote(RequestVote.newBuilder()
                .setCandidateId("v2")
                .setLastLogTerm(1)
                .setLastLogIndex(3)
                .build())
            .build();
        noInLeaseFollower.receive("v2", vote);

        // requestLastLogTerm > localLastLogTerm
        vote = RaftMessage.newBuilder()
            .setTerm(2)
            .setRequestVote(RequestVote.newBuilder()
                .setCandidateId("v2")
                .setLastLogTerm(2)
                .setLastLogIndex(0)
                .build())
            .build();
        noInLeaseFollower.receive("v2", vote);
    }

    @Test
    public void testReceiveObsoleteSnapshot() {
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(clusterConfig)
            .setIndex(5)
            .setTerm(1)
            .build();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", snapshot);

        RaftNodeStateFollower noInLeaseFollower = new RaftNodeStateFollower(1, 0, null,
            defaultRaftConfig, stateStorage, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);

        // obsolete or duplicated snapshot
        RaftMessage installSnapshot = RaftMessage.newBuilder()
            .setTerm(1)
            .setInstallSnapshot(InstallSnapshot.newBuilder()
                .setLeaderId("newLeader")
                .setSnapshot(Snapshot.newBuilder()
                    .setTerm(0)
                    .setIndex(0)
                    .setData(ByteString.copyFrom("snapshot".getBytes()))
                    .build())
                .build())
            .build();

        noInLeaseFollower.receive("newLeader", installSnapshot);
        verify(msgSender, times(0)).send(anyMap());
        verify(eventListener, times(0)).onEvent(any());
        verify(snapshotInstaller, times(0)).install(any(), any(), any());
    }

    @Test
    public void testIgnoreSnapshotWhenCommittedProgressNotBehind() {
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(clusterConfig)
            .setIndex(2)
            .setTerm(2)
            .build();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", snapshot);

        stateStorage.append(Collections.singletonList(LogEntry.newBuilder()
            .setTerm(2)
            .setIndex(3)
            .setData(ByteString.copyFromUtf8("entry"))
            .build()), false);

        RaftNodeStateFollower follower = new RaftNodeStateFollower(2, 3, null,
            defaultRaftConfig, stateStorage, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);

        ByteString fsmSnapshot = ByteString.copyFromUtf8("snapshot");
        RaftMessage installSnapshot = RaftMessage.newBuilder()
            .setTerm(2)
            .setInstallSnapshot(InstallSnapshot.newBuilder()
                .setLeaderId("newLeader")
                .setSnapshot(Snapshot.newBuilder()
                    .setTerm(2)
                    .setIndex(2)
                    .setData(fsmSnapshot)
                    .build())
                .build())
            .build();

        follower.receive("newLeader", installSnapshot);

        verify(snapshotInstaller, times(0)).install(any(), any(), any());
    }

    @Test
    public void testInstallSnapshotWhenItAdvancesCommittedProgress() {
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(clusterConfig)
            .setIndex(0)
            .setTerm(1)
            .build();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", snapshot);

        stateStorage.append(Arrays.asList(
            LogEntry.newBuilder().setTerm(2).setIndex(1).setData(ByteString.copyFromUtf8("e1")).build(),
            LogEntry.newBuilder().setTerm(2).setIndex(2).setData(ByteString.copyFromUtf8("e2")).build(),
            LogEntry.newBuilder().setTerm(3).setIndex(3).setData(ByteString.copyFromUtf8("e3")).build(),
            LogEntry.newBuilder().setTerm(3).setIndex(4).setData(ByteString.copyFromUtf8("e4")).build(),
            LogEntry.newBuilder().setTerm(3).setIndex(5).setData(ByteString.copyFromUtf8("e5")).build()), false);

        RaftNodeStateFollower follower = new RaftNodeStateFollower(3, 0, null,
            defaultRaftConfig, stateStorage, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);

        ByteString fsmSnapshot = ByteString.copyFromUtf8("snapshot");
        RaftMessage installSnapshot = RaftMessage.newBuilder()
            .setTerm(3)
            .setInstallSnapshot(InstallSnapshot.newBuilder()
                .setLeaderId("newLeader")
                .setSnapshot(Snapshot.newBuilder()
                    .setTerm(3)
                    .setIndex(4)
                    .setData(fsmSnapshot)
                    .build())
                .build())
            .build();

        follower.receive("newLeader", installSnapshot);

        verify(snapshotInstaller, times(1)).install(eq(fsmSnapshot), eq("newLeader"), any());
    }

    @Test
    public void testStaleSnapshotInstall() {
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(clusterConfig)
            .setIndex(0)
            .setTerm(1)
            .build();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", snapshot);

        RaftNodeStateFollower noInLeaseFollower = new RaftNodeStateFollower(1, 0, null,
            defaultRaftConfig, stateStorage, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled,
            "cluster", "testCluster", "rangeId", "testRange");

        ByteString fsmSnapshot = ByteString.copyFrom("snapshot".getBytes());
        RaftMessage installSnapshot = RaftMessage.newBuilder()
            .setTerm(1)
            .setInstallSnapshot(InstallSnapshot.newBuilder()
                .setLeaderId("newLeader")
                .setSnapshot(Snapshot.newBuilder()
                    .setTerm(1)
                    .setIndex(1)
                    .setData(fsmSnapshot)
                    .build())
                .build())
            .build();

        noInLeaseFollower.receive("newLeader", installSnapshot);
        noInLeaseFollower.receive("newLeader", RaftMessage.newBuilder()
            .setTerm(1)
            .setInstallSnapshot(InstallSnapshot.newBuilder()
                .setLeaderId("newLeader")
                .setSnapshot(Snapshot.newBuilder()
                    .setTerm(0)
                    .setIndex(0)
                    .setData(ByteString.copyFrom("snapshot".getBytes()))
                    .build())
                .build())
            .build());

        noInLeaseFollower.receive("newLeader", RaftMessage.newBuilder()
            .setTerm(1)
            .setInstallSnapshot(InstallSnapshot.newBuilder()
                .setLeaderId("newLeader")
                .setSnapshot(Snapshot.newBuilder()
                    .setTerm(1)
                    .setIndex(0)
                    .setData(ByteString.copyFrom("snapshot".getBytes()))
                    .build())
                .build())
            .build());

        verify(snapshotInstaller, times(1)).install(eq(fsmSnapshot), eq("newLeader"), any());
    }

    @Test
    public void testSnapshotDuplicateInstall() {
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(clusterConfig)
            .setIndex(0)
            .setTerm(1)
            .build();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", snapshot);

        RaftNodeStateFollower noInLeaseFollower = new RaftNodeStateFollower(1, 0, null,
            defaultRaftConfig, stateStorage, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled,
            "cluster", "testCluster", "rangeId", "testRange");

        ByteString fsmSnapshot = ByteString.copyFrom("snapshot".getBytes());
        RaftMessage installSnapshot = RaftMessage.newBuilder()
            .setTerm(1)
            .setInstallSnapshot(InstallSnapshot.newBuilder()
                .setLeaderId("newLeader")
                .setSnapshot(Snapshot.newBuilder()
                    .setTerm(1)
                    .setIndex(0)
                    .setData(fsmSnapshot)
                    .build())
                .build())
            .build();

        noInLeaseFollower.receive("newLeader", installSnapshot);
        noInLeaseFollower.receive("newLeader", installSnapshot);

        verify(snapshotInstaller, times(1)).install(eq(fsmSnapshot), eq("newLeader"), any());
    }

    @Test
    public void testSnapshotInstallFailed() {
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(clusterConfig)
            .setIndex(0)
            .setTerm(1)
            .build();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", snapshot);

        RaftNodeStateFollower noInLeaseFollower = new RaftNodeStateFollower(1, 0, null,
            defaultRaftConfig, stateStorage, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled,
            "cluster", "testCluster", "rangeId", "testRange");

        ByteString fsmSnapshot = ByteString.copyFrom("snapshot".getBytes());
        RaftMessage installSnapshot = RaftMessage.newBuilder()
            .setTerm(1)
            .setInstallSnapshot(InstallSnapshot.newBuilder()
                .setLeaderId("newLeader")
                .setSnapshot(Snapshot.newBuilder()
                    .setTerm(1)
                    .setIndex(0)
                    .setData(fsmSnapshot)
                    .build())
                .build())
            .build();

        when(onSnapshotInstalled.done(any(), any(), any())).thenAnswer(
            (Answer<CompletableFuture<Void>>) invocation -> {
                CompletableFuture<Void> onDone = new CompletableFuture<>();
                noInLeaseFollower
                    .onSnapshotRestored(invocation.getArgument(0),
                        invocation.getArgument(1),
                        invocation.getArgument(2),
                        onDone);
                return onDone;
            });

        noInLeaseFollower.receive("newLeader", installSnapshot);

        ArgumentCaptor<IRaftNode.IAfterInstalledCallback> promiseCaptor =
            ArgumentCaptor.forClass(IRaftNode.IAfterInstalledCallback.class);
        verify(snapshotInstaller).install(eq(fsmSnapshot), eq("newLeader"), promiseCaptor.capture());

        CompletableFuture<Void> afterSnapshotRestored =
            promiseCaptor.getValue().call(null, new RuntimeException("Test Exception"));

        assertTrue(afterSnapshotRestored.isCompletedExceptionally());

        verify(onSnapshotInstalled).done(any(), any(), any(Throwable.class));
        ArgumentCaptor<Map<String, List<RaftMessage>>> msgCaptor = ArgumentCaptor.forClass(Map.class);
        verify(msgSender, times(1)).send(msgCaptor.capture());
        assertEquals(msgCaptor.getValue(), new HashMap<String, List<RaftMessage>>() {{
            put("newLeader", Collections.singletonList(RaftMessage.newBuilder()
                .setTerm(1)
                .setInstallSnapshotReply(InstallSnapshotReply.newBuilder()
                    .setRejected(true)
                    .setLastIndex(0)
                    .build())
                .build()));
        }});
        verify(eventListener, times(0)).onEvent(any());
    }

    @Test
    public void testSnapshotRestore() {
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(clusterConfig)
            .setIndex(0)
            .setTerm(1)
            .build();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", snapshot);

        RaftNodeStateFollower noInLeaseFollower = new RaftNodeStateFollower(1, 0, null,
            defaultRaftConfig, stateStorage, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);

        ByteString fsmSnapshot = ByteString.copyFrom("snapshot".getBytes());
        RaftMessage installSnapshot = RaftMessage.newBuilder()
            .setTerm(1)
            .setInstallSnapshot(InstallSnapshot.newBuilder()
                .setLeaderId("newLeader")
                .setSnapshot(Snapshot.newBuilder()
                    .setIndex(1)
                    .setTerm(1)
                    .setData(fsmSnapshot)
                    .build())
                .build())
            .build();
        when(onSnapshotInstalled.done(any(), any(), any())).thenAnswer(
            (Answer<CompletableFuture<Void>>) invocation -> {
                CompletableFuture<Void> onDone = new CompletableFuture<>();
                noInLeaseFollower
                    .onSnapshotRestored(invocation.getArgument(0),
                        invocation.getArgument(1),
                        invocation.getArgument(2),
                        onDone);
                return onDone;
            });

        noInLeaseFollower.receive("newLeader", installSnapshot);

        ArgumentCaptor<IRaftNode.IAfterInstalledCallback> promiseCaptor =
            ArgumentCaptor.forClass(IRaftNode.IAfterInstalledCallback.class);
        verify(snapshotInstaller).install(eq(fsmSnapshot), eq("newLeader"), promiseCaptor.capture());

        CompletableFuture<Void> afterSnapshotRestored =
            promiseCaptor.getValue().call(fsmSnapshot, null);

        assertTrue(afterSnapshotRestored.isDone() && !afterSnapshotRestored.isCompletedExceptionally());
        verify(onSnapshotInstalled).done(any(), any(), isNull());

        ArgumentCaptor<Map<String, List<RaftMessage>>> msgCaptor = ArgumentCaptor.forClass(Map.class);
        verify(msgSender, times(1)).send(msgCaptor.capture());
        assertEquals(msgCaptor.getValue(), new HashMap<String, List<RaftMessage>>() {{
            put("newLeader", Collections.singletonList(RaftMessage.newBuilder()
                .setTerm(1)
                .setInstallSnapshotReply(InstallSnapshotReply.newBuilder()
                    .setRejected(false)
                    .setLastIndex(1)
                    .build())
                .build()));
        }});

        ArgumentCaptor<RaftEvent> eventCaptor = ArgumentCaptor.forClass(RaftEvent.class);
        verify(eventListener, times(1)).onEvent(eventCaptor.capture());

        List<RaftEvent> events = eventCaptor.getAllValues();

        assertEquals(events.get(0).type, RaftEventType.SNAPSHOT_RESTORED);
        assertEquals(((SnapshotRestoredEvent) events.get(0)).snapshot,
            installSnapshot.getInstallSnapshot().getSnapshot());
    }

    @Test
    public void testRestoredWithSemanticallySameSnapshot() {
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(clusterConfig)
            .setIndex(0)
            .setTerm(1)
            .build();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", snapshot);

        RaftNodeStateFollower noInLeaseFollower = new RaftNodeStateFollower(1, 0, null,
            defaultRaftConfig, stateStorage, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);

        ByteString fsmSnapshot1 = ByteString.copyFrom("snapshot1".getBytes());
        RaftMessage installSnapshot1 = RaftMessage.newBuilder()
            .setTerm(1)
            .setInstallSnapshot(InstallSnapshot.newBuilder()
                .setLeaderId("newLeader")
                .setSnapshot(Snapshot.newBuilder()
                    .setIndex(0)
                    .setTerm(1)
                    .setData(fsmSnapshot1)
                    .build())
                .build())
            .build();
        ByteString fsmSnapshot2 = ByteString.copyFrom("snapshot2".getBytes());

        when(onSnapshotInstalled.done(any(), any(), any())).thenAnswer(
            (Answer<CompletableFuture<Void>>) invocation -> {
                CompletableFuture<Void> onDone = new CompletableFuture<>();
                noInLeaseFollower
                    .onSnapshotRestored(invocation.getArgument(0),
                        invocation.getArgument(1),
                        invocation.getArgument(2),
                        onDone);
                return onDone;
            });

        noInLeaseFollower.receive("newLeader", installSnapshot1);
        ArgumentCaptor<IRaftNode.IAfterInstalledCallback> promiseCaptor =
            ArgumentCaptor.forClass(IRaftNode.IAfterInstalledCallback.class);
        verify(snapshotInstaller).install(eq(fsmSnapshot1), eq("newLeader"), promiseCaptor.capture());

        CompletableFuture<Void> afterSnapshotRestored =
            promiseCaptor.getValue().call(fsmSnapshot2, null);

        assertTrue(afterSnapshotRestored.isDone() && !afterSnapshotRestored.isCompletedExceptionally());


        verify(onSnapshotInstalled).done(eq(fsmSnapshot1), eq(fsmSnapshot2), isNull());

        ArgumentCaptor<Map<String, List<RaftMessage>>> msgCaptor = ArgumentCaptor.forClass(Map.class);
        verify(msgSender, times(1)).send(msgCaptor.capture());
        assertEquals(msgCaptor.getValue(), new HashMap<String, List<RaftMessage>>() {{
            put("newLeader", Collections.singletonList(RaftMessage.newBuilder()
                .setTerm(1)
                .setInstallSnapshotReply(InstallSnapshotReply.newBuilder()
                    .setRejected(false)
                    .setLastIndex(0)
                    .build())
                .build()));
        }});

        ArgumentCaptor<RaftEvent> eventCaptor = ArgumentCaptor.forClass(RaftEvent.class);
        verify(eventListener, times(1)).onEvent(eventCaptor.capture());

        List<RaftEvent> events = eventCaptor.getAllValues();

        assertEquals(events.get(0).type, RaftEventType.SNAPSHOT_RESTORED);
        assertEquals(((SnapshotRestoredEvent) events.get(0)).snapshot,
            installSnapshot1.getInstallSnapshot().getSnapshot().toBuilder().setData(fsmSnapshot2).build());
    }

    @Test
    public void testReceiveAppendEntries() {
        AtomicInteger onMessageReadyIndex = new AtomicInteger();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower noInLeaseFollower = new RaftNodeStateFollower(1, 0, null, defaultRaftConfig,
            stateStorage, messages -> {
            switch (onMessageReadyIndex.get()) {
                case 0:
                case 1:
                    onMessageReadyIndex.incrementAndGet();
                    assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                        put("newLeader", Collections.singletonList(RaftMessage.newBuilder()
                            .setTerm(1)
                            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                                .setAccept(AppendEntriesReply.Accept.newBuilder()
                                    .setLastIndex(1)
                                    .build())
                                .build())
                            .build()));
                    }});
                    break;
                case 2:
                    onMessageReadyIndex.incrementAndGet();
                    assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                        put("newLeader", Collections.singletonList(RaftMessage.newBuilder()
                            .setTerm(1)
                            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                                .setReject(AppendEntriesReply.Reject.newBuilder()
                                    .setTerm(1)
                                    .setRejectedIndex(2)
                                    .setLastIndex(1)
                                    .build())
                                .build())
                            .build()));
                    }});
                    break;
                case 3:
                    assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                        put("newLeader", Collections.singletonList(RaftMessage.newBuilder()
                            .setTerm(2)
                            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                                .setAccept(AppendEntriesReply.Accept.newBuilder()
                                    .setLastIndex(2)
                                    .build())
                                .build())
                            .build()));
                    }});
                    break;
            }
        }, eventListener,
            (appSMSnapshot, leader, promise) -> {
                assertEquals(appSMSnapshot, ByteString.copyFromUtf8("snapshot"));
                assertEquals(leader, "newLeader");
                promise.call(appSMSnapshot, null);
            }, onSnapshotInstalled);
        stateStorage.addStableListener(noInLeaseFollower::stableTo);
        // appended entries
        RaftMessage appendEntries = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntries(AppendEntries.newBuilder()
                .setLeaderId("newLeader")
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setCommitIndex(1)
                .addAllEntries(Collections.singleton(LogEntry.newBuilder()
                    .setTerm(1)
                    .setIndex(1)
                    .setData(ByteString.EMPTY)
                    .build()))
                .build())
            .build();
        noInLeaseFollower.receive("newLeader", appendEntries);

        // obsolete entries
        appendEntries = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntries(AppendEntries.newBuilder()
                .setLeaderId("newLeader")
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setCommitIndex(1)
                .addAllEntries(Collections.singleton(LogEntry.newBuilder()
                    .setTerm(1)
                    .setIndex(1)
                    .setData(ByteString.EMPTY)
                    .build()))
                .build())
            .build();
        noInLeaseFollower.receive("newLeader", appendEntries);

        // rejected entries
        appendEntries = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntries(AppendEntries.newBuilder()
                .setLeaderId("newLeader")
                .setPrevLogIndex(2)
                .setPrevLogTerm(1)
                .setCommitIndex(1)
                .addAllEntries(Collections.singleton(LogEntry.newBuilder()
                    .setTerm(1)
                    .setIndex(2)
                    .setData(ByteString.EMPTY)
                    .build()))
                .build())
            .build();
        noInLeaseFollower.receive("newLeader", appendEntries);

        // higher term
        appendEntries = RaftMessage.newBuilder()
            .setTerm(2)
            .setAppendEntries(AppendEntries.newBuilder()
                .setLeaderId("newLeader")
                .setPrevLogIndex(1)
                .setPrevLogTerm(1)
                .setCommitIndex(1)
                .addAllEntries(Collections.singleton(LogEntry.newBuilder()
                    .setTerm(2)
                    .setIndex(2)
                    .setData(ByteString.EMPTY)
                    .build()))
                .build())
            .build();
        noInLeaseFollower.receive("newLeader", appendEntries);
    }
}