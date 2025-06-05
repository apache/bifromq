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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.bifromq.basekv.raft.event.CommitEvent;
import org.apache.bifromq.basekv.raft.event.ElectionEvent;
import org.apache.bifromq.basekv.raft.event.RaftEventType;
import org.apache.bifromq.basekv.raft.exception.ClusterConfigChangeException;
import org.apache.bifromq.basekv.raft.exception.DropProposalException;
import org.apache.bifromq.basekv.raft.exception.LeaderTransferException;
import org.apache.bifromq.basekv.raft.proto.AppendEntries;
import org.apache.bifromq.basekv.raft.proto.AppendEntriesReply;
import org.apache.bifromq.basekv.raft.proto.InstallSnapshot;
import org.apache.bifromq.basekv.raft.proto.InstallSnapshotReply;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.raft.proto.RaftMessage;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.raft.proto.RaftNodeSyncState;
import org.apache.bifromq.basekv.raft.proto.RequestVote;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import org.apache.bifromq.basekv.raft.proto.TimeoutNow;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RaftNodeStateLeaderTest extends RaftNodeStateTest {
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
    public void testProposeWithSlowFollower() {
        RaftNodeStateLeader leader = startUpLeader();
        leader.propose(ByteString.copyFromUtf8("command"), new CompletableFuture<>());
        // mock that only v1 and l1 received the command entry, however, the majority has been achieved
        RaftMessage appendEntriesReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setAccept(AppendEntriesReply.Accept.newBuilder()
                    .setLastIndex(2)
                    .build())
                .build())
            .build();
        leader.receive("l1", appendEntriesReply);

        // LogEntries now:
        //   index  entry
        //   1      targetConfigEntry
        //   2      commandEntry
        // commitIndex = 2, compact all entries
        leader.compact(ByteString.copyFromUtf8("appSMSnapshot"), 2, new CompletableFuture<>());

        // tick to heartbeat, send empty appendEntry to v1 and v2
        leader.tick();
        leader.tick();
        leader.tick();

        // v1 respond to heartbeat normally
        appendEntriesReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setAccept(AppendEntriesReply.Accept.newBuilder()
                    .setLastIndex(2)
                    .build())
                .build())
            .build();
        leader.receive("v1", appendEntriesReply);
        PeerLogTracker peerLogTracker = ReflectionUtils.getField(leader, "peerLogTracker");
        assert peerLogTracker != null;
        assertEquals(peerLogTracker.matchIndex("v1"), 2);
        // v2 will reject last empty appendEntry since preLogIndex is mismatched, which causes the state of the log
        // tracker changed to SnapshotSyncing
        RaftMessage appendEntriesReplyRejected = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setReject(AppendEntriesReply.Reject.newBuilder()
                    .setTerm(1)
                    .setRejectedIndex(2)
                    .setLastIndex(1)
                    .build())
                .setReadIndex(0)
                .build())
            .build();
        leader.receive("v2", appendEntriesReplyRejected);
        assertSame(peerLogTracker.status("v2"), RaftNodeSyncState.SnapshotSyncing);
        // tick to heartbeat, send empty appendEntry to v1 and send snapshot to v2
        leader.tick();
        leader.tick();
        leader.tick();


        // v2 responds to installSnapshot normally, and state of the log tracker change to Probing
        RaftMessage installSnapshotReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setInstallSnapshotReply(InstallSnapshotReply.newBuilder()
                .setRejected(false)
                .setLastIndex(2)
                .build())
            .build();
        leader.receive("v2", installSnapshotReply);
        assertSame(peerLogTracker.status("v2"), RaftNodeSyncState.Replicating);
    }

    @Test
    public void testProposeExceptionally() {
        RaftNodeStateLeader proposeThrottledLeader = startUpLeader();
        ByteString cmd = ByteString.copyFromUtf8("command");
        proposeThrottledLeader.propose(cmd, new CompletableFuture<>());
        proposeThrottledLeader.propose(cmd, new CompletableFuture<>());
        proposeThrottledLeader.propose(cmd, new CompletableFuture<>());
        proposeThrottledLeader.propose(cmd, new CompletableFuture<>());
        proposeThrottledLeader.propose(cmd, new CompletableFuture<>());

        // commit all proposed entry
//        RaftMessage appendEntriesReply = RaftMessage.newBuilder()
//                .setTerm(1)
//                .setAppendEntriesReply(AppendEntriesReply.newBuilder()
//                        .setReject(false)
//                        .setLastIndex(6)
//                        .build())
//                .build();
//        proposeThrottledLeader.receive("v1", appendEntriesReply);
//        proposeThrottledLeader.receive("v2", appendEntriesReply);

        // proposeThrottled
        CompletableFuture<Long> proposeDone = new CompletableFuture<>();
        proposeThrottledLeader.propose(cmd, proposeDone);
        assertTrue(proposeDone.isCompletedExceptionally());

        CompletableFuture<Void> onDone = new CompletableFuture<>();
        RaftNodeStateLeader transferringLeader = startUpLeader();
        transferringLeader.transferLeadership("v1", onDone);
        proposeDone = new CompletableFuture<>();
        transferringLeader.propose(cmd, proposeDone);
        assertTrue(proposeDone.isCompletedExceptionally());
    }

    @Test
    public void testReceiveAppendEntriesReplyRejected() {
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateLeader leader = startUpLeader(stateStorage, messages ->
            assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                put("v1", Collections.singletonList(RaftMessage.newBuilder()
                    .setTerm(1)
                    .setInstallSnapshot(InstallSnapshot.newBuilder()
                        .setLeaderId(local)
                        .setSnapshot(stateStorage.latestSnapshot())
                        .build())
                    .build()
                ));
            }}));

        RaftMessage appendEntriesReplyRejectedMessage = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setReject(AppendEntriesReply.Reject.newBuilder()
                    .setTerm(1)
                    .setRejectedIndex(2)
                    .setLastIndex(1) // <-- switch to snapshot syncing
                    .build())
                .build())
            .build();
        leader.receive("v1", appendEntriesReplyRejectedMessage);
    }

    @Test
    public void testElectionElapsed() {
        // use defaultRaftConfig in RaftNodeStateTest, heartbeatTimeoutTick is 5
        RaftNodeStateLeader leader = startUpLeader();

        PeerLogTracker peerLogTracker = ReflectionUtils.getField(leader, "peerLogTracker");
        assert peerLogTracker != null;

        leader.transferLeadership("v1", new CompletableFuture<>());

        leader.tick();
        leader.tick();
        leader.tick();
        leader.tick();

        // trigger activityTracker to poll
        leader.receive("v2", RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setAccept(AppendEntriesReply.Accept.newBuilder()
                    .setLastIndex(2)
                    .build())
                .build())
            .build());

        // electionElapsedTick timeout
        RaftNodeState raftNodeState = leader.tick();

        assertSame(raftNodeState.getState(), RaftNodeStatus.Leader);
        // transfer leadership timeout

        leader.tick();
        leader.tick();
        leader.tick();
        leader.tick();
        raftNodeState = leader.tick();

        // activityTracker will tally failed
        assertSame(raftNodeState.getState(), RaftNodeStatus.Follower);
    }

    @Test
    public void testTransferLeadershipToIndexMatchedFollower() {
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateLeader leader = startUpLeader(stateStorage, messages ->
            assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                put("v1", Collections.singletonList(RaftMessage.newBuilder()
                    .setTerm(1)
                    .setTimeoutNow(TimeoutNow.newBuilder().build())
                    .build()));
            }}));

        leader.transferLeadership("v1", new CompletableFuture<>());
    }

    @Test
    public void testTransferLeadershipToIndexBehindFollower() {
        AtomicInteger onMessageReadyIndex = new AtomicInteger();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateLeader leader = startUpLeader(stateStorage, messages -> {
            if (onMessageReadyIndex.get() == 0) {
                onMessageReadyIndex.incrementAndGet();
                // entry for proposed command
            } else if (onMessageReadyIndex.get() == 1) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                    put("v1", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setAppendEntries(AppendEntries.newBuilder()
                            .setLeaderId(local)
                            .setPrevLogTerm(1)
                            .setPrevLogIndex(2)
                            .setCommitIndex(1)
                            .setReadIndex(0)
                            .build())
                        .build()));
                }});
            } else if (onMessageReadyIndex.get() == 2) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                    put("v1", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setTimeoutNow(TimeoutNow.newBuilder().build())
                        .build()));
                }});
            }
        });

        leader.propose(ByteString.copyFromUtf8("command"), new CompletableFuture<>());

        leader.transferLeadership("v1", new CompletableFuture<>());

        RaftMessage appendEntriesReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setAccept(AppendEntriesReply.Accept.newBuilder()
                    .setLastIndex(3)
                    .build())
                .build())
            .build();
        leader.receive("v1", appendEntriesReply);
    }

    @Test
    public void testTransferLeadershipExceptionally() {
        RaftNodeStateLeader leader = startUpLeader();
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        leader.transferLeadership("v1", onDone);
        assertFalse(onDone.isDone());

        onDone = new CompletableFuture<>();
        leader.transferLeadership("v1", onDone);
        assertTrue(onDone.isCompletedExceptionally());

        onDone = new CompletableFuture<>();
        leader.transferLeadership("v2", onDone);
        assertTrue(onDone.isCompletedExceptionally());

        onDone = new CompletableFuture<>();
        leader.transferLeadership(local, onDone);
        assertTrue(onDone.isCompletedExceptionally());

        onDone = new CompletableFuture<>();
        leader.transferLeadership("notInClusterConfig", onDone);
        assertTrue(onDone.isCompletedExceptionally());
    }

    @Test
    public void testChangeClusterConfig() {
        RaftNodeStateLeader leader = startUpLeader();
        // nextVoters doesn't include local and v1
        Set<String> nextVoters = new HashSet<>() {{
            add("v2");
            add("v3");
        }};
        Set<String> nextLearners = new HashSet<>() {{
            add("l2");
        }};
        CompletableFuture<Void> onDone = new CompletableFuture<>();

        leader.changeClusterConfig("cId", nextVoters, nextLearners, onDone);

        RaftConfigChanger raftConfigChanger = ReflectionUtils.getField(leader, "configChanger");
        assert raftConfigChanger != null;
        assertEquals(raftConfigChanger.state(), RaftConfigChanger.State.CatchingUp);

        // broadcast appendEntries and wait all nextVoters catchup
        leader.tick();
        leader.tick();
        leader.receive("v3", RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setAccept(AppendEntriesReply.Accept.newBuilder()
                    .setLastIndex(1)
                    .build())
                .build())
            .build());
        leader.tick();
        assertEquals(raftConfigChanger.state(), RaftConfigChanger.State.JointConfigCommitting);

        leader.transferLeadership("v1", new CompletableFuture<>());

        leader.stableTo(2);
        RaftMessage appendEntriesReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setAccept(AppendEntriesReply.Accept.newBuilder()
                    .setLastIndex(2)
                    .build())
                .build())
            .build();
        leader.receive("v2", appendEntriesReply);
        leader.receive("v3", appendEntriesReply);
        // broadcast appendEntries(including targetConfigEntry)
        leader.tick();
        leader.tick();
        leader.tick();
        assertEquals(raftConfigChanger.state(), RaftConfigChanger.State.TargetConfigCommitting);

        leader.stableTo(3);

        // broadcast appendEntries(including targetConfigEntry)
        leader.tick();

        appendEntriesReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setAccept(AppendEntriesReply.Accept.newBuilder()
                    .setLastIndex(3)
                    .build())
                .build())
            .build();
        leader.receive("v2", appendEntriesReply);
        RaftNodeState raftNodeState = leader.receive("v3", appendEntriesReply);
        assertSame(raftConfigChanger.state(), RaftConfigChanger.State.Waiting);
        assertSame(raftNodeState.getState(), RaftNodeStatus.Follower);
    }

    @Test
    public void testReceiveHigherTermMessage() {
        RaftNodeStateLeader leader = startUpLeader();
        RaftMessage vote = RaftMessage.newBuilder()
            .setTerm(2)
            .setRequestVote(RequestVote.newBuilder()
                .setLastLogTerm(1)
                .setLastLogIndex(2)
                .build())
            .build();
        RaftNodeState raftNodeState = leader.receive("v3", vote); // v3 is not a member
        assertSame(raftNodeState.getState(), RaftNodeStatus.Leader);

        leader = startUpLeader();
        vote = RaftMessage.newBuilder()
            .setTerm(2)
            .setRequestVote(RequestVote.newBuilder()
                .setLastLogTerm(1)
                .setLastLogIndex(2)
                .build())
            .build();
        raftNodeState = leader.receive("v2", vote); // v2 is a member
        assertSame(raftNodeState.getState(), RaftNodeStatus.Follower);

        leader = startUpLeader(event -> {
            if (event.type == RaftEventType.ELECTION) {
                ElectionEvent electionEvent = ((ElectionEvent) event);
                assertEquals(electionEvent.leaderId, "v1");
                assertEquals(electionEvent.term, 2);
            }
        });
        RaftMessage appendEntries = RaftMessage.newBuilder()
            .setTerm(2)
            .setAppendEntries(AppendEntries.newBuilder()
                .setLeaderId("v1")
                .build())
            .build();
        raftNodeState = leader.receive("v1", appendEntries);
        assertSame(raftNodeState.getState(), RaftNodeStatus.Follower);
    }

    @Test
    public void testStopCancelOngoingTransferLeadership() {
        RaftNodeStateLeader leader = startUpLeader();
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        leader.transferLeadership("v1", onDone);
        assertFalse(onDone.isDone());
        leader.stop();
        assertTrue(onDone.isCompletedExceptionally());
        try {
            onDone.join();
            fail();
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof LeaderTransferException.CancelledException);
        }
    }

    @Test
    public void testStopCancellingOngoingConfigChange() {
        RaftNodeStateLeader leader = startUpLeader();
        // nextVoters doesn't include local and v1
        Set<String> nextVoters = new HashSet<>() {{
            add("v2");
            add("v3");
        }};
        Set<String> nextLearners = new HashSet<>() {{
            add("l2");
        }};
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        leader.changeClusterConfig("cId", nextVoters, nextLearners, onDone);
        leader.stop();
        try {
            onDone.join();
            fail();
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof ClusterConfigChangeException.CancelledException);
        }
    }

    @Test
    public void testStopCancellingOngoingPropose() {
        RaftNodeStateLeader leader = startUpLeader();
        ByteString cmd = ByteString.copyFromUtf8("command");
        CompletableFuture<Long> proposeDone = new CompletableFuture<>();
        leader.propose(cmd, proposeDone);
        assertFalse(proposeDone.isDone());
        leader.stop();
        try {
            proposeDone.join();
            fail();
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof DropProposalException.CancelledException);
        }
    }

    private RaftNodeStateLeader startUpLeader() {
        return startUpLeader(null, null, null);
    }

    private RaftNodeStateLeader startUpLeader(IRaftNode.IRaftEventListener listener) {
        return startUpLeader(null, null, listener);
    }

    private RaftNodeStateLeader startUpLeader(IRaftStateStore stateStorage,
                                              IRaftNode.IRaftMessageSender raftMessageListener) {
        return startUpLeader(stateStorage, raftMessageListener, null);
    }

    // after startUp with this method, commitIndex = 2, layout of LogEntries:
    //   index  entry
    //   1      jointConfigEntry
    //   2      targetConfigEntry
    private RaftNodeStateLeader startUpLeader(IRaftStateStore stateStorage,
                                              IRaftNode.IRaftMessageSender msgSender,
                                              IRaftNode.IRaftEventListener evtListener) {
        if (stateStorage == null) {
            stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
                .setClusterConfig(clusterConfig).build());

        }
        AtomicInteger onMessageReadyIndex = new AtomicInteger();
        AtomicInteger onCommitIndex = new AtomicInteger();
        IRaftStateStore finalStateStorage = stateStorage;
        RaftNodeStateLeader leader = new RaftNodeStateLeader(1, 0, defaultRaftConfig, stateStorage,
            new LinkedHashMap<>(), messages -> {
            if (onMessageReadyIndex.get() == 0) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(messages, new HashMap<String, List<RaftMessage>>() {{
                    put("l1", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setAppendEntries(AppendEntries.newBuilder()
                            .setLeaderId(local)
                            .setPrevLogIndex(0)
                            .setPrevLogTerm(0)
                            .setCommitIndex(0)
                            .setReadIndex(0)
                            .build())
                        .build()));
                    put("v1", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setAppendEntries(AppendEntries.newBuilder()
                            .setLeaderId(local)
                            .setPrevLogIndex(0)
                            .setPrevLogTerm(0)
                            .setCommitIndex(0)
                            .setReadIndex(0)
                            .addAllEntries(toList(finalStateStorage.entries(1, 2,
                                defaultRaftConfig.getMaxSizePerAppend())))
                            .build())
                        .build()));
                    put("v2", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setAppendEntries(AppendEntries.newBuilder()
                            .setLeaderId(local)
                            .setPrevLogIndex(0)
                            .setPrevLogTerm(0)
                            .setCommitIndex(0)
                            .setReadIndex(0)
                            .addAllEntries(toList(finalStateStorage.entries(1, 2,
                                defaultRaftConfig.getMaxSizePerAppend())))
                            .build())
                        .build()));
                }});
            } else if (onMessageReadyIndex.get() > 1) {
                if (msgSender != null) {
                    msgSender.send(messages);
                }
            }
        }, event -> {
            switch (event.type) {
                case COMMIT:
                    if (onCommitIndex.get() == 0) {
                        onCommitIndex.incrementAndGet();
                        assertEquals(((CommitEvent) event).index, 1);
                    }
                    break;
                case ELECTION:
                    if (evtListener != null) {
                        evtListener.onEvent(event);
                    }
                    break;
            }
        }, snapshotInstaller, onSnapshotInstalled);
        RaftConfigChanger raftConfigChanger = ReflectionUtils.getField(leader, "configChanger");
        assert raftConfigChanger != null;
        assertEquals(raftConfigChanger.state(), RaftConfigChanger.State.CatchingUp);

        for (String peer : clusterConfig.getVotersList()) {
            if (!peer.equals(local)) {
                leader.receive(peer, RaftMessage.newBuilder()
                    .setTerm(1)
                    .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                        .setAccept(AppendEntriesReply.Accept.newBuilder()
                            .setLastIndex(0)
                            .build())
                        .build())
                    .build());
            }
        }

        // 1. all peers caught up
        // 2. configChanger state change to TargetConfigCommitting,
        // 3. tick to broadcast appendEntries containing clusterconfig
        leader.tick();
        assertEquals(raftConfigChanger.state(), RaftConfigChanger.State.TargetConfigCommitting);

        // 1. receive jointConfig appendEntriesReply
        // 2. commitTo configChanger and its state change to TargetConfigCommitting
        // 3. tick to trigger heartbeat timeout in which branch appendEntries of targetConfig will be broadcast
        RaftMessage appendEntriesReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setAccept(AppendEntriesReply.Accept.newBuilder()
                    .setLastIndex(1)
                    .build())
                .build())
            .build();
        leader.receive("v1", appendEntriesReply);
        leader.receive("v2", appendEntriesReply);
        assertEquals(raftConfigChanger.state(), RaftConfigChanger.State.Waiting);
        return leader;
    }

    private List<LogEntry> toList(Iterator<LogEntry> itr) {
        List<LogEntry> entries = new ArrayList<>();
        itr.forEachRemaining(entries::add);
        return entries;
    }
}