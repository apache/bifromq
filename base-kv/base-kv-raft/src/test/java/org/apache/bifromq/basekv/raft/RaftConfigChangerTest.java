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

package org.apache.bifromq.basekv.raft;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.raft.exception.ClusterConfigChangeException;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.raft.proto.RaftNodeSyncState;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RaftConfigChangerTest {
    RaftConfigChanger configChanger;
    RaftConfig config = new RaftConfig().setInstallSnapshotTimeoutTick(3).setElectionTimeoutTick(1);
    @Mock
    IRaftStateStore stateStorage;
    @Mock
    PeerLogTracker peerLogTracker;
    @Mock
    Logger logger;

    @Mock
    IPeerLogReplicator logReplicator;

    private AutoCloseable closeable;
    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        configChanger = new RaftConfigChanger(config, stateStorage, peerLogTracker, logger);
    }

    @AfterMethod
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void testStateAfterInit() {
        Assert.assertEquals(configChanger.state(), RaftConfigChanger.State.Waiting);
    }

    @Test
    public void testSubmit() {
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .build());
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        assertTrue(configChanger.remotePeers().containsAll(Arrays.asList("V1", "V2", "V3")));
        configChanger.submit("cId", new HashSet<>() {{
            add("N1");
            add("N2");
            add("N3");
        }}, new HashSet<>() {{
            add("L1");
        }}, onDone);

        verify(peerLogTracker).startTracking(new HashSet<>() {{
            add("N1");
            add("N2");
            add("N3");
            add("L1");
        }}, true);
        Assert.assertEquals(configChanger.state(), RaftConfigChanger.State.CatchingUp);
        assertTrue(configChanger.remotePeers().containsAll(Arrays.asList("V1", "V2", "V3", "N1", "N2", "N3", "L1")));
        assertFalse(onDone.isDone());
    }

    @Test
    public void testSubmitExceptionally() {
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .build());

        CompletableFuture<Void> onDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<>() {{
            add("N1");
            add("N2");
            add("N3");
        }}, new HashSet<>() {{
            add("L1");
        }}, onDone);
        verify(peerLogTracker).startTracking(new HashSet<>() {{
            add("N1");
            add("N2");
            add("N3");
            add("L1");
        }}, true);

        CompletableFuture<Void> nextDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<String>() {{
            add("N1");
            add("N2");
            add("N3");
        }}, new HashSet<>() {{
            add("L1");
        }}, nextDone);
        assertTrue(nextDone.isDone() && nextDone.isCompletedExceptionally());

        nextDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<>(), new HashSet<>(), nextDone);
        assertTrue(nextDone.isDone() && nextDone.isCompletedExceptionally());

        nextDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<String>() {{
            add("N1");
        }}, new HashSet<String>() {{
            add("N1");
        }}, nextDone);
        assertTrue(nextDone.isDone() && nextDone.isCompletedExceptionally());
    }

    @Test
    public void testCatchupTimeout() {
        when(stateStorage.latestClusterConfig())
            .thenReturn(ClusterConfig.newBuilder()
                .addVoters("V1")
                .addVoters("V2")
                .addVoters("V3")
                .addLearners("L1")
                .build());
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<>() {{
            add("V1");
            add("N2");
            add("N3");
        }}, new HashSet<>() {{
            add("L1");
            add("L2");
        }}, onDone);

        verify(peerLogTracker).startTracking(new HashSet<>() {{
            add("V1");
            add("N2");
            add("N3");
            add("L1");
            add("L2");
        }}, true);

        Assert.assertEquals(configChanger.state(), RaftConfigChanger.State.CatchingUp);

        assertTrue(configChanger.remotePeers().containsAll(Arrays.asList("V1", "V2", "V3", "N2", "N3", "L1", "L2")));

        for (int i = 0; i < 12; i++) {
            assertFalse(configChanger.tick(1));
        }
        assertTrue(configChanger.tick(1));

        verify(peerLogTracker).stopTracking(new HashSet<>() {{
            add("N2");
            add("N3");
            add("L2");
        }});

        Assert.assertEquals(configChanger.state(), RaftConfigChanger.State.Waiting);

        assertTrue(configChanger.remotePeers().containsAll(Arrays.asList("V1", "V2", "V3", "L1")));
        assertTrue(onDone.isDone() && onDone.isCompletedExceptionally());
    }

    @Test
    public void testDirectlyTransitToTargetConfigCommitting() {
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .addLearners("L1")
            .build());

        CompletableFuture<Void> onDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<>() {{
            add("V1");
            add("V2");
            add("V3");
        }}, new HashSet<>() {{
            add("L1");
        }}, onDone);
        verify(peerLogTracker).startTracking(new HashSet<>() {{
            add("V1");
            add("V2");
            add("V3");
            add("L1");
        }}, true);

        Assert.assertEquals(configChanger.state(), RaftConfigChanger.State.CatchingUp);

        when(peerLogTracker.status(anyString())).thenReturn(RaftNodeSyncState.Replicating);
        when(peerLogTracker.matchIndex(anyString())).thenReturn(10L);

        when(stateStorage.lastIndex()).thenReturn(10L, 11L);
        when(stateStorage.local()).thenReturn("localId");
        assertTrue(configChanger.tick(1));
        Assert.assertEquals(configChanger.state(), RaftConfigChanger.State.TargetConfigCommitting);
        assertTrue(configChanger.remotePeers().containsAll(Arrays.asList("V1", "V2", "V3", "L1")));
        assertFalse(onDone.isDone());

        verify(peerLogTracker).replicateBy("localId", 11);

        ArgumentCaptor<List<LogEntry>> entries = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Boolean> flush = ArgumentCaptor.forClass(Boolean.class);

        verify(stateStorage).append(entries.capture(), flush.capture());
        assertTrue(flush.getValue());
        assertEquals(entries.getValue().size(), 1);
        LogEntry logEntry = entries.getValue().get(0);
        assertEquals(logEntry.getTerm(), 1);
        assertEquals(logEntry.getIndex(), 12);
        assertTrue(logEntry.hasConfig());
        ClusterConfig targetConfig = logEntry.getConfig();
        assertEquals(new HashSet<>(targetConfig.getVotersList()), new HashSet<String>() {{
            add("V1");
            add("V2");
            add("V3");
        }});
        assertEquals(new HashSet<>(targetConfig.getLearnersList()), new HashSet<String>() {{
            add("L1");
        }});
        assertTrue(targetConfig.getNextVotersList().isEmpty());
        assertTrue(targetConfig.getNextLearnersList().isEmpty());

        // later ticks won't change state
        assertFalse(configChanger.tick(1));
    }

    @Test
    public void testTransitToJointConfigCommitting() {
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .addLearners("L1")
            .build());
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<>() {{
            add("V1");
            add("N2");
            add("N3");
        }}, new HashSet<>() {{
            add("L1");
            add("L2");
        }}, onDone);
        Assert.assertEquals(configChanger.state(), RaftConfigChanger.State.CatchingUp);
        verify(peerLogTracker).startTracking(new HashSet<>() {{
            add("V1");
            add("N2");
            add("N3");
            add("L1");
            add("L2");
        }}, true);
        when(peerLogTracker.status(anyString())).thenReturn(RaftNodeSyncState.Replicating);
        when(peerLogTracker.matchIndex(anyString())).thenReturn(10L);

        when(stateStorage.lastIndex()).thenReturn(10L, 11L);
        when(stateStorage.local()).thenReturn("localId");

        assertTrue(configChanger.tick(1));
        Assert.assertEquals(configChanger.state(), RaftConfigChanger.State.JointConfigCommitting);
        assertTrue(configChanger.remotePeers().containsAll(Arrays.asList("V1", "V2", "V3", "N2", "N3", "L1", "L2")));
        assertFalse(onDone.isDone());

        verify(peerLogTracker).replicateBy("localId", 11);

        ArgumentCaptor<List<LogEntry>> entries = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Boolean> flush = ArgumentCaptor.forClass(Boolean.class);
        verify(stateStorage).append(entries.capture(), flush.capture());
        assertTrue(flush.getValue());
        assertEquals(entries.getValue().size(), 1);
        LogEntry logEntry = entries.getValue().get(0);
        assertEquals(logEntry.getTerm(), 1);
        assertEquals(logEntry.getIndex(), 12);
        assertTrue(logEntry.hasConfig());
        ClusterConfig jointConfig = logEntry.getConfig();
        assertEquals(new HashSet<>(jointConfig.getNextVotersList()), new HashSet<String>() {{
            add("V1");
            add("N2");
            add("N3");
        }});
        assertEquals(new HashSet<>(jointConfig.getNextLearnersList()), new HashSet<String>() {{
            add("L1");
            add("L2");
        }});

        // later ticks won't change state
        assertFalse(configChanger.tick(1));
    }

    @Test
    public void testCommitToInWaitingState() {
        // never change state if it's waiting
        Assert.assertEquals(configChanger.state(), RaftConfigChanger.State.Waiting);
        assertFalse(configChanger.commitTo(100, 100));
    }

    @Test
    public void testCommitToInCatchingUpState() {
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .build());
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<>() {{
            add("N1");
            add("N2");
            add("N3");
        }}, new HashSet<>() {{
            add("L1");
        }}, onDone);

        verify(peerLogTracker).startTracking(new HashSet<>() {{
            add("N1");
            add("N2");
            add("N3");
            add("L1");
        }}, true);
        Assert.assertEquals(configChanger.state(), RaftConfigChanger.State.CatchingUp);
        assertFalse(configChanger.commitTo(100, 100));
    }

    @Test
    public void testAbort() {
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .build());
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<>() {{
            add("N1");
            add("N2");
            add("N3");
        }}, new HashSet<>() {{
            add("L1");
        }}, onDone);
        assertFalse(onDone.isDone());
        configChanger.abort(ClusterConfigChangeException.cancelled());

        verify(peerLogTracker).startTracking(new HashSet<>() {{
            add("N1");
            add("N2");
            add("N3");
            add("L1");
        }}, true);
        assertTrue(onDone.isCompletedExceptionally());
        assertTrue(configChanger.remotePeers().isEmpty());
    }

    @Test
    public void testAllCatchUpWhenProbingAnyPeer() {
        when(peerLogTracker.status(anyString())).thenReturn(RaftNodeSyncState.Probing);

        assertFalse(configChanger.quorumCatchUp(new HashSet<>() {{
            add("V1");
            add("V2");
            add("V3");
        }}));
    }

    @Test
    public void testQuorumCatchUp() {
        when(stateStorage.lastIndex()).thenReturn(10L);

        when(peerLogTracker.matchIndex(anyString())).thenReturn(10L);
        when(peerLogTracker.status(anyString())).thenReturn(RaftNodeSyncState.Replicating);
        assertTrue(configChanger.quorumCatchUp(new HashSet<String>() {{
            add("V1");
        }}));

        when(stateStorage.lastIndex()).thenReturn(10L);
        when(peerLogTracker.matchIndex(anyString())).thenReturn(10L, 10L);
        when(peerLogTracker.status(anyString())).thenReturn(RaftNodeSyncState.Replicating,
            RaftNodeSyncState.Probing);

        assertFalse(configChanger.quorumCatchUp(new HashSet<String>() {{
            add("V1");
            add("V2");
        }}));


        when(stateStorage.lastIndex()).thenReturn(10L);
        when(peerLogTracker.matchIndex(anyString())).thenReturn(10L, 10L, 10L);
        when(peerLogTracker.status(anyString())).thenReturn(RaftNodeSyncState.Replicating);

        assertTrue(configChanger.quorumCatchUp(new HashSet<String>() {{
            add("V1");
            add("V2");
            add("V3");
        }}));

        when(stateStorage.lastIndex()).thenReturn(10L);
        when(peerLogTracker.matchIndex("V1")).thenReturn(15L);
        when(peerLogTracker.matchIndex("V2")).thenReturn(5L);
        when(peerLogTracker.matchIndex("V3")).thenReturn(10L);
        when(peerLogTracker.catchupRate("V2")).thenReturn(0L);
        when(peerLogTracker.status(anyString())).thenReturn(RaftNodeSyncState.Replicating);
        assertFalse(configChanger.quorumCatchUp(new HashSet<String>() {{
            add("V1");
            add("V2");
            add("V3");
        }}));

        when(stateStorage.lastIndex()).thenReturn(10L);
        when(peerLogTracker.matchIndex("V1")).thenReturn(10L);
        when(peerLogTracker.matchIndex("V2")).thenReturn(5L);
        when(peerLogTracker.matchIndex("V3")).thenReturn(10L);
        when(peerLogTracker.catchupRate("V2")).thenReturn(2L);
        when(peerLogTracker.status(anyString())).thenReturn(RaftNodeSyncState.Replicating);

        assertTrue(configChanger.quorumCatchUp(new HashSet<>() {{
            add("V1");
            add("V2");
            add("V3");
        }}));

        when(stateStorage.lastIndex()).thenReturn(10L);
        when(peerLogTracker.matchIndex("V1")).thenReturn(10L);
        when(peerLogTracker.matchIndex("V2")).thenReturn(5L);
        when(peerLogTracker.matchIndex("V3")).thenReturn(10L);
        when(peerLogTracker.catchupRate("V2")).thenReturn(5L);
        when(peerLogTracker.status("V1")).thenReturn(RaftNodeSyncState.Replicating);
        when(peerLogTracker.status("V2")).thenReturn(RaftNodeSyncState.Replicating);
        when(peerLogTracker.status("V3")).thenReturn(RaftNodeSyncState.Replicating);
        assertTrue(configChanger.quorumCatchUp(new HashSet<>() {{
            add("V1");
            add("V2");
            add("V3");
        }}));
    }
}
