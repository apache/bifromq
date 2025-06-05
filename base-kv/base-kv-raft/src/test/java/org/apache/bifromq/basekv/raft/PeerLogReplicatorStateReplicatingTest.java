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

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.raft.proto.RaftNodeSyncState;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import java.util.Optional;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PeerLogReplicatorStateReplicatingTest {
    PeerLogReplicatorStateReplicating stateReplicating;
    RaftConfig config = new RaftConfig().setElectionTimeoutTick(6).setHeartbeatTimeoutTick(5).setMaxInflightAppends(3);

    @Mock
    IRaftStateStore stateStorage;
    @Mock
    Logger logger;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        stateReplicating = new PeerLogReplicatorStateReplicating("V1", config, stateStorage, 1, 2, logger);
    }

    @AfterMethod
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void testInitialize() {
        assertFalse(stateReplicating.pauseReplicating());
        assertEquals(stateReplicating.matchIndex(), 1);
        assertEquals(stateReplicating.nextIndex(), 2);
        assertEquals(stateReplicating.catchupRate(), 0);
    }

    @Test
    public void testCurrentState() {
        assertEquals(stateReplicating.state(), RaftNodeSyncState.Replicating);
    }

    @Test
    public void testPauseByMaxInflight() {
        stateReplicating.replicateTo(2);
        assertEquals(stateReplicating.matchIndex(), 1);
        assertEquals(stateReplicating.nextIndex(), 3);
        assertFalse(stateReplicating.pauseReplicating());
        stateReplicating.replicateTo(3);
        assertEquals(stateReplicating.matchIndex(), 1);
        assertEquals(stateReplicating.nextIndex(), 4);
        assertFalse(stateReplicating.pauseReplicating());
        stateReplicating.replicateTo(4);
        assertEquals(stateReplicating.matchIndex(), 1);
        assertEquals(stateReplicating.nextIndex(), 5);
        assertTrue(stateReplicating.pauseReplicating()); // now we have 3 inflight replicating messages

        stateReplicating.confirmMatch(2);
        assertFalse(stateReplicating.pauseReplicating()); // now we have 2 inflight replicating messages
    }

    @Test
    public void testPauseResumedByHeartbeatTimeout() {
        stateReplicating.replicateTo(2);
        stateReplicating.tick();
        assertFalse(stateReplicating.pauseReplicating());

        stateReplicating.replicateTo(3);
        stateReplicating.tick();
        assertFalse(stateReplicating.pauseReplicating());

        stateReplicating.replicateTo(4);
        assertTrue(stateReplicating.pauseReplicating());
        stateReplicating.tick();
        assertTrue(stateReplicating.pauseReplicating());
        // four more ticks to trigger heartbeat timeout
        stateReplicating.tick();
        assertTrue(stateReplicating.pauseReplicating());
        stateReplicating.tick();
        assertTrue(stateReplicating.pauseReplicating());
        stateReplicating.tick();
        assertTrue(stateReplicating.pauseReplicating());
        stateReplicating.tick();
        assertTrue(stateReplicating.pauseReplicating());
        assertFalse(stateReplicating.needHeartbeat());
        stateReplicating.tick();
        assertTrue(stateReplicating.needHeartbeat());
        assertFalse(stateReplicating.pauseReplicating());
    }

    @Test
    public void testConfirm() {
        stateReplicating.replicateTo(2);
        stateReplicating.replicateTo(3);
        stateReplicating.replicateTo(4);

        stateReplicating.confirmMatch(2);
        assertFalse(stateReplicating.pauseReplicating()); // now we have 2 inflight replicating messages
        assertEquals(stateReplicating.matchIndex(), 2);
        assertEquals(stateReplicating.nextIndex(), 5);

        stateReplicating.confirmMatch(3);
        assertFalse(stateReplicating.pauseReplicating()); // now we have 1 inflight replicating messages
        assertEquals(stateReplicating.matchIndex(), 3);
        assertEquals(stateReplicating.nextIndex(), 5);

        stateReplicating.confirmMatch(4);
        assertFalse(stateReplicating.pauseReplicating()); // now we have 0 inflight replicating messages
        assertEquals(stateReplicating.matchIndex(), 4);
        assertEquals(stateReplicating.nextIndex(), 5);
    }

    @Test
    public void testOutOfOrderConfirm() {
        stateReplicating.replicateTo(2);
        stateReplicating.replicateTo(3);
        stateReplicating.replicateTo(4);

        stateReplicating.confirmMatch(4);
        assertFalse(stateReplicating.pauseReplicating()); // now we have 0 inflight replicating messages
        assertEquals(stateReplicating.matchIndex(), 4);
        assertEquals(stateReplicating.nextIndex(), 5);

        stateReplicating.confirmMatch(2);
        assertFalse(stateReplicating.pauseReplicating()); // now we have 0 inflight replicating messages
        assertEquals(stateReplicating.matchIndex(), 4);
        assertEquals(stateReplicating.nextIndex(), 5);

        stateReplicating.confirmMatch(3);
        assertFalse(stateReplicating.pauseReplicating()); // now we have 0 inflight replicating messages
        assertEquals(stateReplicating.matchIndex(), 4);
        assertEquals(stateReplicating.nextIndex(), 5);
    }

    @Test
    public void testCatchupRate() {
        assertEquals(stateReplicating.catchupRate(), 0);
        stateReplicating.replicateTo(2);
        stateReplicating.tick();
        assertEquals(stateReplicating.catchupRate(), 0);
        stateReplicating.replicateTo(3);
        stateReplicating.tick();
        assertEquals(stateReplicating.catchupRate(), 0);
        stateReplicating.replicateTo(4);
        stateReplicating.tick();
        assertEquals(stateReplicating.catchupRate(), 0);

        stateReplicating.confirmMatch(4);
        assertEquals(stateReplicating.catchupRate(), 0);
        stateReplicating.tick();
        // catchup rate is calculated in tick
        assertEquals(stateReplicating.catchupRate(), 3);
        stateReplicating.tick();
        assertEquals(stateReplicating.catchupRate(), 0);
    }

    @Test
    public void testBackoffIgnoreObsoleteReject() {
        stateReplicating.replicateTo(4);
        stateReplicating.confirmMatch(4);
        PeerLogReplicatorState nextState = stateReplicating.backoff(3, 3);
        assertEquals(nextState, stateReplicating);
    }

    @Test
    public void testBackoffTransitToProbing1() {
        when(stateStorage.entryAt(2)).thenReturn(Optional.ofNullable(LogEntry.newBuilder().build()));

        stateReplicating.replicateTo(4);
        // peerLastIndex is greater than matchIndex
        PeerLogReplicatorState nextState = stateReplicating.backoff(1, 2);
        assertEquals(nextState.state(), RaftNodeSyncState.Probing);
        assertEquals(nextState.matchIndex(), 1);
        assertEquals(nextState.nextIndex(), 2);
    }

    @Test
    public void testBackoffTransitToProbing2() {
        PeerLogReplicatorState stateReplicating =
            new PeerLogReplicatorStateReplicating("V1", config, stateStorage, 10, 15, logger);

        when(stateStorage.entryAt(8)).thenReturn(Optional.ofNullable(LogEntry.newBuilder().build()));

        stateReplicating.replicateTo(20);
        // peerLastIndex is less than matchIndex
        PeerLogReplicatorState nextState = stateReplicating.backoff(14, 8);
        assertEquals(nextState.state(), RaftNodeSyncState.Probing);
        assertEquals(nextState.matchIndex(), 8);
        assertEquals(nextState.nextIndex(), 9);
    }

    @Test
    public void testBackoffTransitToSnapshotSyncing() {
        when(stateStorage.entryAt(2)).thenReturn(Optional.ofNullable(null));
        when(stateStorage.latestSnapshot()).thenReturn(Snapshot.newBuilder().setIndex(1L).build());

        stateReplicating.replicateTo(4);
        // peerLastIndex is greater than matchIndex
        PeerLogReplicatorState nextState = stateReplicating.backoff(1, 2);
        assertEquals(nextState.state(), RaftNodeSyncState.SnapshotSyncing);
        assertEquals(nextState.matchIndex(), 1);
        assertEquals(nextState.nextIndex(), 2);
    }

    @Test
    public void testNoProgressTransitToProbing() {
        // prepare max in-flight appends
        stateReplicating.replicateTo(2);
        stateReplicating.replicateTo(3);
        stateReplicating.replicateTo(4);
        // 5 ticks to trigger heartbeat timeout
        assertEquals(stateReplicating.tick(), stateReplicating);
        stateReplicating.replicateTo(5);
        assertEquals(stateReplicating.tick(), stateReplicating);
        stateReplicating.replicateTo(6);
        assertEquals(stateReplicating.tick(), stateReplicating);
        stateReplicating.replicateTo(7);
        assertEquals(stateReplicating.tick(), stateReplicating);
        stateReplicating.replicateTo(7);
        assertEquals(stateReplicating.tick(), stateReplicating);
        stateReplicating.replicateTo(7);

        // election timeout
        assertEquals(stateReplicating.tick().state(), RaftNodeSyncState.Probing);
    }
}
