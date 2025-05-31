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

public class PeerLogReplicatorStateProbingTest {
    private PeerLogReplicatorStateProbing stateProbing;
    private String peerId = "V1";
    private RaftConfig config = new RaftConfig().setHeartbeatTimeoutTick(5);
    @Mock
    private IRaftStateStore stateStorage;
    @Mock
    private Logger logger;
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
    public void testInitialize() {
        when(stateStorage.lastIndex()).thenReturn(15L);
        when(stateStorage.latestSnapshot()).thenReturn(Snapshot.newBuilder().setIndex(10L).build());
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, logger);

        assertEquals(stateProbing.matchIndex, 10);
        assertEquals(stateProbing.nextIndex, 16);
        assertEquals(RaftNodeSyncState.Probing, stateProbing.state());
    }

    @Test
    public void testHeartbeat() {
        when(stateStorage.lastIndex()).thenReturn(15L);
        when(stateStorage.latestSnapshot()).thenReturn(Snapshot.newBuilder().setIndex(10L).build());
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, logger);

        int i = 6;
        while (i-- > 0) {
            assertEquals(stateProbing, stateProbing.tick());
        }
    }

    @Test
    public void testPauseReplicatingUntilHeartbeatTimeout() {
        when(stateStorage.lastIndex()).thenReturn(0L);
        when(stateStorage.latestSnapshot()).thenReturn(Snapshot.newBuilder().setIndex(0L).build());
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, logger);

        assertFalse(stateProbing.pauseReplicating());
        stateProbing.replicateTo(1);
        assertEquals(stateProbing.catchupRate(), 0);
        assertEquals(stateProbing.nextIndex, 1);
        assertTrue(stateProbing.pauseReplicating());
        int i = 5;
        while (i-- > 0) {
            stateProbing.tick();
            assertEquals(stateProbing, stateProbing.tick());
        }
        stateProbing.tick();
        assertFalse(stateProbing.pauseReplicating());
    }

    @Test
    public void testConfirmMatchIgnoreOutdatedIndex() {
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, 10, 15, logger);

        assertEquals(stateProbing.matchIndex(), 10);
        assertEquals(stateProbing.nextIndex(), 15);
        assertEquals(stateProbing.confirmMatch(9), stateProbing);
    }

    @Test
    public void testConfirmMatch() {
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, 10, 15, logger);
        assertEquals(stateProbing.matchIndex, 10);
        assertEquals(stateProbing.nextIndex, 15);
        PeerLogReplicatorState nextState = stateProbing.confirmMatch(16);
        assertEquals(stateProbing.catchupRate(), 0);
        assertEquals(nextState.state(), RaftNodeSyncState.Replicating);
        assertEquals(nextState.matchIndex(), 16);
        assertEquals(nextState.nextIndex(), 17);
    }

    @Test
    public void testBackoffWithObsoleteIndexWillNotResumeReplicating() {
        when(stateStorage.latestSnapshot()).thenReturn(Snapshot.newBuilder().setIndex(0L).build());
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, 10, 15, logger);
        stateProbing.replicateTo(20);
        assertTrue(stateProbing.pauseReplicating());
        stateProbing.backoff(30, 10); // not matched
        assertTrue(stateProbing.pauseReplicating());
    }

    @Test
    public void testBackoffWithExpectedIndexWillResumeReplicating() {

        when(stateStorage.entryAt(10)).thenReturn(Optional.of(LogEntry.newBuilder().build()));
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, 10, 15, logger);

        stateProbing.replicateTo(20);
        assertTrue(stateProbing.pauseReplicating());
        assertEquals(stateProbing.nextIndex, 15); //next index won't be update in probing
        PeerLogReplicatorState nextState = stateProbing.backoff(14, 9); // not matched
        assertEquals(stateProbing, nextState);
        assertEquals(stateProbing.matchIndex(), 9);
        assertEquals(stateProbing.nextIndex(), 10);
        assertFalse(stateProbing.pauseReplicating());
    }

    @Test
    public void testBackoffWillTransitToSnapshotSyncing() {
        when(stateStorage.entryAt(10)).thenReturn(Optional.ofNullable(null));
        when(stateStorage.latestSnapshot()).thenReturn(Snapshot.newBuilder().setIndex(5L).build());
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, 10, 15, logger);

        stateProbing.replicateTo(20);
        assertTrue(stateProbing.pauseReplicating());
        PeerLogReplicatorState nextState = stateProbing.backoff(14, 9); // not matched
        assertEquals(nextState.state(), RaftNodeSyncState.SnapshotSyncing);
        assertEquals(nextState.matchIndex(), 5);
        assertEquals(nextState.nextIndex(), 6);
    }

    @Test
    public void testBackoffNoLess1() {
        when(stateStorage.entryAt(1)).thenReturn(Optional.of(LogEntry.newBuilder().build()));
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, 1, 2, logger);

        stateProbing.replicateTo(2); // send log entry at index 2
        PeerLogReplicatorState nextState = stateProbing.backoff(1, 1);
        assertEquals(nextState, stateProbing);
        assertEquals(nextState.matchIndex(), 0);
        assertEquals(nextState.nextIndex(), 1);
        assertFalse(stateProbing.pauseReplicating());
    }
}
