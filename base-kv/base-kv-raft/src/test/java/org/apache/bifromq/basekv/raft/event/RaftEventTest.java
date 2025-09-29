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

package org.apache.bifromq.basekv.raft.event;

import static org.testng.Assert.assertEquals;

import java.util.Collections;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import org.testng.annotations.Test;

public class RaftEventTest {
    @Test
    public void typeMatch() {
        CommitEvent commitEvent = new CommitEvent("S1", 1, true);
        assertEquals(commitEvent.type, RaftEventType.COMMIT);

        ElectionEvent electionEvent = new ElectionEvent("S1", "S2", 0);
        assertEquals(electionEvent.type, RaftEventType.ELECTION);

        SnapshotRestoredEvent snapshotRestoredEvent = new SnapshotRestoredEvent("S1", Snapshot.getDefaultInstance());
        assertEquals(snapshotRestoredEvent.type, RaftEventType.SNAPSHOT_RESTORED);

        StatusChangedEvent statusChangedEvent = new StatusChangedEvent("S1", RaftNodeStatus.Leader);
        assertEquals(statusChangedEvent.type, RaftEventType.STATUS_CHANGED);

        SyncStateChangedEvent syncStateChangedEvent = new SyncStateChangedEvent("S1", Collections.emptyMap());
        assertEquals(syncStateChangedEvent.type, RaftEventType.SYNC_STATE_CHANGED);
    }

    @Test
    public void equals() {
        assertEquals(new CommitEvent("S1", 1, true), new CommitEvent("S1", 1, true));
        assertEquals(new ElectionEvent("S1", "S2", 0), new ElectionEvent("S1", "S2", 0));
        assertEquals(new SnapshotRestoredEvent("S1", Snapshot.getDefaultInstance()),
            new SnapshotRestoredEvent("S1", Snapshot.getDefaultInstance()));
        assertEquals(new StatusChangedEvent("S1", RaftNodeStatus.Leader),
            new StatusChangedEvent("S1", RaftNodeStatus.Leader));
        assertEquals(new SyncStateChangedEvent("S1", Collections.emptyMap()),
            new SyncStateChangedEvent("S1", Collections.emptyMap()));
    }
}
