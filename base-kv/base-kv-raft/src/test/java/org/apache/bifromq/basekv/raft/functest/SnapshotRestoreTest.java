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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.raft.event.SnapshotRestoredEvent;
import org.apache.bifromq.basekv.raft.exception.ClusterConfigChangeException;
import org.apache.bifromq.basekv.raft.functest.annotation.Cluster;
import org.apache.bifromq.basekv.raft.functest.annotation.Config;
import org.apache.bifromq.basekv.raft.functest.template.SharedRaftConfigTestTemplate;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

public class SnapshotRestoreTest extends SharedRaftConfigTestTemplate {
    @Cluster(v = "V1")
    @Test(groups = "integration")
    public void joinAfterInitialized() {
        String leader = group.currentLeader().get();
        group.compact("V1", ByteString.EMPTY, 1).join();
        group.propose("V1", ByteString.copyFromUtf8("Value1")).join();
        group.awaitIndexCommitted("V1", 2);

        // v2 is initialized with empty cluster config, so it will be restored from v1's snapshot
        group.addRaftNode("V2", 0, 0, ClusterConfig.getDefaultInstance(), raftConfig());
        group.connect("V2");
        Set<String> newVoters = new HashSet<String>() {{
            add("V1");
            add("V2");
        }};
        group.changeClusterConfig(leader, "cId", newVoters, Collections.emptySet()).join();
        for (String peerId : newVoters) {
            assertTrue(group.awaitIndexCommitted(peerId, 3));
        }
        List<SnapshotRestoredEvent> events = group.snapshotRestoredLogs("V2");
        assertEquals(events.size(), 1);
        assertEquals(events.get(0).nodeId, "V2");
        assertTrue(events.get(0).snapshot.getClusterConfig().getVotersList().contains("V1"));
    }

    @Cluster(v = "V1")
    @Test(groups = "integration")
    public void joinTwoNodes() {
        String leader = group.currentLeader().get();
        group.compact("V1", ByteString.EMPTY, 1).join();
        group.propose("V1", ByteString.copyFromUtf8("Value1")).join();
        group.awaitIndexCommitted("V1", 2);

        // V2 is a single node cluster but has less log than V1, so it will be restored from V1's snapshot
        group.addRaftNode("V2", 0, 0, ClusterConfig.newBuilder().addVoters("V2").build(), raftConfig());
        group.connect("V2");
        Set<String> newVoters = new HashSet<>() {{
            add("V1");
            add("V2");
        }};
        group.changeClusterConfig(leader, "cId", newVoters, Collections.emptySet()).join();
        for (String peerId : newVoters) {
            assertTrue(group.awaitIndexCommitted(peerId, 3));
        }
        List<SnapshotRestoredEvent> events = group.snapshotRestoredLogs("V2");
        assertEquals(events.size(), 1);
        assertEquals(events.get(0).nodeId, "V2");
        assertTrue(events.get(0).snapshot.getClusterConfig().getVotersList().contains("V1"));
    }

    @Cluster(v = "V1")
    @Config(installSnapshotTimeoutTick = 5)
    @Test(groups = "integration")
    public void cannotJoinTerminatedNode() {
        String leader = group.currentLeader().get();
        group.compact("V1", ByteString.EMPTY, 1).join();
        group.propose("V1", ByteString.copyFromUtf8("Value1")).join();
        group.awaitIndexCommitted("V1", 2);

        // we shorten the install snapshot timeout to make the test faster
        // V2 is terminated, so change config will fail with slow peer
        group.addRaftNode("V2", 0, 0, ClusterConfig.newBuilder().addVoters("V1").build(), raftConfig());
        group.connect("V2");
        Set<String> newVoters = new HashSet<>() {{
            add("V1");
            add("V2");
        }};
        try {
            group.changeClusterConfig(leader, "cId", newVoters, Collections.emptySet()).join();
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof ClusterConfigChangeException.SlowLearnerException);
        }
    }
}
