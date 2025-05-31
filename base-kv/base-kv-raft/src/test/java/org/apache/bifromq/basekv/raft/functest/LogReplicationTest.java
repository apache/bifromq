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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.bifromq.basekv.raft.functest.annotation.Cluster;
import org.apache.bifromq.basekv.raft.functest.template.SharedRaftConfigTestTemplate;
import org.apache.bifromq.basekv.raft.proto.RaftNodeSyncState;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class LogReplicationTest extends SharedRaftConfigTestTemplate {
    @Test(groups = "integration")
    public void testLogReplicationNormally() {
        String leader = group.currentLeader().get();
        assertTrue(group.awaitIndexCommitted("V1", 1));
        assertTrue(group.awaitIndexCommitted("V2", 1));
        assertTrue(group.awaitIndexCommitted("V3", 1));
        log.info("Leader {} elected", leader);

        group.propose(leader, ByteString.copyFromUtf8("appCommand1"));
        assertTrue(group.awaitIndexCommitted("V1", 2));
        assertTrue(group.awaitIndexCommitted("V2", 2));
        assertTrue(group.awaitIndexCommitted("V3", 2));

        group.propose(leader, ByteString.copyFromUtf8("appCommand2"));
        group.propose(leader, ByteString.copyFromUtf8("appCommand3"));
        assertTrue(group.awaitIndexCommitted("V1", 4));
        assertTrue(group.awaitIndexCommitted("V2", 4));
        assertTrue(group.awaitIndexCommitted("V3", 4));
    }

    @Cluster(l = "L1,L2")
    @Test(groups = "integration")
    public void testLearnerLogReplication() {
        String leader = group.currentLeader().get();
        assertTrue(group.awaitIndexCommitted("V1", 1));
        assertTrue(group.awaitIndexCommitted("V2", 1));
        assertTrue(group.awaitIndexCommitted("V3", 1));
        assertTrue(group.awaitIndexCommitted("L1", 1));
        assertTrue(group.awaitIndexCommitted("L2", 1));
        log.info("Leader {} elected", leader);

        group.propose(leader, ByteString.copyFromUtf8("appCommand1"));
        group.propose(leader, ByteString.copyFromUtf8("appCommand2"));
        assertTrue(group.awaitIndexCommitted("V1", 3));
        assertTrue(group.awaitIndexCommitted("V2", 3));
        assertTrue(group.awaitIndexCommitted("V3", 3));
        assertTrue(group.awaitIndexCommitted("L1", 3));
        assertTrue(group.awaitIndexCommitted("L2", 3));
    }

    @Test(groups = "integration")
    public void testSlowLogReplication() {
        String leader = group.currentLeader().get();

        String slowFollower = group.currentFollowers().get(0);
        String normalFollower = group.currentFollowers().get(1);
        group.cut(leader, slowFollower);

        group.propose(leader, ByteString.copyFromUtf8("appCommand1"));
        group.propose(leader, ByteString.copyFromUtf8("appCommand2"));
        group.propose(leader, ByteString.copyFromUtf8("appCommand3"));
        assertTrue(group.awaitIndexCommitted(leader, 4));
        assertTrue(group.awaitIndexCommitted(normalFollower, 4));

        group.recoverNetwork();
        assertTrue(group.awaitIndexCommitted(slowFollower, 4));
    }

    @Test(groups = "integration")
    public void testSlowLogReplicationAfterCompact() throws InterruptedException {
        String leader = group.currentLeader().get();
        assertTrue(group.awaitIndexCommitted("V1", 1));
        assertTrue(group.awaitIndexCommitted("V2", 1));
        assertTrue(group.awaitIndexCommitted("V3", 1));

        String slowFollower = group.currentFollowers().get(0);
        String normalFollower = group.currentFollowers().get(1);
        log.info("Leader {} elected, slowFollower={}, normalFollower={}", leader, slowFollower, normalFollower);
        group.cut(leader, slowFollower);

        group.propose(leader, ByteString.copyFromUtf8("appCommand1"));
        group.propose(leader, ByteString.copyFromUtf8("appCommand2"));
        group.propose(leader, ByteString.copyFromUtf8("appCommand3"));
        assertTrue(group.awaitIndexCommitted(leader, 4));
        assertTrue(group.awaitIndexCommitted(normalFollower, 4));

        group.compact(leader, ByteString.copyFromUtf8("appSMSnapshot"), 3).join();

        group.recoverNetwork();
        // slowFollower receive snapshot and logEntry of index 4
        assertTrue(group.awaitIndexCommitted(slowFollower, 4));
        List<RaftNodeSyncState> statusLog = group.syncStateLogs(slowFollower);
        long start = System.currentTimeMillis();
        while (statusLog.get(statusLog.size() - 1) != RaftNodeSyncState.Replicating) {
            Thread.sleep(100);
            statusLog = group.syncStateLogs(slowFollower);
            if (System.currentTimeMillis() - start > 10000) {
                fail();
            }
        }
        assertEquals(group.syncStateLogs(slowFollower),
            Arrays.asList(RaftNodeSyncState.Probing, RaftNodeSyncState.Replicating,
                RaftNodeSyncState.SnapshotSyncing, RaftNodeSyncState.Replicating));
    }
}
