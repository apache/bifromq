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

package com.baidu.bifromq.basekv.raft.functest;

import static com.google.protobuf.ByteString.EMPTY;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.baidu.bifromq.basekv.raft.exception.CompactionException;
import com.baidu.bifromq.basekv.raft.functest.annotation.Cluster;
import com.baidu.bifromq.basekv.raft.functest.template.SharedRaftConfigTestTemplate;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

@Slf4j
public class CompactionTest extends SharedRaftConfigTestTemplate {
    @Cluster(v = "V1")
    @Test(groups = "integration")
    public void compact() {
        String leader = group.currentLeader().get();
        assertTrue(group.awaitIndexCommitted(leader, 1));
        long logIndex = group.propose(leader, copyFromUtf8("appCommand1")).join();
        group.compact(leader, EMPTY, logIndex).join();
        assertEquals(group.latestSnapshot(leader), EMPTY);
    }

    @Cluster(v = "V1")
    @Test(groups = "integration")
    public void compactAndReplaceSnapshot() {
        String leader = group.currentLeader().get();
        assertTrue(group.awaitIndexCommitted(leader, 1));
        long logIndex = group.propose(leader, copyFromUtf8("appCommand1")).join();
        group.compact(leader, EMPTY, logIndex).join();
        assertEquals(group.latestSnapshot(leader), EMPTY);

        ByteString fsmSnapshot = ByteString.copyFromUtf8("hello");
        group.compact(leader, fsmSnapshot, logIndex).join();
        assertEquals(group.latestSnapshot(leader), fsmSnapshot);
    }

    @Cluster(v = "V1")
    @Test(groups = "integration")
    public void compactUsingStaleSnapshot() {
        String leader = group.currentLeader().get();
        assertTrue(group.awaitIndexCommitted(leader, 1));
        long logIndex = group.propose(leader, copyFromUtf8("appCommand1")).join();
        group.compact(leader, EMPTY, logIndex).join();
        assertEquals(group.latestSnapshot(leader), EMPTY);

        ByteString fsmSnapshot = ByteString.copyFromUtf8("hello");
        try {
            group.compact(leader, fsmSnapshot, logIndex - 1).join();
            fail();
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof CompactionException.StaleSnapshotException);
        }
    }
}
