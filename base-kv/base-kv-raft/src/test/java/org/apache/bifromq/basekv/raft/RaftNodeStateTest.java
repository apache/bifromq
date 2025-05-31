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

import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Collections;
import org.mockito.Mock;

abstract class RaftNodeStateTest {
    @Mock
    protected IRaftNode.IRaftMessageSender msgSender;
    @Mock
    protected IRaftNode.ISnapshotInstaller snapshotInstaller;
    @Mock
    protected IRaftNode.IRaftEventListener eventListener;
    @Mock
    protected RaftNodeState.OnSnapshotInstalled onSnapshotInstalled;

    @Mock
    protected IRaftStateStore raftStateStorage;

    protected String local = "testLocal";
    protected String testCandidateId = "testCandidateId";
    protected ByteString command = ByteString.copyFromUtf8("command");

    protected RaftConfig defaultRaftConfig = new RaftConfig()
        .setPreVote(true)
        .setDisableForwardProposal(false)
        .setElectionTimeoutTick(5)
        .setHeartbeatTimeoutTick(3)
        .setInstallSnapshotTimeoutTick(5)
        .setMaxUncommittedProposals(4)
        .setMaxInflightAppends(3)
        .setAsyncAppend(false);
    protected ClusterConfig clusterConfig = ClusterConfig.newBuilder()
        .addAllVoters(Arrays.asList(local, "v1", "v2"))
        .addAllLearners(Collections.singleton("l1"))
        .build();
}