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

package org.apache.bifromq.basekv.client;

import static org.apache.bifromq.basekv.InProcStores.regInProcStore;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.raft.proto.RaftNodeSyncState;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.testng.annotations.Test;

public class KVRangeSettingTest {
    private final String clusterId = "test_cluster";
    private final String localVoter = "localVoter";
    private final String remoteVoter1 = "remoteVoter1";
    private final String remoteVoter2 = "remoteVoter2";
    private final String remoteLearner1 = "remoteLearner1";
    private final String remoteLearner2 = "remoteLearner2";

    @Test
    public void preferInProc() {
        regInProcStore(clusterId, localVoter);
        KVRangeSetting setting = new KVRangeSetting(clusterId, remoteVoter1,
            KVRangeDescriptor.newBuilder().setId(KVRangeIdUtil.generate()).setRole(RaftNodeStatus.Leader).setVer(1)
                .setBoundary(FULL_BOUNDARY).putSyncState(localVoter, RaftNodeSyncState.Replicating)
                .putSyncState(remoteVoter1, RaftNodeSyncState.Replicating)
                .putSyncState(remoteVoter2, RaftNodeSyncState.Replicating)
                .putSyncState(remoteLearner1, RaftNodeSyncState.Replicating)
                .putSyncState(remoteLearner2, RaftNodeSyncState.Replicating).setConfig(
                    ClusterConfig.newBuilder().addVoters(localVoter).addVoters(remoteVoter1).addVoters(remoteVoter2)
                        .addLearners(remoteLearner1).addLearners(remoteLearner2).build()).build());

        assertEquals(setting.randomReplica(), localVoter);
        assertEquals(setting.randomVoters(), localVoter);
    }

    @Test
    public void skipNonReplicating() {
        KVRangeSetting setting = new KVRangeSetting(clusterId, localVoter,
            KVRangeDescriptor.newBuilder().setId(KVRangeIdUtil.generate()).setRole(RaftNodeStatus.Leader).setVer(1)
                .setBoundary(FULL_BOUNDARY).putSyncState(localVoter, RaftNodeSyncState.Replicating)
                .putSyncState(remoteVoter1, RaftNodeSyncState.Replicating)
                .putSyncState(remoteVoter2, RaftNodeSyncState.Probing)
                .putSyncState(remoteLearner1, RaftNodeSyncState.Replicating)
                .putSyncState(remoteLearner2, RaftNodeSyncState.Probing).setConfig(
                    ClusterConfig.newBuilder().addVoters(localVoter).addVoters(remoteVoter1).addVoters(remoteVoter2)
                        .addLearners(remoteLearner1).addLearners(remoteLearner2).build()).build());
        assertFalse(setting.followers().contains(remoteVoter2));
        assertFalse(setting.allReplicas().contains(remoteLearner2));
    }

    @Test
    public void getFact() {
        Boundary fact = toBoundary(ByteString.copyFromUtf8("abc"), ByteString.copyFromUtf8("def"));
        KVRangeSetting setting = new KVRangeSetting(clusterId, localVoter,
            KVRangeDescriptor.newBuilder().setId(KVRangeIdUtil.generate()).setRole(RaftNodeStatus.Leader).setVer(1)
                .setBoundary(FULL_BOUNDARY).putSyncState(localVoter, RaftNodeSyncState.Replicating)
                .putSyncState(remoteVoter1, RaftNodeSyncState.Replicating)
                .putSyncState(remoteVoter2, RaftNodeSyncState.Probing)
                .putSyncState(remoteLearner1, RaftNodeSyncState.Replicating)
                .putSyncState(remoteLearner2, RaftNodeSyncState.Probing).setConfig(
                    ClusterConfig.newBuilder().addVoters(localVoter).addVoters(remoteVoter1).addVoters(remoteVoter2)
                        .addLearners(remoteLearner1).addLearners(remoteLearner2).build())
                .setFact(Any.pack(fact))
                .build());
        assertEquals(fact, setting.getFact(Boundary.class).get());
    }

    @Test
    public void getFactWithWrongParser() {
        Boundary fact = toBoundary(ByteString.copyFromUtf8("abc"), ByteString.copyFromUtf8("def"));
        KVRangeSetting setting = new KVRangeSetting(clusterId, localVoter,
            KVRangeDescriptor.newBuilder().setId(KVRangeIdUtil.generate()).setRole(RaftNodeStatus.Leader).setVer(1)
                .setBoundary(FULL_BOUNDARY).putSyncState(localVoter, RaftNodeSyncState.Replicating)
                .putSyncState(remoteVoter1, RaftNodeSyncState.Replicating)
                .putSyncState(remoteVoter2, RaftNodeSyncState.Probing)
                .putSyncState(remoteLearner1, RaftNodeSyncState.Replicating)
                .putSyncState(remoteLearner2, RaftNodeSyncState.Probing).setConfig(
                    ClusterConfig.newBuilder().addVoters(localVoter).addVoters(remoteVoter1).addVoters(remoteVoter2)
                        .addLearners(remoteLearner1).addLearners(remoteLearner2).build())
                .setFact(Any.pack(fact))
                .build());
        assertFalse(setting.getFact(Struct.class).isPresent());
    }
}
