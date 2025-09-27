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
import static org.testng.Assert.assertTrue;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
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
        KVRangeId rangeId = KVRangeIdUtil.generate();
        Map<String, KVRangeDescriptor> replicas = buildReplicas(rangeId, FULL_BOUNDARY,
            new String[]{localVoter, remoteVoter1, remoteVoter2, remoteLearner1, remoteLearner2}, new HashSet<>());
        // leader descriptor carries config and sync state
        KVRangeDescriptor leaderDesc = KVRangeDescriptor.newBuilder()
            .setId(rangeId).setRole(RaftNodeStatus.Leader).setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .putSyncState(localVoter, RaftNodeSyncState.Replicating)
            .putSyncState(remoteVoter1, RaftNodeSyncState.Replicating)
            .putSyncState(remoteVoter2, RaftNodeSyncState.Replicating)
            .putSyncState(remoteLearner1, RaftNodeSyncState.Replicating)
            .putSyncState(remoteLearner2, RaftNodeSyncState.Replicating)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localVoter).addVoters(remoteVoter1).addVoters(remoteVoter2)
                .addLearners(remoteLearner1).addLearners(remoteLearner2)
                .build())
            .setReadyForQuery(true)
            .build();
        replicas.put(remoteVoter1, leaderDesc);
        KVRangeSetting setting = new KVRangeSetting(clusterId, remoteVoter1, replicas);

        assertEquals(setting.randomReplicaForQuery().orElseThrow(), localVoter);
        assertEquals(setting.inProcQueryReadyReplica().orElseThrow(), localVoter);
    }

    @Test
    public void excludeResettingReplicas() {
        KVRangeId rangeId = KVRangeIdUtil.generate();
        // mark remoteVoter1 and remoteLearner1 as resetting
        Set<String> resetting = new HashSet<>();
        resetting.add(remoteVoter1);
        resetting.add(remoteLearner1);
        Map<String, KVRangeDescriptor> replicas = buildReplicas(rangeId, FULL_BOUNDARY,
            new String[]{localVoter, remoteVoter1, remoteLearner1}, resetting);
        KVRangeDescriptor leaderDesc = KVRangeDescriptor.newBuilder()
            .setId(rangeId).setRole(RaftNodeStatus.Leader).setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .putSyncState(localVoter, RaftNodeSyncState.Replicating)
            .putSyncState(remoteVoter1, RaftNodeSyncState.Replicating)
            .putSyncState(remoteLearner1, RaftNodeSyncState.Replicating)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localVoter).addVoters(remoteVoter1)
                .addLearners(remoteLearner1)
                .build())
            .setReadyForQuery(true)
            .build();
        replicas.put(localVoter, leaderDesc);
        KVRangeSetting setting = new KVRangeSetting(clusterId, localVoter, replicas);

        Set<String> seen = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            seen.add(setting.getQueryReadyReplica(i).orElseThrow());
        }
        assertTrue(seen.contains(localVoter));
        assertFalse(seen.contains(remoteVoter1));
        assertFalse(seen.contains(remoteLearner1));
    }

    @Test
    public void inProcOnlyResetting() {
        regInProcStore(clusterId, localVoter);
        KVRangeId rangeId = KVRangeIdUtil.generate();
        Set<String> resetting = new HashSet<>();
        resetting.add(localVoter);
        Map<String, KVRangeDescriptor> replicas = buildReplicas(rangeId, FULL_BOUNDARY,
            new String[]{localVoter, remoteVoter1}, resetting);
        KVRangeDescriptor leaderDesc = KVRangeDescriptor.newBuilder()
            .setId(rangeId).setRole(RaftNodeStatus.Leader).setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .putSyncState(localVoter, RaftNodeSyncState.Replicating)
            .putSyncState(remoteVoter1, RaftNodeSyncState.Replicating)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localVoter).addVoters(remoteVoter1)
                .build())
            .setReadyForQuery(true)
            .build();
        replicas.put(remoteVoter1, leaderDesc);
        KVRangeSetting setting = new KVRangeSetting(clusterId, remoteVoter1, replicas);

        assertFalse(setting.inProcQueryReadyReplica().isPresent());
        assertEquals(setting.randomReplicaForQuery().orElseThrow(), remoteVoter1);
    }

    @Test
    public void includeNextMembers() {
        KVRangeId rangeId = KVRangeIdUtil.generate();
        Map<String, KVRangeDescriptor> replicas = buildReplicas(rangeId, FULL_BOUNDARY,
            new String[]{localVoter, remoteVoter1, remoteLearner1}, new HashSet<>());
        KVRangeDescriptor leaderDesc = KVRangeDescriptor.newBuilder()
            .setId(rangeId).setRole(RaftNodeStatus.Leader).setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .putSyncState(localVoter, RaftNodeSyncState.Replicating)
            .putSyncState(remoteVoter1, RaftNodeSyncState.Replicating)
            .putSyncState(remoteLearner1, RaftNodeSyncState.Replicating)
            .setConfig(ClusterConfig.newBuilder()
                .addNextVoters(localVoter).addNextVoters(remoteVoter1)
                .addNextLearners(remoteLearner1)
                .build())
            .setReadyForQuery(true)
            .build();
        replicas.put(localVoter, leaderDesc);
        KVRangeSetting setting = new KVRangeSetting(clusterId, localVoter, replicas);

        Set<String> seen = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            seen.add(setting.getQueryReadyReplica(i).orElseThrow());
        }
        assertTrue(seen.contains(localVoter));
        assertTrue(seen.contains(remoteVoter1));
        assertTrue(seen.contains(remoteLearner1));
    }

    @Test
    public void missingReplicaDescriptorExcluded() {
        KVRangeId rangeId = KVRangeIdUtil.generate();
        String missing = "missingStore";
        Map<String, KVRangeDescriptor> replicas = buildReplicas(rangeId, FULL_BOUNDARY,
            new String[]{localVoter}, new HashSet<>()); // intentionally omit 'missing'
        KVRangeDescriptor leaderDesc = KVRangeDescriptor.newBuilder()
            .setId(rangeId).setRole(RaftNodeStatus.Leader).setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .putSyncState(localVoter, RaftNodeSyncState.Replicating)
            .putSyncState(missing, RaftNodeSyncState.Replicating)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localVoter).addVoters(missing)
                .build())
            .setReadyForQuery(true)
            .build();
        replicas.put(localVoter, leaderDesc);
        KVRangeSetting setting = new KVRangeSetting(clusterId, localVoter, replicas);

        Set<String> seen = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            seen.add(setting.getQueryReadyReplica(i).orElseThrow());
        }
        assertTrue(seen.contains(localVoter));
        assertFalse(seen.contains(missing));
    }

    @Test
    public void leaderResettingExcludedEvenIfInProc() {
        regInProcStore(clusterId, localVoter);
        KVRangeId rangeId = KVRangeIdUtil.generate();
        Set<String> resetting = new HashSet<>();
        resetting.add(localVoter);
        Map<String, KVRangeDescriptor> replicas = buildReplicas(rangeId, FULL_BOUNDARY,
            new String[]{localVoter, remoteVoter1}, resetting);
        KVRangeDescriptor leaderDesc = KVRangeDescriptor.newBuilder()
            .setId(rangeId).setRole(RaftNodeStatus.Leader).setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .putSyncState(localVoter, RaftNodeSyncState.Replicating)
            .putSyncState(remoteVoter1, RaftNodeSyncState.Replicating)
            .setReadyForQuery(false)
            .setConfig(ClusterConfig.newBuilder().addVoters(localVoter).addVoters(remoteVoter1).build())
            .build();
        replicas.put(localVoter, leaderDesc);
        KVRangeSetting setting = new KVRangeSetting(clusterId, localVoter, replicas);

        assertFalse(setting.inProcQueryReadyReplica().isPresent());
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            seen.add(setting.getQueryReadyReplica(i).orElseThrow());
        }
        assertFalse(seen.contains(localVoter));
        assertTrue(seen.contains(remoteVoter1));
    }

    @Test
    public void singleReadyReplicaFallback() {
        KVRangeId rangeId = KVRangeIdUtil.generate();
        Map<String, KVRangeDescriptor> replicas = buildReplicas(rangeId, FULL_BOUNDARY,
            new String[]{localVoter, remoteVoter1}, new HashSet<>());
        KVRangeDescriptor leaderDesc = KVRangeDescriptor.newBuilder()
            .setId(rangeId).setRole(RaftNodeStatus.Leader).setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .putSyncState(localVoter, RaftNodeSyncState.Replicating)
            .putSyncState(remoteVoter1, RaftNodeSyncState.Probing)
            .setConfig(ClusterConfig.newBuilder().addVoters(localVoter).addVoters(remoteVoter1).build())
            .setReadyForQuery(true)
            .build();
        replicas.put(localVoter, leaderDesc);
        KVRangeSetting setting = new KVRangeSetting(clusterId, localVoter, replicas);

        assertEquals(setting.randomReplicaForQuery().orElseThrow(), localVoter);
        assertEquals(setting.getQueryReadyReplica(0).orElseThrow(), localVoter);
        assertEquals(setting.getQueryReadyReplica(1).orElseThrow(), localVoter);
    }
    @Test
    public void skipNonReplicating() {
        KVRangeId rangeId = KVRangeIdUtil.generate();
        Map<String, KVRangeDescriptor> replicas = buildReplicas(rangeId, FULL_BOUNDARY,
            new String[]{localVoter, remoteVoter1, remoteVoter2, remoteLearner1, remoteLearner2}, new HashSet<>());
        KVRangeDescriptor leaderDesc = KVRangeDescriptor.newBuilder()
            .setId(rangeId).setRole(RaftNodeStatus.Leader).setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .putSyncState(localVoter, RaftNodeSyncState.Replicating)
            .putSyncState(remoteVoter1, RaftNodeSyncState.Replicating)
            .putSyncState(remoteVoter2, RaftNodeSyncState.Probing)
            .putSyncState(remoteLearner1, RaftNodeSyncState.Replicating)
            .putSyncState(remoteLearner2, RaftNodeSyncState.Probing)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localVoter).addVoters(remoteVoter1).addVoters(remoteVoter2)
                .addLearners(remoteLearner1).addLearners(remoteLearner2)
                .build())
            .setReadyForQuery(true)
            .build();
        replicas.put(localVoter, leaderDesc);
        KVRangeSetting setting = new KVRangeSetting(clusterId, localVoter, replicas);

        // collect a sample of query-ready replicas via round-robin
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            seen.add(setting.getQueryReadyReplica(i).orElseThrow());
        }
        assertFalse(seen.contains(remoteVoter2));
        assertFalse(seen.contains(remoteLearner2));
        assertTrue(seen.contains(localVoter));
        assertTrue(seen.contains(remoteVoter1));
        assertTrue(seen.contains(remoteLearner1));
    }

    @Test
    public void getFact() {
        Boundary fact = toBoundary(ByteString.copyFromUtf8("abc"), ByteString.copyFromUtf8("def"));
        KVRangeId rangeId = KVRangeIdUtil.generate();
        Map<String, KVRangeDescriptor> replicas = buildReplicas(rangeId, FULL_BOUNDARY,
            new String[]{localVoter, remoteVoter1, remoteVoter2, remoteLearner1, remoteLearner2}, new HashSet<>());
        KVRangeDescriptor leaderDesc = KVRangeDescriptor.newBuilder()
            .setId(rangeId).setRole(RaftNodeStatus.Leader).setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .putSyncState(localVoter, RaftNodeSyncState.Replicating)
            .putSyncState(remoteVoter1, RaftNodeSyncState.Replicating)
            .putSyncState(remoteVoter2, RaftNodeSyncState.Probing)
            .putSyncState(remoteLearner1, RaftNodeSyncState.Replicating)
            .putSyncState(remoteLearner2, RaftNodeSyncState.Probing)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localVoter).addVoters(remoteVoter1).addVoters(remoteVoter2)
                .addLearners(remoteLearner1).addLearners(remoteLearner2)
                .build())
            .setFact(Any.pack(fact))
            .build();
        replicas.put(localVoter, leaderDesc);
        KVRangeSetting setting = new KVRangeSetting(clusterId, localVoter, replicas);
        assertEquals(fact, setting.getFact(Boundary.class).get());
    }

    @Test
    public void getFactWithWrongParser() {
        Boundary fact = toBoundary(ByteString.copyFromUtf8("abc"), ByteString.copyFromUtf8("def"));
        KVRangeId rangeId = KVRangeIdUtil.generate();
        Map<String, KVRangeDescriptor> replicas = buildReplicas(rangeId, FULL_BOUNDARY,
            new String[]{localVoter, remoteVoter1, remoteVoter2, remoteLearner1, remoteLearner2}, new HashSet<>());
        KVRangeDescriptor leaderDesc = KVRangeDescriptor.newBuilder()
            .setId(rangeId).setRole(RaftNodeStatus.Leader).setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .putSyncState(localVoter, RaftNodeSyncState.Replicating)
            .putSyncState(remoteVoter1, RaftNodeSyncState.Replicating)
            .putSyncState(remoteVoter2, RaftNodeSyncState.Probing)
            .putSyncState(remoteLearner1, RaftNodeSyncState.Replicating)
            .putSyncState(remoteLearner2, RaftNodeSyncState.Probing)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localVoter).addVoters(remoteVoter1).addVoters(remoteVoter2)
                .addLearners(remoteLearner1).addLearners(remoteLearner2)
                .build())
            .setFact(Any.pack(fact))
            .build();
        replicas.put(localVoter, leaderDesc);
        KVRangeSetting setting = new KVRangeSetting(clusterId, localVoter, replicas);
        assertFalse(setting.getFact(Struct.class).isPresent());
    }

    private Map<String, KVRangeDescriptor> buildReplicas(KVRangeId id, Boundary boundary,
                                                         String[] stores, Set<String> queryReadyStores) {
        Map<String, KVRangeDescriptor> m = new HashMap<>();
        for (String s : stores) {
            boolean readyForQuery = !queryReadyStores.contains(s);
            KVRangeDescriptor desc = KVRangeDescriptor.newBuilder()
                .setId(id)
                .setVer(1)
                .setBoundary(boundary)
                .setReadyForQuery(readyForQuery)
                .build();
            m.put(s, desc);
        }
        return m;
    }
}
