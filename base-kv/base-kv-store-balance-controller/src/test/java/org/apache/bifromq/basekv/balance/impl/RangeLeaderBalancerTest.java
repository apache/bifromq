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

package org.apache.bifromq.basekv.balance.impl;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.balance.BalanceNow;
import org.apache.bifromq.basekv.balance.BalanceResult;
import org.apache.bifromq.basekv.balance.BalanceResultType;
import org.apache.bifromq.basekv.balance.command.BalanceCommand;
import org.apache.bifromq.basekv.balance.command.TransferLeadershipCommand;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.google.protobuf.ByteString;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RangeLeaderBalancerTest {
    private final String clusterId = "testCluster";
    private final String localStoreId = "localStore";
    private final String otherStoreId = "otherStore";
    private RangeLeaderBalancer balancer;

    @BeforeMethod
    public void setUp() {
        balancer = new RangeLeaderBalancer(clusterId, localStoreId);
    }

    @Test
    public void noEffectiveRouteNoBalanceNeeded() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(
                Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a")).setEndKey(ByteString.copyFromUtf8("z"))
                    .build())
            .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor)
            .build();

        balancer.update(Set.of(storeDescriptor));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void hasEffectiveRouteSingleStoreNoBalanceNeeded() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).build())
            .build();
        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor)
            .build();
        balancer.update(Set.of(storeDescriptor));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void hasEffectiveRouteMultiStoreNoBalanceNeeded() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        KVRangeId kvRangeId3 = KVRangeId.newBuilder().setEpoch(1).setId(3).build();
        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(KVRangeDescriptor.newBuilder()
                .setId(kvRangeId1)
                .setRole(RaftNodeStatus.Leader)
                .setBoundary(FULL_BOUNDARY)
                .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).build())
                .build())
            .build();
        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("remoteStore1")
            .addRanges(KVRangeDescriptor.newBuilder()
                .setId(kvRangeId2)
                .setRole(RaftNodeStatus.Leader)
                .setBoundary(FULL_BOUNDARY)
                .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).build())
                .build())
            .build();
        KVRangeStoreDescriptor storeDescriptor3 = KVRangeStoreDescriptor.newBuilder()
            .setId("remoteStore2")
            .addRanges(KVRangeDescriptor.newBuilder()
                .setId(kvRangeId3)
                .setRole(RaftNodeStatus.Leader)
                .setBoundary(FULL_BOUNDARY)
                .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).build())
                .build())
            .build();
        balancer.update(Set.of(storeDescriptor1, storeDescriptor2, storeDescriptor3));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void balanceToOtherNoLeaderStore() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        // KVRange1: [null, z)
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(toBoundary(null, ByteString.copyFromUtf8("z")))
            .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).addLearners("otherStore").build())
            .build();
        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        // KVRange2: [z, null)
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(toBoundary(ByteString.copyFromUtf8("z"), null))
            .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).addVoters("otherStore").build())
            .build();
        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor1)
            .addRanges(kvRangeDescriptor2)
            .build();
        KVRangeId kvRangeId3 = KVRangeId.newBuilder().setEpoch(1).setId(10).build();
        // KVRange3: [z, null)
        KVRangeDescriptor kvRangeDescriptor3 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId3)
            .setRole(RaftNodeStatus.Follower)
            .setBoundary(toBoundary(ByteString.copyFromUtf8("z"), null))
            .setConfig(ClusterConfig.newBuilder().addVoters("otherStore").build())
            .build();
        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("otherStore")
            .addRanges(kvRangeDescriptor3)
            .build();
        balancer.update(Set.of(storeDescriptor1, storeDescriptor2));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.BalanceNow);
        BalanceCommand command = ((BalanceNow<?>) result).command;
        assertTrue(command instanceof TransferLeadershipCommand);
        assertEquals(command.getKvRangeId(), kvRangeId2);
        assertEquals(((TransferLeadershipCommand) command).getNewLeaderStore(), "otherStore");
        assertEquals(((TransferLeadershipCommand) command).getExpectedVer(), 1);
    }

    @Test
    public void transferLeadership() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).addLearners("otherStore").build())
            .build();
        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).addVoters("otherStore").build())
            .build();
        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor1)
            .addRanges(kvRangeDescriptor2)
            .build();
        KVRangeId kvRangeId3 = KVRangeId.newBuilder().setEpoch(1).setId(10).build();
        KVRangeDescriptor kvRangeDescriptor3 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId3)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters("otherStore").build())
            .build();
        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("otherStore")
            .addRanges(kvRangeDescriptor3)
            .build();
        balancer.update(Set.of(storeDescriptor1, storeDescriptor2));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.BalanceNow);
        BalanceCommand command = ((BalanceNow<?>) result).command;
        assertTrue(command instanceof TransferLeadershipCommand);
        assertEquals(command.getKvRangeId(), kvRangeId2);
        assertEquals(((TransferLeadershipCommand) command).getNewLeaderStore(), "otherStore");
        assertEquals(((TransferLeadershipCommand) command).getExpectedVer(), 1);
    }

    @Test
    public void skipTransferLeadershipInOtherStore() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters(otherStoreId).addLearners("otherStore").build())
            .build();
        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters(otherStoreId).addVoters("otherStore").build())
            .build();
        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(otherStoreId)
            .addRanges(kvRangeDescriptor1)
            .addRanges(kvRangeDescriptor2)
            .build();
        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .build();
        balancer.update(Set.of(storeDescriptor1, storeDescriptor2));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.NoNeedBalance);
    }
}