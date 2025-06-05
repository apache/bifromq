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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import org.apache.bifromq.basekv.balance.BalanceNow;
import org.apache.bifromq.basekv.balance.BalanceResult;
import org.apache.bifromq.basekv.balance.BalanceResultType;
import org.apache.bifromq.basekv.balance.command.ChangeConfigCommand;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RedundantRangeRemovalBalancerTest {

    private final String clusterId = "testCluster";
    private final String localStoreId = "localStore";
    private RedundantRangeRemovalBalancer balancer;

    @BeforeMethod
    public void setUp() {
        balancer = new RedundantRangeRemovalBalancer(clusterId, localStoreId);
    }

    @Test
    public void noRedundantEpoch() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);

        balancer.update(storeDescriptors);

        assertSame(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void removeRangeInRedundantEpoch() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("m"))
                .build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .build())
            .build();

        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(2).setId(2).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("n"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("otherStore")
                .addVoters(localStoreId)
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor1)
            .addRanges(kvRangeDescriptor2)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = Set.of(storeDescriptor1);

        balancer.update(storeDescriptors);

        BalanceResult command = balancer.balance();
        assertEquals(command.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand changeConfigCommand = (ChangeConfigCommand) ((BalanceNow<?>) command).command;

        assertEquals(changeConfigCommand.getToStore(), localStoreId);
        assertEquals(changeConfigCommand.getKvRangeId(), kvRangeId2);
        assertEquals(changeConfigCommand.getVoters(), Set.of(localStoreId));
        assertEquals(changeConfigCommand.getLearners(), Collections.emptySet());
    }

    @Test
    public void noLocalLeaderRangeInRedundantEpoch() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("m"))
                .build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .build())
            .build();

        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(2).setId(2).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setRole(RaftNodeStatus.Follower)
            .setVer(1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("n"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor1)
            .addRanges(kvRangeDescriptor2)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = Set.of(storeDescriptor1);

        balancer.update(storeDescriptors);

        assertSame(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void removeRedundantEffectiveRange() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        Boundary boundary = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("z"))
            .build();
        ClusterConfig config = ClusterConfig.newBuilder().addVoters(localStoreId).build();

        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(boundary)
            .setConfig(config)
            .build();
        KVRangeDescriptor range2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(boundary)
            .setConfig(config)
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(range1)
            .addRanges(range2)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = Set.of(storeDescriptor);

        balancer.update(storeDescriptors);

        BalanceResult result = balancer.balance();

        assertEquals(result.type(), BalanceResultType.BalanceNow);

        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;
        assertEquals(command.getToStore(), localStoreId);
        assertEquals(command.getKvRangeId(), kvRangeId2);
        assertEquals(command.getVoters(), Collections.emptySet());
        assertEquals(command.getLearners(), Collections.emptySet());
    }

    @Test
    public void ignoreNonLocalStore() {
        String nonLocalStoreId = "otherStore";
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .setConfig(ClusterConfig.newBuilder().addVoters(nonLocalStoreId).build())
            .build();

        KVRangeStoreDescriptor nonLocalStoreDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId(nonLocalStoreId)
            .addRanges(kvRangeDescriptor)
            .build();
        Set<KVRangeStoreDescriptor> landscape = Collections.singleton(nonLocalStoreDescriptor);

        balancer.update(landscape);

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.NoNeedBalance);
    }
}