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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.util.Set;
import org.apache.bifromq.basekv.balance.BalanceNow;
import org.apache.bifromq.basekv.balance.BalanceResultType;
import org.apache.bifromq.basekv.balance.command.SplitCommand;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.proto.SplitHint;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RangeSplitBalancerTest {
    private static final String HintType = "kv_io_mutation";
    private final String clusterId = "clusterId";
    private KVRangeDescriptor.Builder rangeDescriptorBuilder;
    private KVRangeStoreDescriptor.Builder storeDescriptorBuilder;

    @BeforeMethod
    public void setup() {
        rangeDescriptorBuilder = KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0L)
            .setBoundary(FULL_BOUNDARY)
            .setState(State.StateType.Normal)
            .setRole(RaftNodeStatus.Leader)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("store1")
                .build());
        storeDescriptorBuilder = KVRangeStoreDescriptor
            .newBuilder()
            .setId("store1");
    }

    @Test
    public void defaultLoadRules() {
        RangeSplitBalancer balancer = new RangeSplitBalancer(clusterId, "store1", HintType, 30, 0.8, 30, 30_000);
        assertTrue(balancer.validate(balancer.initialLoadRules()));
    }

    @Test
    public void noSplitHint() {
        RangeSplitBalancer balancer = new RangeSplitBalancer(clusterId, "store1", HintType, 30, 0.8, 30, 30_000);
        balancer.update(Set.of(storeDescriptorBuilder
            .addRanges(rangeDescriptorBuilder.build())
            .build()));
        assertEquals(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void genSplitCommand() {
        RangeSplitBalancer balancer = new RangeSplitBalancer(clusterId, "store1", HintType, 30, 0.8, 30, 30_000);
        balancer.update(Set.of(storeDescriptorBuilder
            .addRanges(rangeDescriptorBuilder
                .addHints(SplitHint.newBuilder()
                    .setType(HintType)
                    .putLoad("ioDensity", 40.0)
                    .putLoad("ioLatencyNanos", 100)
                    .setSplitKey(ByteString.copyFromUtf8("a"))
                    .build())
                .build())
            .putStatistics("cpu.usage", 0.7)
            .build()));
        SplitCommand splitCommand = ((BalanceNow<SplitCommand>) balancer.balance()).command;
        assertEquals(splitCommand.getToStore(), "store1");
        assertEquals(splitCommand.getKvRangeId(), rangeDescriptorBuilder.getId());
        assertEquals(splitCommand.getExpectedVer(), rangeDescriptorBuilder.getVer());
        assertEquals(splitCommand.getSplitKey(), ByteString.copyFromUtf8("a"));
    }

    @Test
    public void stopSplitWhenCPUOverUse() {
        RangeSplitBalancer balancer = new RangeSplitBalancer(clusterId, "store1", HintType, 30, 0.8, 30, 30_000);
        balancer.update(Set.of(storeDescriptorBuilder
            .addRanges(rangeDescriptorBuilder
                .addHints(SplitHint.newBuilder()
                    .setType(HintType)
                    .putLoad("ioDensity", 1.0)
                    .putLoad("ioLatencyNanos", 100)
                    .setSplitKey(ByteString.copyFromUtf8("a"))
                    .build())
                .build())
            .putStatistics("cpu.usage", 0.9)
            .build()));
        assertEquals(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void stopSplitWhenIODensityUnderLimit() {
        RangeSplitBalancer balancer = new RangeSplitBalancer(clusterId, "store1", HintType, 30, 0.8, 30, 30_000);
        balancer.update(Set.of(storeDescriptorBuilder
            .addRanges(rangeDescriptorBuilder
                .addHints(SplitHint.newBuilder()
                    .setType(HintType)
                    .putLoad("ioDensity", 20)
                    .putLoad("ioLatencyNanos", 100)
                    .setSplitKey(ByteString.copyFromUtf8("a"))
                    .build())
                .build())
            .putStatistics("cpu.usage", 0.7)
            .build()));
        assertEquals(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void stopSplitWhenIOLatencyExceedLimit() {
        RangeSplitBalancer balancer = new RangeSplitBalancer(clusterId, "store1", HintType, 30, 0.8, 30, 30_000);
        balancer.update(Set.of(storeDescriptorBuilder
            .addRanges(rangeDescriptorBuilder
                .addHints(SplitHint.newBuilder()
                    .setType(HintType)
                    .putLoad("ioDensity", 40)
                    .putLoad("ioLatencyNanos", 40_000)
                    .setSplitKey(ByteString.copyFromUtf8("a"))
                    .build())
                .build())
            .putStatistics("cpu.usage", 0.7)
            .build()));
        assertEquals(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void stopSplitWhenExceedMaxRanges() {
        RangeSplitBalancer balancer = new RangeSplitBalancer(clusterId, "store1", HintType, 1, 0.8, 30, 30_000);
        balancer.update(Set.of(storeDescriptorBuilder
            .addRanges(rangeDescriptorBuilder
                .addHints(SplitHint.newBuilder()
                    .setType(HintType)
                    .putLoad("ioDensity", 40)
                    .putLoad("ioLatencyNanos", 20_000)
                    .setSplitKey(ByteString.copyFromUtf8("a"))
                    .build())
                .build())
            .putStatistics("cpu.usage", 0.7)
            .build()));
        assertEquals(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }
}
