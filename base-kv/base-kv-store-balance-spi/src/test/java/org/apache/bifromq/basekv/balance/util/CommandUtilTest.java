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

package org.apache.bifromq.basekv.balance.util;

import static org.apache.bifromq.basekv.balance.util.CommandUtil.diffBy;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.balance.BalanceNow;
import org.apache.bifromq.basekv.balance.BalanceResult;
import org.apache.bifromq.basekv.balance.BalanceResultType;
import org.apache.bifromq.basekv.balance.command.BalanceCommand;
import org.apache.bifromq.basekv.balance.command.BootstrapCommand;
import org.apache.bifromq.basekv.balance.command.ChangeConfigCommand;
import org.apache.bifromq.basekv.balance.command.MergeCommand;
import org.apache.bifromq.basekv.balance.command.SplitCommand;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.basekv.utils.EffectiveRoute;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.basekv.utils.LeaderRange;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import org.testng.annotations.Test;

public class CommandUtilTest {
    @Test
    public void quitWithMultipleVoters() {
        String localStoreId = "localStore";
        KVRangeId kvRangeId = KVRangeId.newBuilder().setId(1).setEpoch(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setVer(1)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .addVoters("otherStore")
                .build())
            .build();

        BalanceResult result = CommandUtil.quit(localStoreId, kvRangeDescriptor);

        assertEquals(result.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;

        assertEquals(command.getToStore(), localStoreId);
        assertEquals(command.getKvRangeId(), kvRangeId);
        assertEquals(command.getVoters(), Set.of(localStoreId));
        assertEquals(command.getLearners(), Collections.emptySet());
    }

    @Test
    public void quitWithLearners() {
        String localStoreId = "localStore";
        KVRangeId kvRangeId = KVRangeId.newBuilder().setId(1).setEpoch(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setVer(1)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .addLearners("learnerStore")
                .build())
            .build();

        BalanceResult result = CommandUtil.quit(localStoreId, kvRangeDescriptor);

        assertEquals(result.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;

        assertEquals(command.getToStore(), localStoreId);
        assertEquals(command.getKvRangeId(), kvRangeId);
        assertEquals(command.getVoters(), Set.of(localStoreId));
        assertEquals(command.getLearners(), Collections.emptySet());
    }

    @Test
    public void quitWithSingleVoterNoLearners() {
        String localStoreId = "localStore";
        KVRangeId kvRangeId = KVRangeId.newBuilder().setId(1).setEpoch(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setVer(1)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .build())
            .build();

        BalanceResult result = CommandUtil.quit(localStoreId, kvRangeDescriptor);

        assertEquals(result.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;

        assertEquals(command.getToStore(), localStoreId);
        assertEquals(command.getKvRangeId(), kvRangeId);
        assertEquals(command.getVoters(), Collections.emptySet());
        assertEquals(command.getLearners(), Collections.emptySet());
    }

    @Test
    public void noDiff() {
        NavigableMap<Boundary, ClusterConfig> expected = new TreeMap<>(BoundaryUtil::compare);
        expected.put(boundary(null, "a"), ClusterConfig.newBuilder().addVoters("voter1").build());
        expected.put(boundary("a", null), ClusterConfig.newBuilder().addVoters("voter1").build());

        NavigableMap<Boundary, LeaderRange> current = new TreeMap<>(BoundaryUtil::compare);
        KVRangeId id = KVRangeIdUtil.generate();
        KVRangeId id1 = KVRangeIdUtil.next(id);
        current.put(boundary(null, "a"), new LeaderRange(KVRangeDescriptor
            .newBuilder()
            .setId(id)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().setCorrelateId("c1").addVoters("voter1").build())
            .build(),
            KVRangeStoreDescriptor.newBuilder().setId("voter1").build()));
        current.put(boundary("a", null), new LeaderRange(KVRangeDescriptor
            .newBuilder()
            .setId(id1)
            .setConfig(ClusterConfig.newBuilder().setCorrelateId("c1").addVoters("voter1").build())
            .build(),
            KVRangeStoreDescriptor.newBuilder().setId("voter1").build()
        ));
        assertNull(diffBy(expected, new EffectiveRoute(id.getEpoch(), current)));
    }

    @Test
    public void diffByNull() {
        NavigableMap<Boundary, ClusterConfig> expected = new TreeMap<>(BoundaryUtil::compare);
        expected.put(boundary(null, "a"),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());
        expected.put(boundary("a", null), ClusterConfig.newBuilder().addVoters("voter1").build());

        NavigableMap<Boundary, LeaderRange> current = new TreeMap<>(BoundaryUtil::compare);
        KVRangeId id = KVRangeIdUtil.generate();
        KVRangeId id1 = KVRangeIdUtil.next(id);
        current.put(boundary(null, "a"), new LeaderRange(KVRangeDescriptor
            .newBuilder()
            .setId(id)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter2").addNextVoters("voter3").build())
            .build(), KVRangeStoreDescriptor.newBuilder().setId("voter1").build()));
        current.put(boundary("a", null), new LeaderRange(KVRangeDescriptor
            .newBuilder()
            .setId(id1)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter2").build())
            .build(), KVRangeStoreDescriptor.newBuilder().setId("voter1").build()));
        assertNull(diffBy(expected, new EffectiveRoute(id.getEpoch(), current)));
    }

    @Test
    public void diffByConfigChange() {
        NavigableMap<Boundary, ClusterConfig> expected = new TreeMap<>(BoundaryUtil::compare);
        expected.put(boundary(null, "a"),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());
        expected.put(boundary("a", null), ClusterConfig.newBuilder().addVoters("voter1").build());

        NavigableMap<Boundary, LeaderRange> current = new TreeMap<>(BoundaryUtil::compare);
        KVRangeId id = KVRangeIdUtil.generate();
        KVRangeId id1 = KVRangeIdUtil.next(id);
        current.put(boundary(null, "a"), new LeaderRange(KVRangeDescriptor
            .newBuilder()
            .setId(id)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter2").build())
            .build(), KVRangeStoreDescriptor.newBuilder().setId("voter1").build()));
        current.put(boundary("a", null), new LeaderRange(KVRangeDescriptor
            .newBuilder()
            .setId(id1)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter2").build())
            .build(), KVRangeStoreDescriptor.newBuilder().setId("voter1").build()));
        ChangeConfigCommand command = (ChangeConfigCommand) diffBy(expected,
            new EffectiveRoute(id.getEpoch(), current));
        assertNotNull(command);
        assertEquals(command.getToStore(), "voter1");
        assertEquals(command.getKvRangeId(), id);
        assertEquals(command.getExpectedVer(), 1L);
        assertEquals(command.getVoters(), Set.of("voter1"));
        assertEquals(command.getLearners(), Set.of("learner1"));
    }

    @Test
    public void diffBySplit() {
        NavigableMap<Boundary, ClusterConfig> expected = new TreeMap<>(BoundaryUtil::compare);
        expected.put(boundary(null, "a"),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());
        expected.put(boundary("a", "b"),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());
        expected.put(boundary("b", null),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());

        NavigableMap<Boundary, LeaderRange> current = new TreeMap<>(BoundaryUtil::compare);
        KVRangeId id = KVRangeIdUtil.generate();
        KVRangeId id1 = KVRangeIdUtil.next(id);
        current.put(boundary(null, "b"), new LeaderRange(KVRangeDescriptor
            .newBuilder()
            .setId(id)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build(), KVRangeStoreDescriptor.newBuilder().setId("voter1").build()));
        current.put(boundary("b", null), new LeaderRange(KVRangeDescriptor
            .newBuilder()
            .setId(id1)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build(), KVRangeStoreDescriptor.newBuilder().setId("voter1").build()));
        SplitCommand command = (SplitCommand) diffBy(expected, new EffectiveRoute(id.getEpoch(), current));
        assertNotNull(command);
        assertEquals(command.getToStore(), "voter1");
        assertEquals(command.getKvRangeId(), id);
        assertEquals(command.getExpectedVer(), 1L);
        assertEquals(command.getSplitKey(), ByteString.copyFromUtf8("a"));
    }

    @Test
    public void diffByConfigChangeDuringMerging() {
        NavigableMap<Boundary, ClusterConfig> expected = new TreeMap<>(BoundaryUtil::compare);
        expected.put(boundary(null, "b"),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());
        expected.put(boundary("b", null),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());

        NavigableMap<Boundary, LeaderRange> current = new TreeMap<>(BoundaryUtil::compare);
        KVRangeId id = KVRangeIdUtil.generate();
        KVRangeId id1 = KVRangeIdUtil.next(id);
        KVRangeId id2 = KVRangeIdUtil.next(id1);
        current.put(boundary(null, "a"), new LeaderRange(KVRangeDescriptor
            .newBuilder()
            .setId(id)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build(), KVRangeStoreDescriptor.newBuilder().setId("voter1").build()));
        current.put(boundary("a", "b"), new LeaderRange(KVRangeDescriptor
            .newBuilder()
            .setId(id1)
            .setVer(2L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").build())
            .build(), KVRangeStoreDescriptor.newBuilder().setId("voter1").build()));
        current.put(boundary("b", null), new LeaderRange(KVRangeDescriptor
            .newBuilder()
            .setId(id2)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build(), KVRangeStoreDescriptor.newBuilder().setId("voter1").build()));
        ChangeConfigCommand command = (ChangeConfigCommand) diffBy(expected,
            new EffectiveRoute(id.getEpoch(), current));
        assertNotNull(command);
        assertEquals(command.getToStore(), "voter1");
        assertEquals(command.getKvRangeId(), id1);
        assertEquals(command.getExpectedVer(), 2L);
        assertEquals(command.getVoters(), Set.of("voter1"));
        assertEquals(command.getLearners(), Set.of("learner1"));
    }

    @Test
    public void diffByMergeDuringMerging() {
        NavigableMap<Boundary, ClusterConfig> expected = new TreeMap<>(BoundaryUtil::compare);
        expected.put(boundary(null, "b"),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());
        expected.put(boundary("b", null),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());

        NavigableMap<Boundary, LeaderRange> current = new TreeMap<>(BoundaryUtil::compare);
        KVRangeId id = KVRangeIdUtil.generate();
        KVRangeId id1 = KVRangeIdUtil.next(id);
        KVRangeId id2 = KVRangeIdUtil.next(id1);
        current.put(boundary(null, "a"), new LeaderRange(KVRangeDescriptor
            .newBuilder()
            .setId(id)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build(), KVRangeStoreDescriptor.newBuilder().setId("voter1").build()));
        current.put(boundary("a", "b"), new LeaderRange(KVRangeDescriptor
            .newBuilder()
            .setId(id1)
            .setVer(2L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build(), KVRangeStoreDescriptor.newBuilder().setId("voter1").build()));
        current.put(boundary("b", null), new LeaderRange(KVRangeDescriptor
            .newBuilder()
            .setId(id2)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build(), KVRangeStoreDescriptor.newBuilder().setId("voter1").build()));
        MergeCommand command = (MergeCommand) diffBy(expected, new EffectiveRoute(id.getEpoch(), current));
        assertNotNull(command);
        assertEquals(command.getToStore(), "voter1");
        assertEquals(command.getKvRangeId(), id);
        assertEquals(command.getExpectedVer(), 1L);
        assertEquals(command.getMergeeId(), id1);
    }

    @Test
    public void diffByNullDuringMerging() {
        NavigableMap<Boundary, ClusterConfig> expected = new TreeMap<>(BoundaryUtil::compare);
        expected.put(boundary(null, "b"),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());
        expected.put(boundary("b", null),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());

        NavigableMap<Boundary, LeaderRange> current = new TreeMap<>(BoundaryUtil::compare);
        KVRangeId id = KVRangeIdUtil.generate();
        KVRangeId id1 = KVRangeIdUtil.next(id);
        KVRangeId id2 = KVRangeIdUtil.next(id1);
        current.put(boundary(null, "a"), new LeaderRange(KVRangeDescriptor
            .newBuilder()
            .setId(id)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build(), KVRangeStoreDescriptor.newBuilder().setId("voter1").build()));
        current.put(boundary("a", "b"), new LeaderRange(KVRangeDescriptor
            .newBuilder()
            .setId(id1)
            .setVer(2L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addNextVoters("voter2").build())
            .build(), KVRangeStoreDescriptor.newBuilder().setId("voter1").build()));
        current.put(boundary("b", null), new LeaderRange(KVRangeDescriptor
            .newBuilder()
            .setId(id2)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build(), KVRangeStoreDescriptor.newBuilder().setId("voter1").build()));
        assertNull(diffBy(expected, new EffectiveRoute(id.getEpoch(), current)));
    }

    @Test
    public void diffByBootstrapAtBeginning() {
        // expected layout: [null, "a") and ["a", null)
        NavigableMap<Boundary, ClusterConfig> expected = new TreeMap<>(BoundaryUtil::compare);
        expected.put(boundary(null, "a"), ClusterConfig.newBuilder().addVoters("store1").build());
        expected.put(boundary("a", null), ClusterConfig.newBuilder().addVoters("store1").build());

        // effectiveRoute has only ["b", null)
        NavigableMap<Boundary, LeaderRange> current = new TreeMap<>(BoundaryUtil::compare);
        KVRangeId id = KVRangeIdUtil.generate();
        current.put(boundary("b", null), new LeaderRange(
            KVRangeDescriptor.newBuilder()
                .setId(id)
                .setVer(1L)
                .setConfig(ClusterConfig.newBuilder().addVoters("store1").build())
                .build(),
            KVRangeStoreDescriptor.newBuilder().setId("store1").build()
        ));

        BalanceCommand command = CommandUtil.diffBy(expected, new EffectiveRoute(id.getEpoch(), current));
        assertNotNull(command);
        // Expect the bootstrap command to fill the gap
        assertTrue(command instanceof BootstrapCommand);
        BootstrapCommand bootstrap = (BootstrapCommand) command;
        assertEquals(bootstrap.getBoundary(), boundary(null, "b"));
        assertNotNull(bootstrap.getKvRangeId());
        assertEquals(bootstrap.getToStore(), "store1");
    }

    @Test
    public void diffByBootstrapAtEnd() {
        // expected layout: [null, "a"), ["a", "b"), ["b", null)
        NavigableMap<Boundary, ClusterConfig> expected = new TreeMap<>(BoundaryUtil::compare);
        expected.put(boundary(null, "a"), ClusterConfig.newBuilder().addVoters("store1").build());
        expected.put(boundary("a", "b"), ClusterConfig.newBuilder().addVoters("store1").build());
        expected.put(boundary("b", null), ClusterConfig.newBuilder().addVoters("store1").build());

        // effectiveRoute missing ["b", null)
        NavigableMap<Boundary, LeaderRange> current = new TreeMap<>(BoundaryUtil::compare);
        KVRangeId id = KVRangeIdUtil.generate();
        KVRangeId id1 = KVRangeIdUtil.next(id);
        current.put(boundary(null, "a"), new LeaderRange(KVRangeDescriptor.newBuilder()
            .setId(id)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().addVoters("store1").build())
            .build(),
            KVRangeStoreDescriptor.newBuilder().setId("store1").build()));
        current.put(boundary("a", "b"), new LeaderRange(KVRangeDescriptor.newBuilder()
            .setId(id1)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().addVoters("store1").build())
            .build(),
            KVRangeStoreDescriptor.newBuilder().setId("store1").build()));

        BalanceCommand command = CommandUtil.diffBy(expected, new EffectiveRoute(id.getEpoch(), current));
        assertNotNull(command);
        assertTrue(command instanceof BootstrapCommand);
        BootstrapCommand bootstrap = (BootstrapCommand) command;
        assertEquals(bootstrap.getBoundary(), boundary("b", null));
        assertNotNull(bootstrap.getKvRangeId());
        assertEquals(bootstrap.getToStore(), "store1");
    }

    @Test
    public void diffByBootstrapInMiddle() {
        // expected layout : [null, "a"), ["a", "b"), ["b", "c"), ["c", null)
        NavigableMap<Boundary, ClusterConfig> expected = new TreeMap<>(BoundaryUtil::compare);
        expected.put(boundary(null, "a"), ClusterConfig.newBuilder().addVoters("store1").build());
        expected.put(boundary("a", "b"), ClusterConfig.newBuilder().addVoters("store1").build());
        expected.put(boundary("b", "c"), ClusterConfig.newBuilder().addVoters("store1").build());
        expected.put(boundary("c", null), ClusterConfig.newBuilder().addVoters("store1").build());

        // effectiveRoute missing ["a", "c")
        NavigableMap<Boundary, LeaderRange> current = new TreeMap<>(BoundaryUtil::compare);
        KVRangeId id = KVRangeIdUtil.generate();
        KVRangeId id1 = KVRangeIdUtil.next(id);
        current.put(boundary(null, "a"), new LeaderRange(KVRangeDescriptor.newBuilder()
            .setId(id)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().addVoters("store1").build())
            .build(),
            KVRangeStoreDescriptor.newBuilder().setId("store1").build()));
        current.put(boundary("c", null), new LeaderRange(KVRangeDescriptor.newBuilder()
            .setId(id1)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().addVoters("store1").build())
            .build(),
            KVRangeStoreDescriptor.newBuilder().setId("store1").build()));

        BalanceCommand command = CommandUtil.diffBy(expected, new EffectiveRoute(id.getEpoch(), current));
        assertNotNull(command);
        assertTrue(command instanceof BootstrapCommand);
        BootstrapCommand bootstrap = (BootstrapCommand) command;
        assertEquals(bootstrap.getBoundary(), toBoundary(copyFromUtf8("a"), copyFromUtf8("c")));
        assertNotNull(bootstrap.getKvRangeId());
        assertEquals(bootstrap.getToStore(), "store1");
    }

    private Boundary boundary(String startKey, String endKey) {
        Boundary.Builder builder = Boundary.newBuilder();
        if (startKey != null) {
            builder.setStartKey(copyFromUtf8(startKey));
        }
        if (endKey != null) {
            builder.setEndKey(copyFromUtf8(endKey));
        }
        return builder.build();
    }
}
