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

package org.apache.bifromq.basekv.utils;

import static org.apache.bifromq.basekv.utils.DescriptorUtil.getEffectiveEpoch;
import static org.apache.bifromq.basekv.utils.DescriptorUtil.organizeByEpoch;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class DescriptorUtilTest {

    @Test
    public void organizeByEpochSingleEpoch() {
        // Prepare data with a single epoch
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .putStatistics("stat1", 1.0)
            .addRanges(kvRangeDescriptor)
            .setHlc(12345L)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);

        // Execute the method
        Map<Long, Set<KVRangeStoreDescriptor>> result = organizeByEpoch(storeDescriptors);

        // Validate the result
        assertEquals(result.size(), 1);
        assertTrue(result.containsKey(1L));
        Set<KVRangeStoreDescriptor> epochSet = result.get(1L);
        assertEquals(epochSet.size(), 1);
        KVRangeStoreDescriptor resultDescriptor = epochSet.iterator().next();
        assertEquals(resultDescriptor.getId(), "store1");
        assertEquals(resultDescriptor.getRangesCount(), 1);
        assertEquals(resultDescriptor.getRanges(0).getId(), kvRangeId);
    }

    @Test
    public void organizeByEpochMultipleEpochs() {
        // Prepare data with multiple epochs
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("m"))
                .build())
            .build();

        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(2).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("n"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .putStatistics("stat1", 1.0)
            .addRanges(kvRangeDescriptor1)
            .setHlc(12345L)
            .build();

        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("store2")
            .putStatistics("stat2", 2.0)
            .addRanges(kvRangeDescriptor2)
            .setHlc(67890L)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor1);
        storeDescriptors.add(storeDescriptor2);

        // Execute the method
        Map<Long, Set<KVRangeStoreDescriptor>> result = organizeByEpoch(storeDescriptors);

        // Validate the result
        assertEquals(result.size(), 2);
        assertTrue(result.containsKey(1L));
        assertTrue(result.containsKey(2L));

        // Validate epoch 1
        Set<KVRangeStoreDescriptor> epoch1Set = result.get(1L);
        assertEquals(epoch1Set.size(), 2);

        // Validate epoch 2
        Set<KVRangeStoreDescriptor> epoch2Set = result.get(2L);
        assertEquals(epoch2Set.size(), 2);
    }

    @Test
    public void organizeByEpochMultipleRangesSameStoreDifferentEpochs() {
        // Prepare data with multiple ranges in the same store but different epochs
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("m"))
                .build())
            .build();

        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(2).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("n"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .putStatistics("stat1", 1.0)
            .addRanges(kvRangeDescriptor1)
            .addRanges(kvRangeDescriptor2)
            .setHlc(12345L)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);

        // Execute the method
        Map<Long, Set<KVRangeStoreDescriptor>> result = organizeByEpoch(storeDescriptors);

        // Validate the result
        assertEquals(result.size(), 2);
        assertTrue(result.containsKey(1L));
        assertTrue(result.containsKey(2L));

        // Validate epoch 1
        Set<KVRangeStoreDescriptor> epoch1Set = result.get(1L);
        assertEquals(epoch1Set.size(), 1);
        KVRangeStoreDescriptor resultDescriptor1 = epoch1Set.iterator().next();
        assertEquals(resultDescriptor1.getId(), "store1");
        assertEquals(resultDescriptor1.getRangesCount(), 1);
        assertEquals(resultDescriptor1.getRanges(0).getId(), kvRangeId1);

        // Validate epoch 2
        Set<KVRangeStoreDescriptor> epoch2Set = result.get(2L);
        assertEquals(epoch2Set.size(), 1);
        KVRangeStoreDescriptor resultDescriptor2 = epoch2Set.iterator().next();
        assertEquals(resultDescriptor2.getId(), "store1");
        assertEquals(resultDescriptor2.getRangesCount(), 1);
        assertEquals(resultDescriptor2.getRanges(0).getId(), kvRangeId2);
    }

    @Test
    public void organizeByEpochWithMissingEpoch() {
        // Prepare data with multiple epochs and some missing epochs
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("m"))
                .build())
            .build();

        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(2).setId(2).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("n"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .putStatistics("stat1", 1.0)
            .addRanges(kvRangeDescriptor1)
            .addRanges(kvRangeDescriptor2)
            .setHlc(12345L)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor1);

        // Execute the method
        NavigableMap<Long, Set<KVRangeStoreDescriptor>> result = organizeByEpoch(storeDescriptors);

        // Validate the result
        assertEquals(result.size(), 2);
        assertTrue(result.containsKey(1L));
        assertTrue(result.containsKey(2L));

        // Validate epoch 1
        Set<KVRangeStoreDescriptor> epoch1Set = result.get(1L);
        assertEquals(epoch1Set.size(), 1);
        KVRangeStoreDescriptor resultDescriptor1 = epoch1Set.iterator().next();
        assertEquals(resultDescriptor1.getId(), "store1");
        assertEquals(resultDescriptor1.getRangesCount(), 1);
        assertEquals(resultDescriptor1.getRanges(0).getId().getEpoch(), 1L);

        // Validate epoch 2
        Set<KVRangeStoreDescriptor> epoch2Set = result.get(2L);
        assertEquals(epoch2Set.size(), 1);
        KVRangeStoreDescriptor resultDescriptor2 = epoch2Set.iterator().next();
        assertEquals(resultDescriptor2.getId(), "store1");
        assertEquals(resultDescriptor2.getRangesCount(), 1);
        assertEquals(resultDescriptor2.getRanges(0).getId().getEpoch(), 2L);
    }

    @Test
    public void organizeByEpochWithEmptyKVRangeDescriptorInEpoch() {
        // Prepare data with multiple epochs, but some epoch lacks KVRangeDescriptor
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("m"))
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .putStatistics("stat1", 1.0)
            .addRanges(kvRangeDescriptor1)
            .setHlc(12345L)
            .build();

        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("store2")
            .putStatistics("stat2", 2.0)
            .setHlc(67890L) // No KVRangeDescriptor for this store in epoch 1 or 2
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor1);
        storeDescriptors.add(storeDescriptor2);

        // Execute the method
        NavigableMap<Long, Set<KVRangeStoreDescriptor>> result = organizeByEpoch(storeDescriptors);

        // Validate the result
        assertEquals(result.size(), 1); // Should contain two epochs
        assertTrue(result.containsKey(1L));
        assertEquals(result.get(1L).size(), 2);

        // Validate epoch 1
        Set<KVRangeStoreDescriptor> epoch1Set = result.get(1L);
        assertEquals(epoch1Set.size(), 2);
        for (KVRangeStoreDescriptor descriptor : epoch1Set) {
            if (descriptor.getId().equals("store1")) {
                assertEquals(descriptor.getRangesCount(), 1);
                assertEquals(descriptor.getRanges(0).getId().getEpoch(), 1L);
            } else if (descriptor.getId().equals("store2")) {
                assertEquals(descriptor.getRangesCount(), 0); // No ranges in epoch 1 for store2
            }
        }
    }

    @Test
    public void organizeByEpochAllEmptyEpochs() {
        // Prepare data with stores having no ranges but different epochs
        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .putStatistics("stat1", 1.0)
            .setHlc(12345L)
            .build();

        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("store2")
            .putStatistics("stat2", 2.0)
            .setHlc(67890L)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor1);
        storeDescriptors.add(storeDescriptor2);

        // Execute the method
        NavigableMap<Long, Set<KVRangeStoreDescriptor>> result = organizeByEpoch(storeDescriptors);

        // Validate the result
        assertTrue(result.isEmpty()); // Should be empty since no epochs or ranges are present
    }

    @Test
    public void getEffectiveEpochSingleEpoch() {
        // Prepare data with a single epoch
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .putStatistics("stat1", 1.0)
            .addRanges(kvRangeDescriptor)
            .setHlc(12345L)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);

        // Execute the method
        Set<KVRangeStoreDescriptor> result = getEffectiveEpoch(storeDescriptors).get().storeDescriptors();

        // Validate the result
        assertEquals(result.size(), 1);
        assertTrue(result.contains(storeDescriptor));
    }

    @Test
    public void getEffectiveEpochMultipleEpochs() {
        // Prepare data with multiple epochs
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("m"))
                .build())
            .build();

        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(2).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("n"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .putStatistics("stat1", 1.0)
            .addRanges(kvRangeDescriptor1)
            .setHlc(12345L)
            .build();

        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("store2")
            .putStatistics("stat2", 2.0)
            .addRanges(kvRangeDescriptor2)
            .setHlc(67890L)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor1);
        storeDescriptors.add(storeDescriptor2);

        // Execute the method
        Set<KVRangeStoreDescriptor> result = getEffectiveEpoch(storeDescriptors).get().storeDescriptors();

        // Validate the result
        assertEquals(result.size(), 2);
        assertTrue(result.contains(storeDescriptor1));
    }

    @Test
    public void getEffectiveEpochEmptySet() {
        // Prepare an empty set of store descriptors
        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();

        // Validate the result
        assertTrue(getEffectiveEpoch(storeDescriptors).isEmpty());
    }

    @Test
    public void getEffectiveEpochMultipleRangesSameStoreDifferentEpochs() {
        // Prepare data with multiple ranges in the same store but different epochs
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("m"))
                .build())
            .build();

        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(2).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("n"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .putStatistics("stat1", 1.0)
            .addRanges(kvRangeDescriptor1)
            .addRanges(kvRangeDescriptor2)
            .setHlc(12345L)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);

        // Execute the method
        Set<KVRangeStoreDescriptor> result = getEffectiveEpoch(storeDescriptors).get().storeDescriptors();

        // Validate the result
        assertEquals(result.size(), 1);
        KVRangeStoreDescriptor resultDescriptor = result.iterator().next();
        assertEquals(resultDescriptor.getRangesCount(), 1);
        assertEquals(resultDescriptor.getRanges(0).getId().getEpoch(), 1L);
        assertEquals(resultDescriptor.getRanges(0).getId(), kvRangeId1);
    }

    @Test
    public void getEffectiveRouteContiguousChain() {
        KVRangeId id1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        Boundary.Builder boundaryBuilder1 = Boundary.newBuilder();
        boundaryBuilder1.setEndKey(ByteString.copyFromUtf8("b"));
        KVRangeDescriptor r1 = KVRangeDescriptor.newBuilder()
            .setId(id1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(boundaryBuilder1.build())
            .build();

        KVRangeId id2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        Boundary.Builder boundaryBuilder2 = Boundary.newBuilder();
        boundaryBuilder2.setStartKey(ByteString.copyFromUtf8("b"));
        boundaryBuilder2.setEndKey(ByteString.copyFromUtf8("m"));
        KVRangeDescriptor r2 = KVRangeDescriptor.newBuilder()
            .setId(id2)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(boundaryBuilder2.build())
            .build();

        KVRangeId id3 = KVRangeId.newBuilder().setEpoch(1).setId(3).build();
        Boundary.Builder boundaryBuilder3 = Boundary.newBuilder();
        boundaryBuilder3.setStartKey(ByteString.copyFromUtf8("m"));
        KVRangeDescriptor r3 = KVRangeDescriptor.newBuilder()
            .setId(id3)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(boundaryBuilder3.build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .addRanges(r1)
            .addRanges(r2)
            .addRanges(r3)
            .build();
        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);

        EffectiveEpoch effectiveEpoch = new EffectiveEpoch(1, storeDescriptors);

        EffectiveRoute effectiveRoute = DescriptorUtil.getEffectiveRoute(effectiveEpoch);
        NavigableMap<Boundary, LeaderRange> routeMap = effectiveRoute.leaderRanges();

        assertEquals(routeMap.size(), 3);
        List<Long> actualIds = routeMap.values().stream()
            .map(lr -> lr.descriptor().getId().getId())
            .sorted()
            .collect(Collectors.toList());
        List<Long> expectedIds = Arrays.asList(1L, 2L, 3L);
        assertEquals(actualIds, expectedIds);
    }

    @Test
    public void getEffectiveRouteSelectsSmallestVer() {
        KVRangeId id1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeId id2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();

        Boundary boundary = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("m"))
            .build();
        KVRangeDescriptor r1 = KVRangeDescriptor.newBuilder()
            .setId(id1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(boundary)
            .build();
        KVRangeDescriptor r2 = KVRangeDescriptor.newBuilder()
            .setId(id2)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(boundary)
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .addRanges(r1)
            .addRanges(r2)
            .build();
        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);

        EffectiveEpoch effectiveEpoch = new EffectiveEpoch(1, storeDescriptors);
        EffectiveRoute effectiveRoute = DescriptorUtil.getEffectiveRoute(effectiveEpoch);
        NavigableMap<Boundary, LeaderRange> routeMap = effectiveRoute.leaderRanges();

        assertEquals(routeMap.size(), 1);
        LeaderRange selected = routeMap.firstEntry().getValue();
        assertEquals(selected.descriptor().getId().getId(), 1L);
    }

    @Test
    public void getEffectiveRouteWithNoLeaders() {
        KVRangeId id1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        Boundary boundary = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("z"))
            .build();
        KVRangeDescriptor followerRange = KVRangeDescriptor.newBuilder()
            .setId(id1)
            .setRole(RaftNodeStatus.Follower)
            .setBoundary(boundary)
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .addRanges(followerRange)
            .build();
        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);

        EffectiveEpoch effectiveEpoch = new EffectiveEpoch(1, storeDescriptors);
        EffectiveRoute effectiveRoute = DescriptorUtil.getEffectiveRoute(effectiveEpoch);
        NavigableMap<Boundary, LeaderRange> routeMap = effectiveRoute.leaderRanges();

        assertTrue(routeMap.isEmpty());
    }
}