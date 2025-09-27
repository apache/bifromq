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

import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.buildClientRoute;
import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByBoundary;
import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByKey;
import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.patchRouteMap;
import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.refreshRouteMap;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.NULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.beust.jcommander.internal.Lists;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.basekv.utils.RangeLeader;
import org.testng.annotations.Test;

public class KVRangeRouterUtilTest {

    @Test
    public void emptyRouter() {
        assertFalse(findByKey(ByteString.copyFromUtf8("a"), Collections.emptyNavigableMap()).isPresent());
        assertTrue(findByBoundary(FULL_BOUNDARY, Collections.emptyNavigableMap()).isEmpty());
    }

    @Test
    public void findByKeyTest() {
        // Prepare test data with full space coverage
        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(1).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("b")).build()) // (null, "b")
            .build();
        KVRangeSetting rangeSetting1 = new KVRangeSetting("testCluster", "V1", new HashMap<>() {
            {
                put("V1", range1);
            }
        });

        KVRangeDescriptor range2 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(2).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("b"))
                .setEndKey(ByteString.copyFromUtf8("c"))
                .build()) // ["b", "c")
            .build();
        KVRangeSetting rangeSetting2 = new KVRangeSetting("testCluster", "V1", new HashMap<>() {
            {
                put("V1", range2);
            }
        });

        KVRangeDescriptor range3 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(3).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("c"))
                .setEndKey(ByteString.copyFromUtf8("d"))
                .build()) // ["c", "d")
            .build();
        KVRangeSetting rangeSetting3 = new KVRangeSetting("testCluster", "V1", new HashMap<>() {
            {
                put("V1", range3);
            }
        });

        KVRangeDescriptor range4 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(4).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("d"))
                .build()) // ["d", null)
            .build();
        KVRangeSetting rangeSetting4 = new KVRangeSetting("testCluster", "V1", new HashMap<>() {
            {
                put("V1", range4);
            }
        });

        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        router.put(range1.getBoundary(), rangeSetting1);
        router.put(range2.getBoundary(), rangeSetting2);
        router.put(range3.getBoundary(), rangeSetting3);
        router.put(range4.getBoundary(), rangeSetting4);

        // Test find by key within the first range
        Optional<KVRangeSetting> result1 = findByKey(ByteString.copyFromUtf8("a"), router);
        assertTrue(result1.isPresent());
        assertEquals(result1.get().id().getId(), 1L);

        // Test find by key within the second range
        Optional<KVRangeSetting> result2 = findByKey(ByteString.copyFromUtf8("b"), router);
        assertTrue(result2.isPresent());
        assertEquals(result2.get().id().getId(), 2L);

        // Test find by key within the third range
        Optional<KVRangeSetting> result3 = findByKey(ByteString.copyFromUtf8("c"), router);
        assertTrue(result3.isPresent());
        assertEquals(result3.get().id().getId(), 3L);

        // Test find by key within the fourth range
        Optional<KVRangeSetting> result4 = findByKey(ByteString.copyFromUtf8("d"), router);
        assertTrue(result4.isPresent());
        assertEquals(result4.get().id().getId(), 4L);

        // Test find by key not in any range
        Optional<KVRangeSetting> result5 = findByKey(ByteString.copyFromUtf8("z"), router);
        assertTrue(result5.isPresent());
        assertEquals(result5.get().id().getId(), 4L);
    }

    @Test
    public void findByBoundaryTest() {
        // Prepare test data with full space coverage
        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(1).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("b")).build()) // (null, "b")
            .build();
        KVRangeSetting rangeSetting1 = new KVRangeSetting("testCluster", "V1", new HashMap<>() {
            {
                put("V1", range1);
            }
        });

        KVRangeDescriptor range2 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(2).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("b"))
                .setEndKey(ByteString.copyFromUtf8("c"))
                .build()) // ["b", "c")
            .build();
        KVRangeSetting rangeSetting2 = new KVRangeSetting("testCluster", "V1", new HashMap<>() {
            {
                put("V1", range2);
            }
        });

        KVRangeDescriptor range3 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(3).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("c"))
                .setEndKey(ByteString.copyFromUtf8("d"))
                .build()) // ["c", "d")
            .build();
        KVRangeSetting rangeSetting3 = new KVRangeSetting("testCluster", "V1", new HashMap<>() {
            {
                put("V1", range3);
            }
        });

        KVRangeDescriptor range4 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(4).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("d"))
                .build()) // ["d", null)
            .build();
        KVRangeSetting rangeSetting4 = new KVRangeSetting("testCluster", "V1", new HashMap<>() {
            {
                put("V1", range4);
            }
        });

        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        router.put(range1.getBoundary(), rangeSetting1);
        router.put(range2.getBoundary(), rangeSetting2);
        router.put(range3.getBoundary(), rangeSetting3);
        router.put(range4.getBoundary(), rangeSetting4);

        Collection<KVRangeSetting> result0 = findByBoundary(
            Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("a")).build(), router); // (null, "a")
        assertEquals(result0.size(), 1);
        assertEquals(result0.stream().findFirst().get().id().getId(), 1L);

        // Test find by exact boundary of the first range
        Collection<KVRangeSetting> result1 = findByBoundary(
            Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("b")).build(), router); // (null, "b")
        assertEquals(result1.size(), 1);
        assertEquals(result1.stream().findFirst().get().id().getId(), 1L);

        // Test find by overlapping boundary with the second range
        List<KVRangeSetting> result2 = Lists.newArrayList(findByBoundary(
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("b")).build(), router)); // ["b", null)
        assertEquals(result2.size(), 3); // Covers ranges 2, 3, and 4
        assertEquals(result2.get(0).boundary(), range2.getBoundary());
        assertEquals(result2.get(1).boundary(), range3.getBoundary());
        assertEquals(result2.get(2).boundary(), range4.getBoundary());

        // Test find by a boundary that overlaps multiple ranges
        List<KVRangeSetting> result3 = Lists.newArrayList(findByBoundary(
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("b"))
                .setEndKey(ByteString.copyFromUtf8("d"))
                .build(), router)); // ["b", "d")
        assertEquals(result3.size(), 2); // Covers ranges 2 and 3
        assertEquals(result3.get(0).boundary(), range2.getBoundary());
        assertEquals(result3.get(1).boundary(), range3.getBoundary());

        List<KVRangeSetting> result4 = Lists.newArrayList(findByBoundary(
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("x"))
                .setEndKey(ByteString.copyFromUtf8("y"))
                .build(), router)); // ["x", "y")
        assertEquals(result4.size(), 1);
        assertEquals(result4.get(0).boundary(), range4.getBoundary());

        List<KVRangeSetting> result5 = Lists.newArrayList(findByBoundary(FULL_BOUNDARY, router));
        assertEquals(result5.size(), 4);
        assertEquals(result5.get(0).boundary(), range1.getBoundary());
        assertEquals(result5.get(1).boundary(), range2.getBoundary());
        assertEquals(result5.get(2).boundary(), range3.getBoundary());
        assertEquals(result5.get(3).boundary(), range4.getBoundary());

        List<KVRangeSetting> result6 = Lists.newArrayList(findByBoundary(NULL_BOUNDARY, router));
        assertEquals(result6.size(), 1);
        assertEquals(result6.get(0).boundary(), range1.getBoundary());
    }

    @Test
    public void patchRouteMapAddsAndUpdatesByHlc() {
        Map<KVRangeId, Map<String, KVRangeDescriptor>> current = new HashMap<>();
        String s1 = "S1";
        String s2 = "S2";
        KVRangeId r1 = KVRangeIdUtil.generate();
        Boundary boundary = toBoundary(null, ByteString.copyFromUtf8("b"));

        KVRangeDescriptor r1_s1_hlc1 = KVRangeDescriptor.newBuilder()
            .setId(r1)
            .setVer(1)
            .setBoundary(boundary)
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setHlc(1)
            .build();
        KVRangeDescriptor r1_s1_hlc2 = r1_s1_hlc1.toBuilder().setHlc(2).build();
        KVRangeDescriptor r1_s1_hlc2_dup = r1_s1_hlc1.toBuilder().setHlc(2).build();
        KVRangeDescriptor r1_s2_hlc3 = r1_s1_hlc1.toBuilder().setHlc(3).build();

        // add first descriptor
        patchRouteMap(s1, r1_s1_hlc1, current);
        assertEquals(current.size(), 1);
        assertEquals(current.get(r1).size(), 1);
        assertEquals(current.get(r1).get(s1).getHlc(), 1L);

        // upgrade same replica by higher HLC
        patchRouteMap(s1, r1_s1_hlc2, current);
        assertEquals(current.get(r1).get(s1).getHlc(), 2L);

        // equal HLC should not downgrade
        patchRouteMap(s1, r1_s1_hlc2_dup, current);
        assertEquals(current.get(r1).get(s1).getHlc(), 2L);

        // add another store replica
        patchRouteMap(s2, r1_s2_hlc3, current);
        assertEquals(current.get(r1).size(), 2);
        assertEquals(current.get(r1).get(s2).getHlc(), 3L);

        // patching another range doesn't affect existing entries
        KVRangeId r2 = KVRangeIdUtil.generate();
        KVRangeDescriptor r2_s1_hlc1 = r1_s1_hlc1.toBuilder().setId(r2).setHlc(1).build();
        patchRouteMap(s1, r2_s1_hlc1, current);
        assertEquals(current.size(), 2);
        assertEquals(current.get(r1).get(s1).getHlc(), 2L);
        assertEquals(current.get(r2).get(s1).getHlc(), 1L);
    }

    @Test
    public void refreshRouteMapAppliesPatchAndCleansRemoved() {
        // current has two ranges and an extra replica to be cleaned
        Map<KVRangeId, Map<String, KVRangeDescriptor>> current = new HashMap<>();
        KVRangeId rA = KVRangeIdUtil.generate();
        KVRangeId rB = KVRangeIdUtil.generate();
        String A1 = "A1";
        String A2 = "A2";
        String A3 = "A3"; // A3 will be removed
        String B1 = "B1";
        String B2 = "B2";
        Boundary boundaryA = toBoundary(null, ByteString.copyFromUtf8("m"));
        Boundary boundaryB = toBoundary(ByteString.copyFromUtf8("m"), null);

        current.computeIfAbsent(rA, k -> new HashMap<>()).put(A1, descriptor(rA, boundaryA, 10));
        current.get(rA).put(A2, descriptor(rA, boundaryA, 9));
        current.get(rA).put(A3, descriptor(rA, boundaryA, 4)); // extra replica
        current.computeIfAbsent(rB, k -> new HashMap<>()).put(B1, descriptor(rB, boundaryB, 5));
        current.get(rB).put(B2, descriptor(rB, boundaryB, 2));

        // patch contains only rA with A1 higher HLC and A2 lower HLC; rB should be removed; A3 should be removed
        Map<KVRangeId, Map<String, KVRangeDescriptor>> patch = new HashMap<>();
        patch.computeIfAbsent(rA, k -> new HashMap<>()).put(A1, descriptor(rA, boundaryA, 11));
        patch.get(rA).put(A2, descriptor(rA, boundaryA, 7)); // lower than current(9), should keep 9

        Map<KVRangeId, Map<String, KVRangeDescriptor>> refreshed = refreshRouteMap(current, patch);

        // only rA remains
        assertEquals(refreshed.size(), 1);
        assertTrue(refreshed.containsKey(rA));
        // rA only has A1 and A2, A3 cleaned
        assertEquals(refreshed.get(rA).size(), 2);
        assertTrue(refreshed.get(rA).containsKey(A1));
        assertTrue(refreshed.get(rA).containsKey(A2));
        // HLC merge policy: higher wins, lower ignored
        assertEquals(refreshed.get(rA).get(A1).getHlc(), 11L);
        assertEquals(refreshed.get(rA).get(A2).getHlc(), 9L);
    }

    @Test
    public void buildClientRouteBuildsOrderedRouterWithExpectedSettings() {
        // prepare three leader ranges covering (null, b), [b, d), [d, null)
        ByteString b = ByteString.copyFromUtf8("b");
        ByteString d = ByteString.copyFromUtf8("d");

        KVRangeId r1 = KVRangeIdUtil.generate();
        KVRangeId r2 = KVRangeIdUtil.generate();
        KVRangeId r3 = KVRangeIdUtil.generate();
        String s1 = "S1";
        String s2 = "S2";
        String s3 = "S3";

        KVRangeDescriptor rd1 = KVRangeDescriptor.newBuilder()
            .setId(r1).setVer(1).setHlc(1)
            .setBoundary(toBoundary(null, b))
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .build();
        KVRangeDescriptor rd2 = KVRangeDescriptor.newBuilder()
            .setId(r2).setVer(2).setHlc(2)
            .setBoundary(toBoundary(b, d))
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .build();
        KVRangeDescriptor rd3 = KVRangeDescriptor.newBuilder()
            .setId(r3).setVer(3).setHlc(3)
            .setBoundary(toBoundary(d, null))
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .build();

        // leader map in boundary order
        NavigableMap<Boundary, RangeLeader> leaderMap = new TreeMap<>(BoundaryUtil::compare);
        leaderMap.put(rd1.getBoundary(), new RangeLeader(s1, rd1));
        leaderMap.put(rd2.getBoundary(), new RangeLeader(s2, rd2));
        leaderMap.put(rd3.getBoundary(), new RangeLeader(s3, rd3));

        // route map must contain all replicas for each range; at least leader replica needed
        Map<KVRangeId, Map<String, KVRangeDescriptor>> routeMap = new HashMap<>();
        routeMap.computeIfAbsent(r1, k -> new HashMap<>()).put(s1, rd1);
        routeMap.computeIfAbsent(r2, k -> new HashMap<>()).put(s2, rd2);
        routeMap.computeIfAbsent(r3, k -> new HashMap<>()).put(s3, rd3);

        String clusterId = "cluster-1";
        NavigableMap<Boundary, KVRangeSetting> router = buildClientRoute(clusterId, leaderMap, routeMap);

        assertEquals(router.size(), leaderMap.size());
        // verify each boundary maps to expected KVRangeSetting fields
        leaderMap.forEach((boundary, leaderRange) -> {
            KVRangeSetting setting = router.get(boundary);
            assertNotNull(setting);
            assertEquals(setting.id(), leaderRange.descriptor().getId());
            assertEquals(setting.ver(), leaderRange.descriptor().getVer());
            assertEquals(setting.boundary(), leaderRange.descriptor().getBoundary());
            assertEquals(setting.leader(), leaderRange.storeId());
        });
    }

    private KVRangeDescriptor descriptor(KVRangeId id, Boundary boundary, long hlc) {
        return KVRangeDescriptor.newBuilder()
            .setId(id).setVer(1)
            .setBoundary(boundary)
            .setRole(RaftNodeStatus.Leader)
            .setState(State.StateType.Normal)
            .setHlc(hlc)
            .build();
    }
}