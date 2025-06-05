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

import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByBoundary;
import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.NULL_BOUNDARY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import com.beust.jcommander.internal.Lists;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import org.testng.annotations.Test;

public class KVRangeRouterUtilTest {

    @Test
    public void reset() {

    }

    @Test
    public void emptyRouter() {
        assertFalse(findByKey(ByteString.copyFromUtf8("a"), Collections.emptyNavigableMap()).isPresent());
        assertTrue(findByBoundary(FULL_BOUNDARY, Collections.emptyNavigableMap()).isEmpty());
    }

    @Test
    public void testFindByKey() {
        // Prepare test data with full space coverage
        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(1).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("b")).build()) // (null, "b")
            .build();
        KVRangeSetting rangeSetting1 = new KVRangeSetting("testCluster", "V1", range1);

        KVRangeDescriptor range2 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(2).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("b"))
                .setEndKey(ByteString.copyFromUtf8("c"))
                .build()) // ["b", "c")
            .build();
        KVRangeSetting rangeSetting2 = new KVRangeSetting("testCluster", "V1", range2);

        KVRangeDescriptor range3 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(3).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("c"))
                .setEndKey(ByteString.copyFromUtf8("d"))
                .build()) // ["c", "d")
            .build();
        KVRangeSetting rangeSetting3 = new KVRangeSetting("testCluster", "V1", range3);

        KVRangeDescriptor range4 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(4).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("d"))
                .build()) // ["d", null)
            .build();
        KVRangeSetting rangeSetting4 = new KVRangeSetting("testCluster", "V1", range4);

        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        router.put(range1.getBoundary(), rangeSetting1);
        router.put(range2.getBoundary(), rangeSetting2);
        router.put(range3.getBoundary(), rangeSetting3);
        router.put(range4.getBoundary(), rangeSetting4);

        // Test find by key within the first range
        Optional<KVRangeSetting> result1 = findByKey(ByteString.copyFromUtf8("a"), router);
        assertTrue(result1.isPresent());
        assertEquals(result1.get().id.getId(), 1L);

        // Test find by key within the second range
        Optional<KVRangeSetting> result2 = findByKey(ByteString.copyFromUtf8("b"), router);
        assertTrue(result2.isPresent());
        assertEquals(result2.get().id.getId(), 2L);

        // Test find by key within the third range
        Optional<KVRangeSetting> result3 = findByKey(ByteString.copyFromUtf8("c"), router);
        assertTrue(result3.isPresent());
        assertEquals(result3.get().id.getId(), 3L);

        // Test find by key within the fourth range
        Optional<KVRangeSetting> result4 = findByKey(ByteString.copyFromUtf8("d"), router);
        assertTrue(result4.isPresent());
        assertEquals(result4.get().id.getId(), 4L);

        // Test find by key not in any range
        Optional<KVRangeSetting> result5 = findByKey(ByteString.copyFromUtf8("z"), router);
        assertTrue(result5.isPresent());
        assertEquals(result5.get().id.getId(), 4L);
    }

    @Test
    public void testFindByBoundary() {
        // Prepare test data with full space coverage
        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(1).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("b")).build()) // (null, "b")
            .build();
        KVRangeSetting rangeSetting1 = new KVRangeSetting("testCluster", "V1", range1);

        KVRangeDescriptor range2 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(2).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("b"))
                .setEndKey(ByteString.copyFromUtf8("c"))
                .build()) // ["b", "c")
            .build();
        KVRangeSetting rangeSetting2 = new KVRangeSetting("testCluster", "V1", range2);

        KVRangeDescriptor range3 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(3).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("c"))
                .setEndKey(ByteString.copyFromUtf8("d"))
                .build()) // ["c", "d")
            .build();
        KVRangeSetting rangeSetting3 = new KVRangeSetting("testCluster", "V1", range3);

        KVRangeDescriptor range4 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(4).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("d"))
                .build()) // ["d", null)
            .build();
        KVRangeSetting rangeSetting4 = new KVRangeSetting("testCluster", "V1", range4);

        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        router.put(range1.getBoundary(), rangeSetting1);
        router.put(range2.getBoundary(), rangeSetting2);
        router.put(range3.getBoundary(), rangeSetting3);
        router.put(range4.getBoundary(), rangeSetting4);

        Collection<KVRangeSetting> result0 = findByBoundary(
            Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("a")).build(), router); // (null, "a")
        assertEquals(result0.size(), 1);
        assertEquals(result0.stream().findFirst().get().id.getId(), 1L);

        // Test find by exact boundary of the first range
        Collection<KVRangeSetting> result1 = findByBoundary(
            Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("b")).build(), router); // (null, "b")
        assertEquals(result1.size(), 1);
        assertEquals(result1.stream().findFirst().get().id.getId(), 1L);

        // Test find by overlapping boundary with the second range
        List<KVRangeSetting> result2 = Lists.newArrayList(findByBoundary(
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("b")).build(), router)); // ["b", null)
        assertEquals(result2.size(), 3); // Covers ranges 2, 3, and 4
        assertEquals(result2.get(0).boundary, range2.getBoundary());
        assertEquals(result2.get(1).boundary, range3.getBoundary());
        assertEquals(result2.get(2).boundary, range4.getBoundary());

        // Test find by a boundary that overlaps multiple ranges
        List<KVRangeSetting> result3 = Lists.newArrayList(findByBoundary(
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("b"))
                .setEndKey(ByteString.copyFromUtf8("d"))
                .build(), router)); // ["b", "d")
        assertEquals(result3.size(), 2); // Covers ranges 2 and 3
        assertEquals(result3.get(0).boundary, range2.getBoundary());
        assertEquals(result3.get(1).boundary, range3.getBoundary());

        List<KVRangeSetting> result4 = Lists.newArrayList(findByBoundary(
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("x"))
                .setEndKey(ByteString.copyFromUtf8("y"))
                .build(), router)); // ["x", "y")
        assertEquals(result4.size(), 1);
        assertEquals(result4.get(0).boundary, range4.getBoundary());

        List<KVRangeSetting> result5 = Lists.newArrayList(findByBoundary(FULL_BOUNDARY, router));
        assertEquals(result5.size(), 4);
        assertEquals(result5.get(0).boundary, range1.getBoundary());
        assertEquals(result5.get(1).boundary, range2.getBoundary());
        assertEquals(result5.get(2).boundary, range3.getBoundary());
        assertEquals(result5.get(3).boundary, range4.getBoundary());

        List<KVRangeSetting> result6 = Lists.newArrayList(findByBoundary(NULL_BOUNDARY, router));
        assertEquals(result6.size(), 1);
        assertEquals(result6.get(0).boundary, range1.getBoundary());
    }
}