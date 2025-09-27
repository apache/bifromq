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

package org.apache.bifromq.dist.server.scheduler;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.dist.rpc.proto.Fact;
import org.apache.bifromq.dist.rpc.proto.GlobalFilterLevels;
import org.apache.bifromq.dist.worker.schema.KVSchemaUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TenantRangeLookupCacheTest {
    private static final String TENANT = "tenantA";

    private TenantRangeLookupCache cache;

    private static ByteString tenantStart() {
        return KVSchemaUtil.tenantBeginKey(TENANT);
    }

    private static ByteString tenantRouteKey(String... levels) {
        List<String> list = new ArrayList<>();
        for (String l : levels) {
            list.add(l);
        }
        return KVSchemaUtil.tenantRouteStartKey(TENANT, list);
    }

    private static GlobalFilterLevels first(String tenant, String... levels) {
        GlobalFilterLevels.Builder builder = GlobalFilterLevels.newBuilder();
        builder.addFilterLevel(tenant);
        for (String l : levels) {
            builder.addFilterLevel(l);
        }
        return builder.build();
    }

    private static GlobalFilterLevels last(String tenant, String... levels) {
        GlobalFilterLevels.Builder builder = GlobalFilterLevels.newBuilder();
        builder.addFilterLevel(tenant);
        for (String l : levels) {
            builder.addFilterLevel(l);
        }
        return builder.build();
    }

    private static Fact fact(GlobalFilterLevels first, GlobalFilterLevels last) {
        return Fact.newBuilder()
            .setFirstGlobalFilterLevels(first)
            .setLastGlobalFilterLevels(last)
            .build();
    }

    private static KVRangeSetting setting(long id, Boundary boundary, Fact fact) {
        KVRangeDescriptor.Builder desc = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(id).build())
            .setVer(1)
            .setBoundary(boundary)
            .setConfig(ClusterConfig.newBuilder().addVoters("S1").build());
        if (fact != null) {
            desc.setFact(Any.pack(fact));
        }
        return new KVRangeSetting("test_cluster", "S1", new HashMap<>() {
            {
                put("S1", desc.build());
            }
        });
    }

    @BeforeMethod
    public void setUp() {
        cache = new TenantRangeLookupCache(TENANT, Duration.ofMinutes(10), 1000);
    }

    @Test
    public void emptyRouterReturnsEmpty() {
        Collection<KVRangeSetting> result = cache.lookup("a", new TreeMap<>(BoundaryUtil::compare));
        assertTrue(result.isEmpty());
    }

    @Test
    public void singleCandidateFullFactCovers() {
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        KVRangeSetting r1 = setting(1,
            toBoundary(tenantStart(), null),
            fact(first(TENANT, "a"), last(TENANT, "z")));
        router.put(r1.boundary(), r1);

        Collection<KVRangeSetting> result = cache.lookup("m/n", router);
        assertEquals(result.size(), 1);
        assertTrue(result.contains(r1));
    }

    @Test
    public void singleCandidateFullFactNotCover() {
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        KVRangeSetting r1 = setting(1,
            toBoundary(tenantStart(), null),
            fact(first(TENANT, "m"), last(TENANT, "z")));
        router.put(r1.boundary(), r1);

        Collection<KVRangeSetting> result = cache.lookup("a/b", router);
        assertTrue(result.isEmpty());
    }

    @Test
    public void singleCandidateMissingFirstOrLastIsEmptyRange() {
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);

        // Missing last
        KVRangeSetting r1 = setting(1,
            toBoundary(tenantStart(), null),
            Fact.newBuilder().setFirstGlobalFilterLevels(first(TENANT, "a")).build());
        router.put(r1.boundary(), r1);
        assertTrue(cache.lookup("a", router).isEmpty());

        // Missing first
        router.clear();
        KVRangeSetting r2 = setting(2,
            toBoundary(tenantStart(), null),
            Fact.newBuilder().setLastGlobalFilterLevels(last(TENANT, "z")).build());
        router.put(r2.boundary(), r2);
        assertTrue(cache.lookup("a", router).isEmpty());
    }

    @Test
    public void singleCandidateNoFactIsIncluded() {
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        // No fact -> Optional.empty -> conservatively include
        KVRangeSetting r1 = setting(1, toBoundary(tenantStart(), null), null);
        router.put(r1.boundary(), r1);

        Collection<KVRangeSetting> result = cache.lookup("topic", router);
        assertEquals(result.size(), 1);
        assertTrue(result.contains(r1));
    }

    @Test
    public void multiCandidatesTwoCoveringAndOneNot() {
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        ByteString kN = tenantRouteKey("n");
        ByteString kT = tenantRouteKey("t");

        // R1: [tenantStart, kN), Fact covers [a..z]
        KVRangeSetting r1 = setting(1, toBoundary(tenantStart(), kN), fact(first(TENANT, "a"), last(TENANT, "z")));
        // R2: [kN, kT), Fact covers [n..s]
        KVRangeSetting r2 = setting(2, toBoundary(kN, kT), fact(first(TENANT, "n"), last(TENANT, "s")));
        // R3: [kT, null), Fact covers [t..z]
        KVRangeSetting r3 = setting(3, toBoundary(kT, null), fact(first(TENANT, "t"), last(TENANT, "z")));

        router.put(r1.boundary(), r1);
        router.put(r2.boundary(), r2);
        router.put(r3.boundary(), r3);

        Collection<KVRangeSetting> result = cache.lookup("n/1", router);
        assertEquals(result.size(), 2);
        assertTrue(result.contains(r1));
        assertTrue(result.contains(r2));
        assertFalse(result.contains(r3));
    }

    @Test
    public void multiCandidatesMixWithNoFact() {
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        ByteString kN = tenantRouteKey("n");
        ByteString kZ = tenantRouteKey("z");

        // R1: [tenantStart, kN), No Fact -> include
        KVRangeSetting r1 = setting(1, toBoundary(tenantStart(), kN), null);
        // R2: [kN, kZ), Fact not covering topic
        KVRangeSetting r2 = setting(2, toBoundary(kN, kZ), fact(first(TENANT, "x"), last(TENANT, "z")));
        // R3: [kZ, null), Fact covering topic
        KVRangeSetting r3 = setting(3, toBoundary(kZ, null), fact(first(TENANT, "z"), last(TENANT, "zz")));

        router.put(r1.boundary(), r1);
        router.put(r2.boundary(), r2);
        router.put(r3.boundary(), r3);

        Collection<KVRangeSetting> result = cache.lookup("z/1", router);
        assertEquals(result.size(), 2);
        assertTrue(result.contains(r1));
        assertTrue(result.contains(r3));
        assertFalse(result.contains(r2));
    }

    @Test
    public void multiCandidatesLastLessThanTopicThenFollowingCovers() {
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        ByteString kN = tenantRouteKey("n");

        // R1: [tenantStart, kN), last < topic
        KVRangeSetting r1 = setting(1, toBoundary(tenantStart(), kN), fact(first(TENANT, "a"), last(TENANT, "m")));
        // R2: [kN, null), covering topic
        KVRangeSetting r2 = setting(2, toBoundary(kN, null), fact(first(TENANT, "n"), last(TENANT, "z")));

        router.put(r1.boundary(), r1);
        router.put(r2.boundary(), r2);

        Collection<KVRangeSetting> result = cache.lookup("n/1", router);
        assertEquals(result.size(), 1);
        assertTrue(result.contains(r2));
    }

    @Test
    public void earlyStopTopicLessThanFirstOfFirstCandidate() {
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        ByteString kB = tenantRouteKey("b");

        // R1 first > topic -> will trigger early stop, and R2 won't be examined
        KVRangeSetting r1 = setting(1, toBoundary(tenantStart(), kB), fact(first(TENANT, "b"), last(TENANT, "c")));
        // R2 no fact, but should not be reached
        KVRangeSetting r2 = setting(2, toBoundary(kB, null), null);

        router.put(r1.boundary(), r1);
        router.put(r2.boundary(), r2);

        Collection<KVRangeSetting> result = cache.lookup("a", router);
        assertTrue(result.isEmpty());
    }

    @Test
    public void earlyStopAfterIncludingNoFactFirst() {
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        ByteString kB = tenantRouteKey("b");

        // R1 no fact -> included
        KVRangeSetting r1 = setting(1, toBoundary(tenantStart(), kB), null);
        // R2 first > topic -> triggers early stop
        KVRangeSetting r2 = setting(2, toBoundary(kB, null), fact(first(TENANT, "b"), last(TENANT, "c")));

        router.put(r1.boundary(), r1);
        router.put(r2.boundary(), r2);

        Collection<KVRangeSetting> result = cache.lookup("a", router);
        assertEquals(result.size(), 1);
        assertTrue(result.contains(r1));
    }

    @Test
    public void includeWhenEqualToFirstOrLast() {
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);

        // Equals first
        KVRangeSetting r1 = setting(1, toBoundary(tenantStart(), null), fact(first(TENANT, "b"), last(TENANT, "z")));
        router.put(r1.boundary(), r1);
        assertEquals(cache.lookup("b", router).size(), 1);

        // Equals last
        router.clear();
        KVRangeSetting r2 = setting(2, toBoundary(tenantStart(), null), fact(first(TENANT, "a"), last(TENANT, "b")));
        router.put(r2.boundary(), r2);
        assertEquals(cache.lookup("b", router).size(), 1);
    }

    @Test
    public void multiLevelTopicAndOrder() {
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        ByteString kB = tenantRouteKey("b");

        // R1: first=[tenant,a,b], last=[tenant,a,z]
        KVRangeSetting r1 = setting(1, toBoundary(tenantStart(), kB),
            fact(first(TENANT, "a", "b"), last(TENANT, "a", "z")));
        // R2: first=[tenant,b], last=[tenant,c]
        KVRangeSetting r2 = setting(2, toBoundary(kB, null),
            fact(first(TENANT, "b"), last(TENANT, "c")));

        router.put(r1.boundary(), r1);
        router.put(r2.boundary(), r2);

        Collection<KVRangeSetting> result = cache.lookup("a/b/c", router);
        assertEquals(result.size(), 1);
        assertTrue(result.contains(r1));
    }

    @Test
    public void cacheFunctionalConsistency() {
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        ByteString kN = tenantRouteKey("n");
        KVRangeSetting r1 = setting(1, toBoundary(tenantStart(), kN), fact(first(TENANT, "a"), last(TENANT, "m")));
        KVRangeSetting r2 = setting(2, toBoundary(kN, null), fact(first(TENANT, "n"), last(TENANT, "z")));
        router.put(r1.boundary(), r1);
        router.put(r2.boundary(), r2);

        Collection<KVRangeSetting> first = cache.lookup("n/1", router);
        Collection<KVRangeSetting> second = cache.lookup("n/1", router);
        assertEquals(first, second);

        // change candidates -> should change result
        router.clear();
        KVRangeSetting r3 = setting(3, toBoundary(tenantStart(), null), fact(first(TENANT, "x"), last(TENANT, "z")));
        router.put(r3.boundary(), r3);
        Collection<KVRangeSetting> third = cache.lookup("n/1", router);
        assertTrue(third.isEmpty());
    }
}