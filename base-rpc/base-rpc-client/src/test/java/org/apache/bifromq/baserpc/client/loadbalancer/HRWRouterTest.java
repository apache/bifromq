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

package org.apache.bifromq.baserpc.client.loadbalancer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.testng.annotations.Test;

public class HRWRouterTest {

    @Test
    public void testEmptyNodesReturnsNull() {
        HRWRouter<String> r = newRouter(Collections.emptyList(), Collections.emptyMap());
        assertNull(r.routeNode("any"));
    }

    @Test
    public void testAllZeroWeightsReturnsNull() {
        List<String> nodes = List.of("A", "B", "C");
        Map<String, Integer> w = Map.of("A", 0, "B", 0, "C", 0);
        HRWRouter<String> r = newRouter(nodes, w);
        assertNull(r.routeNode("k1"));
        assertNull(r.routeNode("k2"));
    }

    @Test
    public void testNegativeAndZeroWeightsAreIgnored() {
        List<String> nodes = List.of("A", "B", "C");
        Map<String, Integer> w = new HashMap<>();
        w.put("A", -1);
        w.put("B", 0);
        w.put("C", 3);
        HRWRouter<String> r = newRouter(nodes, w);
        for (int i = 0; i < 100; i++) {
            assertEquals(r.routeNode("k" + i), "C");
        }
    }

    @Test
    public void testDeterministicForSameKeyAndNodes() {
        List<String> nodes = List.of("A", "B", "C");
        Map<String, Integer> w = Map.of("A", 1, "B", 1, "C", 1);
        HRWRouter<String> r1 = newRouter(nodes, w);
        HRWRouter<String> r2 = newRouter(nodes, w);

        for (int i = 0; i < 1000; i++) {
            String key = "k" + i;
            assertEquals(r1.routeNode(key), r2.routeNode(key));
        }
    }

    @Test
    public void testNearUniformWhenEqualWeights() {
        List<String> nodes = List.of("A", "B", "C", "D");
        Map<String, Integer> w = Map.of("A", 1, "B", 1, "C", 1, "D", 1);
        HRWRouter<String> r = newRouter(nodes, w);

        int samples = 20_000;
        Map<String, Integer> count = new HashMap<>();
        nodes.forEach(n -> count.put(n, 0));

        for (int i = 0; i < samples; i++) {
            String key = "k" + i;
            String owner = r.routeNode(key);
            count.compute(owner, (k, v) -> v + 1);
        }

        double expect = samples / (double) nodes.size();
        double tol = expect * 0.05; // 5% tolerant
        for (String n : nodes) {
            assertTrue(Math.abs(count.get(n) - expect) <= tol);
        }
    }

    @Test
    public void testWeightedProportion() {
        // A:2, B:1, C:1 => probabilities 0.5, 0.25, 0.25
        List<String> nodes = List.of("A", "B", "C");
        Map<String, Integer> w = Map.of("A", 2, "B", 1, "C", 1);
        HRWRouter<String> r = newRouter(nodes, w);

        int samples = 20_000;
        Map<String, Integer> count = new HashMap<>();
        nodes.forEach(n -> count.put(n, 0));

        for (int i = 0; i < samples; i++) {
            String key = "k" + i;
            String owner = r.routeNode(key);
            count.compute(owner, (k, v) -> v + 1);
        }

        double pA = 2.0 / 4.0;
        double pB = 1.0 / 4.0;
        double pC = 1.0 / 4.0;
        double tol = samples * 0.03; // 3% tolerant
        assertTrue(Math.abs(count.get("A") - pA * samples) <= tol);
        assertTrue(Math.abs(count.get("B") - pB * samples) <= tol);
        assertTrue(Math.abs(count.get("C") - pC * samples) <= tol);
    }

    @Test
    public void testIncreasingWeightIncreasesShare() {
        List<String> nodes = List.of("A", "B");
        Map<String, Integer> w1 = Map.of("A", 1, "B", 1);
        Map<String, Integer> w2 = Map.of("A", 3, "B", 1);

        HRWRouter<String> r1 = newRouter(nodes, w1);
        HRWRouter<String> r2 = newRouter(nodes, w2);

        int samples = 20_000;
        int a1 = 0, a2 = 0;
        for (int i = 0; i < samples; i++) {
            String key = "k" + i;
            if ("A".equals(r1.routeNode(key))) {
                a1++;
            }
            if ("A".equals(r2.routeNode(key))) {
                a2++;
            }
        }
        assertTrue(a2 > a1);
    }

    @Test
    public void testMinimalMovementOnAddNode() {
        // N=3 -> N=4, theoretically about ~1/(N+1)=~25% of keys move to the new node
        List<String> nodes3 = List.of("A", "B", "C");
        Map<String, Integer> w3 = Map.of("A", 1, "B", 1, "C", 1);
        HRWRouter<String> r3 = newRouter(nodes3, w3);

        List<String> nodes4 = List.of("A", "B", "C", "D");
        Map<String, Integer> w4 = Map.of("A", 1, "B", 1, "C", 1, "D", 1);
        HRWRouter<String> r4 = newRouter(nodes4, w4);

        int samples = 20_000;
        int movedToD = 0;
        for (int i = 0; i < samples; i++) {
            String key = "k" + i;
            String o3 = r3.routeNode(key);
            String o4 = r4.routeNode(key);
            if (!Objects.equals(o3, o4) && "D".equals(o4)) {
                movedToD++;
            }
        }
        double ratio = movedToD / (double) samples;
        // 0.25 ± 0.08
        assertTrue(Math.abs(ratio - 0.25) <= 0.08,
            "movement ratio to new node not close to 1/(N+1): " + ratio);
    }

    @Test
    public void testMinimalMovementOnRemoveNode() {
        // N=4 -> N=3, remove D, theoretically about ~1/(N)=~25% of keys move to other nodes
        List<String> nodes4 = List.of("A", "B", "C", "D");
        Map<String, Integer> w4 = Map.of("A", 1, "B", 1, "C", 1, "D", 1);
        HRWRouter<String> r4 = newRouter(nodes4, w4);

        List<String> nodes3 = List.of("A", "B", "C");
        Map<String, Integer> w3 = Map.of("A", 1, "B", 1, "C", 1);
        HRWRouter<String> r3 = newRouter(nodes3, w3);

        int samples = 20_000;
        int moved = 0;
        int nodesInD = 0;
        for (int i = 0; i < samples; i++) {
            String key = "k" + i;
            String o4 = r4.routeNode(key);
            String o3 = r3.routeNode(key);
            if ("D".equals(o4)) {
                nodesInD++;
                if (!Objects.equals(o3, o4)) {
                    moved++;
                }
            }
        }
        // moved should be close to nodesInD (all keys owned by D should move)
        double ratio = moved / (double) Math.max(1, nodesInD);
        assertTrue(ratio > 0.95);
    }

    private HRWRouter<String> newRouter(Collection<String> nodes, Map<String, Integer> weights) {
        return new HRWRouter<>(nodes, n -> n, weights::get);
    }
}