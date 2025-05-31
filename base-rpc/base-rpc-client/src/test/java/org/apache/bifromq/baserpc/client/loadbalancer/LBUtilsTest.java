/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.baserpc.client.loadbalancer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class LBUtilsTest {
    @Test
    void emptyWeightedValues() {
        Map<String, Integer> weightedValues = Collections.emptyMap();
        List<String> result = LBUtils.toWeightedRRSequence(weightedValues);
        assertTrue(result.isEmpty());
    }

    @Test
    void equalWeights() {
        Map<String, Integer> weightedValues = Map.of(
            "server1", 1,
            "server2", 1,
            "server3", 1
        );
        List<String> result = LBUtils.toWeightedRRSequence(weightedValues);
        List<String> expected = List.of("server1", "server2", "server3");
        assertEquals(result.size(), 3);
        assertEquals(new HashSet<>(expected), new HashSet<>(result));
    }

    @Test
    void differentWeights() {
        Map<String, Integer> weightedValues = Map.of(
            "server1", 3,
            "server2", 1
        );
        List<String> result = LBUtils.toWeightedRRSequence(weightedValues);
        Map<String, Integer> counts = new HashMap<>();
        // Expected sequence: server1 should appear three times more often than server2
        result.forEach(server -> counts.compute(server, (k, v) -> {
            if (v == null) {
                return 1;
            } else {
                return v + 1;
            }
        }));
        assertEquals(weightedValues, counts);
    }

    @Test
    void gcdWeights() {
        Map<String, Integer> weightedValues = Map.of(
            "server1", 4,
            "server2", 2
        );
        List<String> result = LBUtils.toWeightedRRSequence(weightedValues);
        Map<String, Integer> counts = new HashMap<>();
        // GCD of 4 and 2 is 2, so the sequence will reflect this ratio
        Map<String, Integer> expected = Map.of(
            "server1", 2,
            "server2", 1
        );
        result.forEach(server -> counts.compute(server, (k, v) -> {
            if (v == null) {
                return 1;
            } else {
                return v + 1;
            }
        }));
        assertEquals(expected, counts);
    }

    @Test
    void singleWeight() {
        Map<String, Integer> weightedValues = Map.of("server1", 5);
        List<String> result = LBUtils.toWeightedRRSequence(weightedValues);
        // Only one server, it should appear according to its weight
        List<String> expected = List.of("server1");
        assertEquals(expected, result, "Expected the single server to repeat according to its weight");
    }
}
