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

package org.apache.bifromq.base.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

public class RendezvousHashTest {
    @Test
    public void testSameKeyReturnsSameNode() {
        List<String> nodes = Arrays.asList("NodeA", "NodeB", "NodeC");
        RendezvousHash<String, String> rendezvousHash = RendezvousHash.<String, String>builder()
            .keyFunnel((from, into) -> into.putString(from, StandardCharsets.UTF_8))
            .nodeFunnel((from, into) -> into.putString(from, StandardCharsets.UTF_8))
            .nodes(nodes)
            .build();

        String key = "testKey";
        String node1 = rendezvousHash.get(key);
        String node2 = rendezvousHash.get(key);
        assertNotNull(node1);
        assertNotNull(node2);
        assertEquals(node1, node2);
    }

    @Test
    public void testDifferentKeysReturnNodes() {
        List<String> nodes = Arrays.asList("NodeA", "NodeB", "NodeC");
        RendezvousHash<String, String> rendezvousHash = RendezvousHash.<String, String>builder()
            .keyFunnel((from, into) -> into.putString(from, StandardCharsets.UTF_8))
            .nodeFunnel((from, into) -> into.putString(from, StandardCharsets.UTF_8))
            .nodes(nodes)
            .build();

        String key1 = "key1";
        String key2 = "key2";
        String node1 = rendezvousHash.get(key1);
        String node2 = rendezvousHash.get(key2);
        assertNotNull(node1);
        assertNotNull(node2);
    }

    @Test
    public void testDistribution() {
        List<String> nodes = Arrays.asList("NodeA", "NodeB", "NodeC");
        RendezvousHash<String, String> rendezvousHash = RendezvousHash.<String, String>builder()
            .keyFunnel((from, into) -> into.putString(from, StandardCharsets.UTF_8))
            .nodeFunnel((from, into) -> into.putString(from, StandardCharsets.UTF_8))
            .nodes(nodes)
            .build();

        int numKeys = 10000;
        int[] counts = new int[nodes.size()];

        for (int i = 0; i < numKeys; i++) {
            String key = "key" + i;
            String node = rendezvousHash.get(key);
            int index = nodes.indexOf(node);
            if (index >= 0) {
                counts[index]++;
            }
        }

        int avg = numKeys / nodes.size();
        double tolerance = avg * 0.3;

        for (int i = 0; i < counts.length; i++) {
            assertTrue(Math.abs(counts[i] - avg) <= tolerance);
        }
    }
}
