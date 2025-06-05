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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.testng.annotations.Test;

public class WCHRouterTest {
    @Test
    void emptyRing() {
        Map<String, Integer> pNodes = Collections.emptyMap();
        WCHRouter<Map.Entry<String, Integer>> router =
            new WCHRouter<>(pNodes.entrySet(), Map.Entry::getKey, Map.Entry::getValue, 3);
        assertTrue(router.isEmpty());
        assertNull(router.routeNode("testKey"));
    }

    @Test
    void testSingleNodeNoWeight() {
        String node = "Node1";
        WCHRouter<String> router = new WCHRouter<>(List.of(node), n -> n, n -> 1, 3);

        assertFalse(router.isEmpty());

        String result = router.routeNode("testKey");
        assertEquals(node, result);
    }

    @Test
    void multipleNodesWithWeights() {
        var nodes = List.of("Node1", "Node2");
        WCHRouter<String> router =
            new WCHRouter<>(nodes, n -> n, n -> n.equals("Node1") ? 2 : 1, 3);

        assertFalse(router.isEmpty());

        // Test routing for different keys
        String result1 = router.routeNode("testKey1");
        String result2 = router.routeNode("testKey2");

        assertTrue(result1.equals("Node1") || result1.equals("Node2"));
        assertTrue(result2.equals("Node1") || result2.equals("Node2"));

        // Node1 should appear more frequently due to its higher weight
        long node1Count = nodes.stream().filter(n -> router.routeNode("testKey1").equals("Node1")).count();
        assertTrue(node1Count > 1);
    }

    @Test
    void addNodeBalance() {
        String node1 = "Node1";
        String node2 = "Node2";
        WCHRouter<String> router = new WCHRouter<>(List.of(node1), n -> n, n -> 1, 3);

        assertEquals(node1, router.routeNode("testKey1"));

        // Add new node
        router.addNode(node2, 3);

        // Check if both nodes are in the ring and routing is balanced
        String result = router.routeNode("testKey2");
        assertTrue(result.equals(node1) || result.equals(node2));
    }

    @Test
    void addNodeBalanceWithRandomKeys() {
        String node1 = "Node1";
        String node2 = "Node2";

        // 设置虚拟节点数量
        int vNodeCountNode1 = 60;
        int vNodeCountNode2 = 60;

        // 创建 ConsistentHashRouter 并仅添加 Node1
        WCHRouter<String> router = new WCHRouter<>(List.of(node1),
            n -> n,
            n -> 1,
            vNodeCountNode1);

        // 添加 Node2
        router.addNode(node2, vNodeCountNode2);

        // 随机生成1000个键
        int totalKeys = 10000;
        Random random = new Random();
        Map<String, Integer> routingResults = new HashMap<>();
        routingResults.put(node1, 0);
        routingResults.put(node2, 0);

        for (int i = 0; i < totalKeys; i++) {
            // 生成随机的 key
            String randomKey = "key" + random.nextInt();
            // 路由该 key
            String routedNode = router.routeNode(randomKey);
            // 统计路由结果
            routingResults.put(routedNode, routingResults.get(routedNode) + 1);
        }

        double ratioNode1 = routingResults.get(node1) / (double) totalKeys;
        double ratioNode2 = routingResults.get(node2) / (double) totalKeys;

        double expectedRatioNode1 = vNodeCountNode1 / (double) (vNodeCountNode1 + vNodeCountNode2);
        double expectedRatioNode2 = vNodeCountNode2 / (double) (vNodeCountNode1 + vNodeCountNode2);

        double tolerance = 0.1;

        assertTrue(Math.abs(ratioNode1 - expectedRatioNode1) < tolerance);
        assertTrue(Math.abs(ratioNode2 - expectedRatioNode2) < tolerance);
    }

    @Test
    void weightAffectsVirtualNodes() {
        String node1 = "Node1";
        String node2 = "Node2";

        WCHRouter<String> router = new WCHRouter<>(
            List.of(node1, node2),
            n -> n,
            n -> n.equals("Node1") ? 2 : 1, 3
        );

        // Node1 should have more virtual nodes than Node2 due to its higher weight
        long node1Replicas = router.getExistingReplicas(node1);
        long node2Replicas = router.getExistingReplicas(node2);

        assertTrue(node1Replicas > node2Replicas);
    }
}
