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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.Test;

public class WeightedServerGroupRouterTest {
    @Test
    void isSameGroup() {
        Map<String, Boolean> allServers = Map.of("server1", false, "server2", false, "server3", false);
        Map<String, Integer> trafficAssignment = Map.of("group1", 5);
        Map<String, Set<String>> groupAssignment = Map.of(
            "group1", Set.of("server1", "server2", "server3")
        );
        Map<String, Set<String>> groupAssignment1 = Map.of(
            "group1", Set.of("server1", "server2")
        );
        WeightedServerGroupRouter router1 =
            new WeightedServerGroupRouter(allServers, trafficAssignment, groupAssignment);
        WeightedServerGroupRouter router2 =
            new WeightedServerGroupRouter(allServers, trafficAssignment, groupAssignment);
        assertTrue(router1.isSameGroup(router1));
        assertTrue(router1.isSameGroup(router2));
        assertTrue(router2.isSameGroup(router1));

        WeightedServerGroupRouter router3 =
            new WeightedServerGroupRouter(allServers, trafficAssignment, groupAssignment1);
        assertFalse(router1.isSameGroup(router3));
        assertFalse(router3.isSameGroup(router1));
    }

    @Test
    void randomRouting() {
        Map<String, Boolean> allServers = Map.of("server1", false, "server2", false, "server3", false);

        Map<String, Integer> trafficAssignment = Map.of("group1", 5);
        Map<String, Set<String>> groupAssignment = Map.of(
            "group1", Set.of("server1", "server2", "server3")
        );

        WeightedServerGroupRouter router =
            new WeightedServerGroupRouter(allServers, trafficAssignment, groupAssignment);

        Optional<String> server = router.random();
        assertTrue(server.isPresent());
        assertTrue(Set.of("server1", "server2", "server3").contains(server.get()));
    }

    @Test
    void randomRoutingPreferInProc() {
        Map<String, Boolean> allServers = Map.of("server1", true, "server2", false, "server3", false);

        Map<String, Integer> trafficAssignment = Map.of("group1", 5);
        Map<String, Set<String>> groupAssignment = Map.of(
            "group1", Set.of("server1", "server2", "server3")
        );
        WeightedServerGroupRouter router =
            new WeightedServerGroupRouter(allServers, trafficAssignment, groupAssignment);

        Optional<String> server = router.random();
        assertTrue(server.isPresent());
        assertEquals("server1", server.get());
    }

    @Test
    void inProcIgnored() {
        Map<String, Boolean> allServers = Map.of("server1", true, "server2", false, "server3", false, "server4", false);

        Map<String, Integer> trafficAssignment = Map.of("group1", 5);
        Map<String, Set<String>> groupAssignment = Map.of(
            "group1", Set.of("server1", "server2", "server3")
        );

        WeightedServerGroupRouter router =
            new WeightedServerGroupRouter(allServers, trafficAssignment, groupAssignment);

        Optional<String> server = router.random();
        assertTrue(server.isPresent());
        assertNotEquals(server.get(), "server4");

        server = router.roundRobin();
        assertTrue(server.isPresent());
        assertNotEquals(server.get(), "server4");
    }

    @Test
    void roundRobinRouting() {
        Map<String, Boolean> allServers = Map.of("server1", false, "server2", false, "server3", false);

        Map<String, Integer> trafficAssignment = Map.of("group1", 5);
        Map<String, Set<String>> groupAssignment = Map.of(
            "group1", Set.of("server1", "server2")
        );

        WeightedServerGroupRouter router =
            new WeightedServerGroupRouter(allServers, trafficAssignment, groupAssignment);

        // First round-robin call should return server1, then server2
        assertEquals(router.roundRobin().get(), "server1");
        assertEquals(router.roundRobin().get(), "server2");
        assertEquals(router.roundRobin().get(), "server1");
    }

    @Test
    void roundRobinPreferInProc() {
        Map<String, Boolean> allServers = Map.of("server1", true, "server2", false, "server3", false);

        Map<String, Integer> trafficAssignment = Map.of("group1", 5);
        Map<String, Set<String>> groupAssignment = Map.of(
            "group1", Set.of("server1", "server2")
        );

        WeightedServerGroupRouter router =
            new WeightedServerGroupRouter(allServers, trafficAssignment, groupAssignment);

        assertEquals(router.roundRobin().get(), "server1");
        assertEquals(router.roundRobin().get(), "server1");
        assertEquals(router.roundRobin().get(), "server1");
    }

    @Test
    void hashingRouting() {
        Map<String, Boolean> allServers = Map.of("server1", false, "server2", false, "server3", false);

        Map<String, Integer> trafficAssignment = Map.of("group1", 5);
        Map<String, Set<String>> groupAssignment = Map.of(
            "group1", Set.of("server1", "server2", "server3")
        );

        WeightedServerGroupRouter router =
            new WeightedServerGroupRouter(allServers, trafficAssignment, groupAssignment);

        String key = "myKey";
        Optional<String> server = router.hashing(key);
        assertTrue(server.isPresent());
        assertTrue(Set.of("server1", "server2", "server3").contains(server.get()));
    }

    @Test
    void stickHashingRouting() {
        Map<String, Boolean> allServers = Map.of("server1", false, "server2", false, "server3", false);
        Map<String, Boolean> allServersWithInProc = Map.of("server1", false, "server2", true, "server3", false);
        Map<String, Integer> trafficAssignment = Map.of("group1", 5);
        Map<String, Set<String>> groupAssignment = Map.of(
            "group1", Set.of("server1", "server2", "server3")
        );
        WeightedServerGroupRouter router =
            new WeightedServerGroupRouter(allServers, trafficAssignment, groupAssignment);
        WeightedServerGroupRouter routerWithInProc =
            new WeightedServerGroupRouter(allServersWithInProc, trafficAssignment, groupAssignment);
        String key = "myKey";
        Optional<String> server = router.stickyHashing(key);
        assertTrue(server.isPresent());
        assertEquals(server.get(), "server1");

        server = routerWithInProc.stickyHashing(key);
        assertTrue(server.isPresent());
        assertEquals(server.get(), "server2");
    }
}
