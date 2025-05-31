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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class TrafficRouterTest {
    @Test
    void getRouterForTenant() {
        Map<String, Boolean> servers = Map.of("server1", false, "server2", false);

        Map<String, Map<String, Integer>> trafficDirective = Map.of(
            "tenantA", Map.of("group1", 5),
            "tenantB", Map.of("group2", 3)
        );

        Map<String, Set<String>> serverGroups = Map.of(
            "server1", Set.of("group1"),
            "server2", Set.of("group2")
        );

        TenantRouter router = new TenantRouter(servers, trafficDirective, serverGroups);

        IServerGroupRouter tenantARouter = router.get("tenantA");
        assertNotNull(tenantARouter);
        assertEquals("server1", tenantARouter.random().get());

        IServerGroupRouter tenantBRouter = router.get("tenantB");
        assertNotNull(tenantBRouter);
        assertEquals("server2", tenantBRouter.random().get());

        IServerGroupRouter unknownTenantRouter = router.get("unknownTenant");
        assertNotNull(unknownTenantRouter);
        assertFalse(unknownTenantRouter.random().isPresent());
    }
}
