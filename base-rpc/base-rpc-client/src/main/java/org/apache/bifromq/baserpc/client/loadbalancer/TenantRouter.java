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

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;

class TenantRouter implements ITenantRouter {
    private final Map<String, Boolean> allServers;
    private final TrieMap<WeightedServerGroupRouter> matcher = new TrieMap<>();

    TenantRouter() {
        this(emptyMap(), emptyMap(), emptyMap());
    }

    // trafficDirective: tenantIdPrefix -> groupTag -> weight
    // serverGroupTags: serverId -> groupTags
    TenantRouter(Map<String, Boolean> allServers,
                 Map<String, Map<String, Integer>> trafficDirective,
                 Map<String, Set<String>> serverGroupTags) {
        this.allServers = allServers;

        Set<String> defaultLBGroup = Sets.newHashSet();
        Map<String, Set<String>> lbGroups = Maps.newHashMap();
        for (String serverId : serverGroupTags.keySet()) {
            Set<String> groupTags = serverGroupTags.get(serverId);
            if (groupTags.isEmpty()) {
                // no group tag assigned, add it to default group
                defaultLBGroup.add(serverId);
            } else {
                groupTags.forEach(lbGroupTag ->
                    lbGroups.computeIfAbsent(lbGroupTag, l -> Sets.newHashSet()).add(serverId));
            }
        }
        // default group is used as fallback assignment
        // make sure there is always a matcher for any tenantId
        prepareMatcher("", singletonMap("", 1), singletonMap("", defaultLBGroup));

        for (String tenantIdPrefix : trafficDirective.keySet()) {
            if (tenantIdPrefix.isEmpty()) {
                continue;
            }
            prepareMatcher(tenantIdPrefix, trafficDirective.get(tenantIdPrefix), lbGroups);
        }
    }

    private void prepareMatcher(String tenantIdPrefix,
                                Map<String, Integer> serverWeights,
                                Map<String, Set<String>> serverGroups) {
        matcher.put(tenantIdPrefix, new WeightedServerGroupRouter(allServers, serverWeights, serverGroups));
    }

    public IServerGroupRouter get(String tenantId) {
        return matcher.bestMatch(tenantId);
    }
}
