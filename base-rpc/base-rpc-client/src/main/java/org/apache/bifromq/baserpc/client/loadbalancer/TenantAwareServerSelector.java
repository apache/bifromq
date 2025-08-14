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

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
class TenantAwareServerSelector implements IServerSelector {
    private final Map<String, Boolean> allServers;
    private final Map<String, Set<String>> serverGroupTags;
    private final Map<String, Map<String, Integer>> trafficDirective;
    @EqualsAndHashCode.Exclude
    private final ITenantRouter tenantRouter;

    public TenantAwareServerSelector(Map<String, Boolean> allServers,
                                     Map<String, Set<String>> serverGroupTags,
                                     Map<String, Map<String, Integer>> trafficDirective) {
        this.allServers = Maps.newHashMap(allServers);
        this.serverGroupTags = Maps.newHashMap(serverGroupTags);
        this.trafficDirective = Maps.newHashMap(trafficDirective);
        this.tenantRouter = new TenantRouter(this.allServers, this.trafficDirective, this.serverGroupTags);
    }

    @Override
    public boolean exists(String serverId) {
        return allServers.containsKey(serverId);
    }

    @Override
    public IServerGroupRouter get(String tenantId) {
        return tenantRouter.get(tenantId);
    }

    @Override
    public String toString() {
        return allServers.toString();
    }
}
