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

package org.apache.bifromq.baserpc.trafficgovernor;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * The interface for the traffic governor of a RPC service.
 */
public interface IRPCServiceTrafficGovernor extends IRPCServiceLandscape {
    /**
     * Update the groupTags for a server. If the server not join yet, nothing happens.
     *
     * @param serverId  the serverId of the server
     * @param groupTags the groupTags to be assigned
     */
    CompletableFuture<Void> setServerGroups(String serverId, Set<String> groupTags);

    /**
     * Set the traffic directive for a tenantIdPrefix.
     *
     * @param tenantIdPrefix the tenantIdPrefix
     * @param loadAssignment the loadAssignment in the form of groupTag -> weight
     * @return the future
     */
    CompletableFuture<Void> setTrafficRules(String tenantIdPrefix, Map<String, Integer> loadAssignment);

    /**
     * Unset the traffic directive for a tenantIdPrefix if exists.
     *
     * @param tenantIdPrefix the tenantIdPrefix
     * @return the future
     */
    CompletableFuture<Void> unsetTrafficRules(String tenantIdPrefix);
}
