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

package org.apache.bifromq.plugin.resourcethrottler;

import org.pf4j.ExtensionPoint;

public interface IResourceThrottler extends ExtensionPoint {
    /**
     * Determine if the tenant has enough resource of given type.
     *
     * @param tenantId the id of the tenant
     * @param type     the resource type
     * @return true for there is enough resource of given type for the tenant
     */
    boolean hasResource(String tenantId, TenantResourceType type);

    /**
     * Will be called during broker shutdown.
     */
    default void close() {

    }
}
