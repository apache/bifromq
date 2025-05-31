/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.pf4j.Extension;

@Extension
public class ResourceThrottlerTestStub implements IResourceThrottler {
    private final Map<String, Map<TenantResourceType, Boolean>> throttlingMap = new HashMap<>();

    @Override
    public boolean hasResource(String tenantId, TenantResourceType type) {
        return throttlingMap.getOrDefault(tenantId, Collections.emptyMap()).getOrDefault(type, true);
    }

    public void setResource(String tenantId, TenantResourceType type, boolean hasResource) {
        throttlingMap.computeIfAbsent(tenantId, k -> new HashMap<>()).put(type, hasResource);
    }
}
