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

package org.apache.bifromq.retain.store;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.bifromq.basekv.store.api.IKVReader;

class TenantsStats {
    private final Map<String, TenantStats> retainedSet = new ConcurrentHashMap<>();
    private final IKVReader reader;
    private final String[] tags;

    TenantsStats(IKVReader reader, String... tags) {
        this.reader = reader;
        this.tags = tags;
    }

    void increaseTopicCount(String tenantId, int delta) {
        retainedSet.compute(tenantId, (k, v) -> {
            if (v == null) {
                v = new TenantStats(tenantId, reader, tags);
            }
            if (v.incrementTopicCount(delta) == 0) {
                v.destroy();
                return null;
            }
            return v;
        });
    }

    public void toggleMetering(boolean isLeader) {
        retainedSet.values().forEach(s -> s.toggleMetering(isLeader));
    }

    void destroy() {
        retainedSet.values().forEach(TenantStats::destroy);
        retainedSet.clear();
    }
}
