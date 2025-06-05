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

package org.apache.bifromq.dist.worker;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.intersect;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.isNULLRange;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.tenantBeginKey;

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class TenantsState implements ITenantsState {
    private final Map<String, TenantRouteState> tenantRouteStates = new ConcurrentHashMap<>();
    private final IKVCloseableReader reader;
    private final String[] tags;
    private transient Boundary boundary;

    TenantsState(IKVCloseableReader reader, String... tags) {
        this.reader = reader;
        this.tags = tags;
        boundary = reader.boundary();
    }

    @Override
    public void incNormalRoutes(String tenantId) {
        incNormalRoutes(tenantId, 1);
    }

    @Override
    public void incNormalRoutes(String tenantId, int count) {
        assert count > 0;
        tenantRouteStates.computeIfAbsent(tenantId,
            k -> new TenantRouteState(tenantId, getSpaceUsageProvider(tenantId), tags)).addNormalRoutes(count);
    }

    @Override
    public void decNormalRoutes(String tenantId) {
        decSharedRoutes(tenantId, 1);
    }

    @Override
    public void decNormalRoutes(String tenantId, int count) {
        assert count > 0;
        tenantRouteStates.computeIfPresent(tenantId, (k, v) -> {
            v.addNormalRoutes(-count);
            if (v.isNoRoutes()) {
                v.destroy();
                return null;
            }
            return v;
        });
    }

    @Override
    public void incSharedRoutes(String tenantId) {
        incSharedRoutes(tenantId, 1);
    }

    @Override
    public void incSharedRoutes(String tenantId, int count) {
        assert count > 0;
        tenantRouteStates.computeIfAbsent(tenantId,
            k -> new TenantRouteState(tenantId, getSpaceUsageProvider(tenantId), tags)).addSharedRoutes(count);
    }

    @Override
    public void decSharedRoutes(String tenantId) {
        decSharedRoutes(tenantId, 1);
    }

    @Override
    public void decSharedRoutes(String tenantId, int count) {
        assert count > 0;
        tenantRouteStates.computeIfPresent(tenantId, (k, v) -> {
            v.addSharedRoutes(-count);
            if (v.isNoRoutes()) {
                v.destroy();
                return null;
            }
            return v;
        });
    }

    @Override
    public void reset() {
        tenantRouteStates.values().forEach(TenantRouteState::destroy);
        tenantRouteStates.clear();
        boundary = reader.boundary();
    }

    @Override
    public void close() {
        reset();
        reader.close();
    }

    private Supplier<Number> getSpaceUsageProvider(String tenantId) {
        return () -> {
            try {
                ByteString tenantStartKey = tenantBeginKey(tenantId);
                Boundary tenantSection = intersect(boundary, toBoundary(tenantStartKey, upperBound(tenantStartKey)));
                if (isNULLRange(tenantSection)) {
                    return 0;
                }
                return reader.size(tenantSection);
            } catch (Exception e) {
                log.error("Unexpected error", e);
                return 0;
            }
        };
    }
}
