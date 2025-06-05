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

package org.apache.bifromq.dist.worker.spi;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.tenantBeginKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.tenantRouteStartKey;

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.util.TopicUtil;
import com.google.protobuf.ByteString;

/**
 * Utility for generating valid split key for dist worker.
 */
public final class SplitKey {
    /**
     * The key boundary of the tenant.
     *
     * @param tenantId the tenant id
     * @return the boundary
     */
    public static Boundary tenantBoundary(String tenantId) {
        ByteString tenantBeginKey = tenantBeginKey(tenantId);
        return toBoundary(tenantBeginKey, upperBound(tenantBeginKey));
    }

    /**
     * The boundary of the route range for given topic filter.
     *
     * @param tenantId    the tenant id
     * @param topicFilter the topic filter
     * @return the boundary
     */
    public static Boundary routeBoundary(String tenantId, String topicFilter) {
        ByteString routeStartKey = tenantRouteStartKey(tenantId, TopicUtil.from(topicFilter).getFilterLevelList());
        return toBoundary(routeStartKey, upperBound(routeStartKey));
    }
}
