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

package org.apache.bifromq.dist.worker.schema.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Interner;
import org.apache.bifromq.dist.rpc.proto.RouteGroup;
import org.apache.bifromq.dist.worker.schema.GroupMatching;
import org.apache.bifromq.dist.worker.schema.Matching;
import org.apache.bifromq.dist.worker.schema.RouteDetail;
import org.apache.bifromq.type.RouteMatcher;

public class GroupMatchingCache {
    private static final Interner<CacheKey> CACHE_KEY_INTERNER = Interner.newWeakInterner();
    private static final Cache<CacheKey, Matching> GROUP_MATCHING_CACHE = Caffeine.newBuilder().weakKeys()
        .build();
    private static final Interner<Matching> MATCHING_INTERNER = Interner.newWeakInterner();

    public static Matching get(RouteDetail routeDetail, RouteGroup group) {
        assert routeDetail.matcher().getType() != RouteMatcher.Type.Normal;
        CacheKey key = CACHE_KEY_INTERNER.intern(new CacheKey(routeDetail, group));
        return GROUP_MATCHING_CACHE.get(key, k ->
            MATCHING_INTERNER.intern(new GroupMatching(k.routeDetail().tenantId(), k.routeDetail.matcher(),
                k.routeGroup().getMembersMap())));
    }

    private record CacheKey(RouteDetail routeDetail, RouteGroup routeGroup) {
    }
}
