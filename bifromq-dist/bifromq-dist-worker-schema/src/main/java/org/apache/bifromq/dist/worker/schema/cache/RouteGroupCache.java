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
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.bifromq.dist.rpc.proto.RouteGroup;

public class RouteGroupCache {
    private static final Interner<ByteString> ROUTE_GROUP_BYTES_INTERNER = Interner.newWeakInterner();
    private static final Interner<RouteGroup> ROUTE_GROUP_INTERNER = Interner.newWeakInterner();
    private static final Cache<ByteString, RouteGroup> ROUTE_GROUP_CACHE = Caffeine.newBuilder().weakKeys().build();

    public static RouteGroup get(ByteString routeGroupBytes) {
        routeGroupBytes = ROUTE_GROUP_BYTES_INTERNER.intern(routeGroupBytes);
        return ROUTE_GROUP_CACHE.get(routeGroupBytes, k -> {
            try {
                return ROUTE_GROUP_INTERNER.intern(RouteGroup.parseFrom(k));
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException("Unable to parse matching record", e);
            }
        });
    }
}
