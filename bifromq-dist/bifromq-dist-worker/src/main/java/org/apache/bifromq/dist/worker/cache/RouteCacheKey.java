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

package org.apache.bifromq.dist.worker.cache;

import java.util.concurrent.atomic.AtomicReference;
import lombok.EqualsAndHashCode;
import org.apache.bifromq.basekv.proto.Boundary;

@EqualsAndHashCode(cacheStrategy = EqualsAndHashCode.CacheStrategy.LAZY)
public final class RouteCacheKey {
    public final String topic;
    public final Boundary matchRecordBoundary;
    @EqualsAndHashCode.Exclude
    public final AtomicReference<IMatchedRoutes> cachedMatchedRoutes = new AtomicReference<>();

    public RouteCacheKey(String topic, Boundary matchRecordBoundary) {
        this.topic = topic;
        this.matchRecordBoundary = matchRecordBoundary;
    }
}
