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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.bifromq.type.RouteMatcher;

/**
 * Represent a group matching route.
 */
@EqualsAndHashCode(callSuper = true, cacheStrategy = EqualsAndHashCode.CacheStrategy.LAZY)
@ToString
public final class GroupMatching extends Matching {
    @EqualsAndHashCode.Exclude
    public final boolean ordered;
    @EqualsAndHashCode.Exclude
    public final List<NormalMatching> receiverList;
    private final Map<String, Long> receivers;

    GroupMatching(String tenantId, RouteMatcher matcher, Map<String, Long> members) {
        super(tenantId, matcher);
        assert matcher.getType() != RouteMatcher.Type.Normal;
        this.receivers = members;

        this.ordered = matcher.getType() == RouteMatcher.Type.OrderedShare;
        this.receiverList = members.entrySet().stream()
            .map(e -> new NormalMatching(tenantId, matcher, e.getKey(), e.getValue()))
            .collect(Collectors.toList());
    }

    @Override
    public Type type() {
        return Type.Group;
    }

    public Map<String, Long> receivers() {
        return receivers;
    }
}
