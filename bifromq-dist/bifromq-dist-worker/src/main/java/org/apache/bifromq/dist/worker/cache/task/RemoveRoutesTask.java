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

package org.apache.bifromq.dist.worker.cache.task;

import java.util.NavigableMap;
import java.util.Set;
import org.apache.bifromq.dist.worker.schema.Matching;
import org.apache.bifromq.type.RouteMatcher;

public class RemoveRoutesTask extends RefreshEntriesTask {

    private RemoveRoutesTask(
        NavigableMap<RouteMatcher, Set<Matching>> removed) {
        super(removed);
    }

    public static RemoveRoutesTask of(NavigableMap<RouteMatcher, Set<Matching>> added) {
        return new RemoveRoutesTask(added);
    }

    @Override
    public CacheTaskType type() {
        return CacheTaskType.RemoveRoutes;
    }
}
