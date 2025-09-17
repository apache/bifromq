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

import java.util.Set;
import org.apache.bifromq.dist.worker.schema.GroupMatching;
import org.apache.bifromq.dist.worker.schema.Matching;
import org.apache.bifromq.dist.worker.schema.NormalMatching;

/**
 * A mutable matched routes for a topic.
 */
public interface IMatchedRoutes {
    /**
     * The maximum persistent fanout count for the matched topic.
     *
     * @return the maximum count of persistent fanout matches
     */
    int maxPersistentFanout();

    /**
     * The maximum group fanout count for the matched topic.
     *
     * @return the maximum count of group fanout matches
     */
    int maxGroupFanout();

    /**
     * The persistent fanout count for the matched topic.
     *
     * @return the count of persistent fanout matches
     */
    int persistentFanout();

    /**
     * The count of group fanout for the matched topic.
     *
     * @return the count of group fanout
     */
    int groupFanout();

    /**
     * The matched routes.
     *
     * @return the matched routes.
     */
    Set<Matching> routes();

    /**
     * Add a normal matching to the matched routes.
     *
     * @param matching the normal matching to add
     */
    AddResult addNormalMatching(NormalMatching matching);

    /**
     * Remove a normal matching from the matched routes.
     *
     * @param matching the normal matching to remove
     */
    void removeNormalMatching(NormalMatching matching);

    /**
     * Add a new or override an existing group matching to the matched routes.
     *
     * @param matching the group matching to add or override
     */
    AddResult putGroupMatching(GroupMatching matching);

    /**
     * Remove an existing group matching.
     *
     * @param matching the group matching to remove
     */
    void removeGroupMatching(GroupMatching matching);

    /**
     * Adjust matched routes to new fanout limits. If the current fanout exceeds the new limits, the matched routes
     * will be clamped to the new limits by removing excess persistent fanout matchings.
     * If the new limits are higher than the previous and the current fanout is already at the previous limits,
     * ReloadNeeded is returned.
     *
     * @param newMaxPersistentFanout the new maximum persistent fanout
     * @param newMaxGroupFanout the new maximum group fanout
     * @return the adjust result
     */
    AdjustResult adjust(int newMaxPersistentFanout, int newMaxGroupFanout);

    enum AdjustResult {
        Clamped, Adjusted, ReloadNeeded
    }

    enum AddResult {
        Added, Exists, ExceedFanoutLimit
    }
}
