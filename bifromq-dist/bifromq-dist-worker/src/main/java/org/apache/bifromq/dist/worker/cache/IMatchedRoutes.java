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
import org.apache.bifromq.dist.worker.schema.Matching;

/**
 * The result of matching a topic within a boundary.
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

}
