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

package org.apache.bifromq.basekv.store.range;

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;

/**
 * A builder-like interface to update the KVRange metadata.
 *
 * @param <T> the type of the updatable
 */
interface IKVRangeMetadataUpdatable<T extends IKVRangeMetadataUpdatable<T>> {
    /**
     * Set the version to a specific value.
     *
     * @param ver the version to set
     * @return the updatable
     */
    T ver(long ver);

    /**
     * Set the last applied index.
     *
     * @param lastAppliedIndex the last applied index
     * @return the updatable
     */
    T lastAppliedIndex(long lastAppliedIndex);

    /**
     * Set the boundary.
     *
     * @param boundary the boundary
     * @return the updatable
     */
    T boundary(Boundary boundary);

    /**
     * Set the state.
     *
     * @param state the state
     * @return the updatable
     */
    T state(State state);

    /**
     * Set the cluster configuration.
     *
     * @param clusterConfig the cluster configuration
     * @return the updatable
     */
    T clusterConfig(ClusterConfig clusterConfig);
}
