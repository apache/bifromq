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

package org.apache.bifromq.basekv.store;

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class KVRangeConfig {
    private final String clusterId;
    public final KVRangeId id;
    public final long ver;
    public final Boundary boundary;
    public final String leader;
    public final ClusterConfig clusterConfig;

    public KVRangeConfig(String clusterId, String leaderStoreId, KVRangeDescriptor desc) {
        this.clusterId = clusterId;
        id = desc.getId();
        ver = desc.getVer();
        boundary = desc.getBoundary();
        leader = leaderStoreId;
        clusterConfig = desc.getConfig();
    }
}
