/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.basekv.store.range;

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.store.api.IKVRangeMetadata;

public interface IKVRangeMetadataUpdatable<T extends IKVRangeMetadataUpdatable<T>> extends IKVRangeMetadata {
    T bumpVer(boolean boundaryChange);

    T resetVer(long ver);

    T lastAppliedIndex(long lastAppliedIndex);

    T boundary(Boundary boundary);

    T state(State state);

    T clusterConfig(ClusterConfig clusterConfig);
}
