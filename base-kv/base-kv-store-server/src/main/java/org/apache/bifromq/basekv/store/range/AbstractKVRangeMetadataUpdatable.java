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

import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_CLUSTER_CONFIG_BYTES;
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_LAST_APPLIED_INDEX_BYTES;
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_RANGE_BOUND_BYTES;
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_STATE_BYTES;
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_VER_BYTES;
import static org.apache.bifromq.basekv.store.util.VerUtil.bump;

import org.apache.bifromq.basekv.localengine.IKVSpaceMetadata;
import org.apache.bifromq.basekv.localengine.IKVSpaceMetadataUpdatable;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.store.util.KVUtil;

abstract class AbstractKVRangeMetadataUpdatable<T extends AbstractKVRangeMetadataUpdatable<T>>
    extends AbstractKVRangeMetadata implements IKVRangeMetadataUpdatable<T> {

    AbstractKVRangeMetadataUpdatable(KVRangeId id, IKVSpaceMetadata keyRangeMetadata) {
        super(id, keyRangeMetadata);
    }

    @Override
    public final T bumpVer(boolean boundaryChange) {
        resetVer(bump(version(), boundaryChange));
        return thisT();
    }

    @Override
    public final T resetVer(long ver) {
        keyRangeWriter().metadata(METADATA_VER_BYTES, KVUtil.toByteStringNativeOrder(ver));
        return thisT();
    }

    @Override
    public final T lastAppliedIndex(long lastAppliedIndex) {
        keyRangeWriter().metadata(METADATA_LAST_APPLIED_INDEX_BYTES, KVUtil.toByteString(lastAppliedIndex));
        return thisT();
    }

    @Override
    public final T boundary(Boundary boundary) {
        keyRangeWriter().metadata(METADATA_RANGE_BOUND_BYTES, boundary.toByteString());
        return thisT();
    }

    @Override
    public final T state(State state) {
        keyRangeWriter().metadata(METADATA_STATE_BYTES, state.toByteString());
        return thisT();
    }

    @Override
    public final T clusterConfig(ClusterConfig clusterConfig) {
        keyRangeWriter().metadata(METADATA_CLUSTER_CONFIG_BYTES, clusterConfig.toByteString());
        return thisT();
    }

    @SuppressWarnings("unchecked")
    private T thisT() {
        return (T) this;
    }

    protected abstract IKVSpaceMetadataUpdatable<?> keyRangeWriter();
}
