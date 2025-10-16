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

import com.google.protobuf.ByteString;
import org.apache.bifromq.basekv.localengine.IRestoreSession;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.store.util.KVUtil;

class KVRangeRestoreSession implements IKVRangeRestoreSession {
    private final IRestoreSession restoreSession;

    KVRangeRestoreSession(IRestoreSession restoreSession) {
        this.restoreSession = restoreSession;
    }

    @Override
    public void done() {
        restoreSession.done();
    }

    @Override
    public void abort() {
        restoreSession.abort();
    }

    @Override
    public int count() {
        return restoreSession.count();
    }

    @Override
    public IKVRangeRestoreSession ver(long ver) {
        restoreSession.metadata(METADATA_VER_BYTES, KVUtil.toByteStringNativeOrder(ver));
        return this;
    }

    @Override
    public IKVRangeRestoreSession lastAppliedIndex(long lastAppliedIndex) {
        restoreSession.metadata(METADATA_LAST_APPLIED_INDEX_BYTES, KVUtil.toByteString(lastAppliedIndex));
        return this;
    }

    @Override
    public IKVRangeRestoreSession boundary(Boundary boundary) {
        restoreSession.metadata(METADATA_RANGE_BOUND_BYTES, boundary.toByteString());
        return this;
    }

    @Override
    public IKVRangeRestoreSession state(State state) {
        restoreSession.metadata(METADATA_STATE_BYTES, state.toByteString());
        return this;
    }

    @Override
    public IKVRangeRestoreSession clusterConfig(ClusterConfig clusterConfig) {
        restoreSession.metadata(METADATA_CLUSTER_CONFIG_BYTES, clusterConfig.toByteString());
        return this;
    }

    @Override
    public IKVRangeRestoreSession put(ByteString key, ByteString value) {
        restoreSession.put(key, value);
        return this;
    }
}
