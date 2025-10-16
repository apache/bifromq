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

import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_LAST_APPLIED_INDEX_BYTES;
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_STATE_BYTES;
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_VER_BYTES;

import com.google.protobuf.ByteString;
import java.util.Optional;
import org.apache.bifromq.basekv.localengine.IKVSpaceReader;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVRangeReader;
import org.apache.bifromq.basekv.store.util.KVUtil;

class KVRangeReader implements IKVRangeReader {
    protected final IKVSpaceReader kvSpaceReader;

    public KVRangeReader(IKVSpaceReader spaceReader) {
        this.kvSpaceReader = spaceReader;
    }

    @Override
    public final long version() {
        return kvSpaceReader.metadata(METADATA_VER_BYTES).map(KVUtil::toLongNativeOrder).orElse(-1L);
    }

    @Override
    public final State state() {
        return kvSpaceReader.metadata(METADATA_STATE_BYTES)
            .map(stateBytes -> {
                try {
                    return State.parseFrom(stateBytes);
                } catch (Throwable e) {
                    return State.newBuilder().setType(State.StateType.NoUse).build();
                }
            })
            .orElse(State.newBuilder().setType(State.StateType.NoUse).build());
    }

    @Override
    public final long lastAppliedIndex() {
        return kvSpaceReader.metadata(METADATA_LAST_APPLIED_INDEX_BYTES).map(KVUtil::toLong).orElse(-1L);
    }

    @Override
    public final Boundary boundary() {
        return kvSpaceReader.metadata(KVRangeKeys.METADATA_RANGE_BOUND_BYTES)
            .map(boundaryBytes -> {
                try {
                    return Boundary.parseFrom(boundaryBytes);
                } catch (Throwable e) {
                    return Boundary.getDefaultInstance();
                }
            })
            .orElse(Boundary.getDefaultInstance());
    }

    @Override
    public final ClusterConfig clusterConfig() {
        return kvSpaceReader.metadata(KVRangeKeys.METADATA_CLUSTER_CONFIG_BYTES)
            .map(clusterConfigBytes -> {
                try {
                    return ClusterConfig.parseFrom(clusterConfigBytes);
                } catch (Throwable e) {
                    return ClusterConfig.getDefaultInstance();
                }
            })
            .orElse(ClusterConfig.getDefaultInstance());
    }

    @Override
    public final long size(Boundary boundary) {
        return kvSpaceReader.size(boundary);
    }

    @Override
    public final boolean exist(ByteString key) {
        return kvSpaceReader.exist(key);
    }

    @Override
    public final Optional<ByteString> get(ByteString key) {
        return kvSpaceReader.get(key);
    }

    @Override
    public final IKVIterator iterator() {
        return new KVIterator(kvSpaceReader.newIterator());
    }

    @Override
    public final IKVIterator iterator(Boundary boundary) {
        return new KVIterator(kvSpaceReader.newIterator(boundary));
    }

    @Override
    public final void close() {
        kvSpaceReader.close();
    }
}
