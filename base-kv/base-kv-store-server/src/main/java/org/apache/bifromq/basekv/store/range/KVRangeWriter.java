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

import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_CLUSTER_CONFIG_BYTES;
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_RANGE_BOUND_BYTES;
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_STATE_BYTES;
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_VER_BYTES;

import org.apache.bifromq.basekv.localengine.IKVSpaceWriter;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.store.api.IKVWriter;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.protobuf.ByteString;
import java.util.Optional;

public class KVRangeWriter extends AbstractKVRangeMetadataUpdatable<KVRangeWriter>
    implements IKVRangeWriter<KVRangeWriter> {
    private final IKVSpaceWriter spaceWriter;

    public KVRangeWriter(KVRangeId id, IKVSpaceWriter spaceWriter) {
        super(id, spaceWriter);
        this.spaceWriter = spaceWriter;
    }

    @Override
    protected IKVSpaceWriter keyRangeWriter() {
        return spaceWriter;
    }

    @Override
    public IKVRangeMetadataWriter<?> migrateTo(KVRangeId targetRangeId, Boundary boundary) {
        return new KVRangeMetadataWriter(targetRangeId,
            spaceWriter.migrateTo(KVRangeIdUtil.toString(targetRangeId), boundary));
    }

    @Override
    public IKVRangeMetadataWriter<?> migrateFrom(KVRangeId fromRangeId, Boundary boundary) {
        return new KVRangeMetadataWriter(fromRangeId,
            spaceWriter.migrateFrom(KVRangeIdUtil.toString(fromRangeId), boundary));
    }

    @Override
    public IKVWriter kvWriter() {
        return new KVWriter(spaceWriter);
    }

    @Override
    public void abort() {
        keyRangeWriter().abort();
    }

    @Override
    public int count() {
        return keyRangeWriter().count();
    }

    @Override
    public void done() {
        keyRangeWriter().done();
    }

    @Override
    public long version() {
        Optional<ByteString> verBytes = spaceWriter.metadata(METADATA_VER_BYTES);
        return version(verBytes.orElse(null));
    }

    @Override
    public State state() {
        Optional<ByteString> stateData = spaceWriter.metadata(METADATA_STATE_BYTES);
        return state(stateData.orElse(null));
    }

    @Override
    public Boundary boundary() {
        return boundary(spaceWriter.metadata(METADATA_RANGE_BOUND_BYTES).orElse(null));
    }

    @Override
    public ClusterConfig clusterConfig() {
        return clusterConfig(spaceWriter.metadata(METADATA_CLUSTER_CONFIG_BYTES).orElse(null));
    }
}
