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
import java.util.HashSet;
import java.util.Set;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceMigratableWriter;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.store.api.IKVWriter;
import org.apache.bifromq.basekv.store.util.KVUtil;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;

class KVRangeWriter implements IKVRangeWriter<KVRangeWriter> {
    private final KVRangeId id;
    private final ICPableKVSpace space;
    private final IKVSpaceMigratableWriter spaceWriter;
    private final Set<IKVRangeRestoreSession> activeRestoreSessions = new HashSet<>();

    KVRangeWriter(KVRangeId id, ICPableKVSpace space) {
        this.id = id;
        this.space = space;
        this.spaceWriter = space.toWriter();
    }

    @Override
    public KVRangeId id() {
        return id;
    }

    @Override
    public final KVRangeWriter ver(long ver) {
        spaceWriter.metadata(METADATA_VER_BYTES, KVUtil.toByteStringNativeOrder(ver));
        return this;
    }

    @Override
    public final KVRangeWriter lastAppliedIndex(long lastAppliedIndex) {
        spaceWriter.metadata(METADATA_LAST_APPLIED_INDEX_BYTES, KVUtil.toByteString(lastAppliedIndex));
        return this;
    }

    @Override
    public final KVRangeWriter boundary(Boundary boundary) {
        spaceWriter.metadata(METADATA_RANGE_BOUND_BYTES, boundary.toByteString());
        return this;
    }

    @Override
    public final KVRangeWriter state(State state) {
        spaceWriter.metadata(METADATA_STATE_BYTES, state.toByteString());
        return this;
    }

    @Override
    public final KVRangeWriter clusterConfig(ClusterConfig clusterConfig) {
        spaceWriter.metadata(METADATA_CLUSTER_CONFIG_BYTES, clusterConfig.toByteString());
        return this;
    }

    @Override
    public void migrateTo(KVRangeId targetRangeId, KVRangeSnapshot snapshot) {
        activeRestoreSessions.add(new KVRangeRestoreSession(
            spaceWriter.migrateTo(KVRangeIdUtil.toString(targetRangeId), snapshot.getBoundary()))
            .ver(snapshot.getVer())
            .lastAppliedIndex(snapshot.getLastAppliedIndex())
            .boundary(snapshot.getBoundary())
            .state(snapshot.getState())
            .clusterConfig(snapshot.getClusterConfig()));
    }

    @Override
    public Migrater startMerging(MigrationProgressListener progressListener) {
        IKVRangeRestoreSession restoreSession = new KVRangeRestoreSession(
            space.startReceiving(progressListener::onProgress));
        activeRestoreSessions.add(restoreSession);
        return new Migrater() {
            @Override
            public Migrater ver(long ver) {
                restoreSession.ver(ver);
                return this;
            }

            @Override
            public Migrater state(State state) {
                restoreSession.state(state);
                return this;
            }

            @Override
            public Migrater boundary(Boundary boundary) {
                restoreSession.boundary(boundary);
                return this;
            }

            @Override
            public void put(ByteString key, ByteString value) {
                restoreSession.put(key, value);
            }

            @Override
            public void abort() {
                restoreSession.abort();
            }
        };
    }

    @Override
    public IKVWriter kvWriter() {
        return new KVWriter(spaceWriter);
    }

    @Override
    public void abort() {
        spaceWriter.abort();
        for (IKVRangeRestoreSession session : activeRestoreSessions) {
            session.abort();
        }
        activeRestoreSessions.clear();
    }

    @Override
    public int count() {
        return spaceWriter.count();
    }

    @Override
    public void done() {
        spaceWriter.done();
        for (IKVRangeRestoreSession session : activeRestoreSessions) {
            session.done();
        }
        activeRestoreSessions.clear();
    }
}
