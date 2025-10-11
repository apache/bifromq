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
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_RANGE_BOUND_BYTES;
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_STATE_BYTES;
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_VER_BYTES;

import com.google.protobuf.ByteString;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceMigratableWriter;
import org.apache.bifromq.basekv.localengine.IKVSpaceWriter;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.store.api.IKVWriter;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;

public class KVRangeWriter extends AbstractKVRangeMetadataUpdatable<KVRangeWriter>
    implements IKVRangeWriter<KVRangeWriter> {
    private final ICPableKVSpace space;
    private final IKVSpaceMigratableWriter spaceWriter;
    private final Set<IKVRangeRestoreSession> activeRestoreSessions = new HashSet<>();

    public KVRangeWriter(KVRangeId id, ICPableKVSpace space) {
        super(id, space);
        this.space = space;
        this.spaceWriter = space.toWriter();
    }

    @Override
    protected IKVSpaceWriter keyRangeWriter() {
        return spaceWriter;
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
        keyRangeWriter().abort();
        for (IKVRangeRestoreSession session : activeRestoreSessions) {
            session.abort();
        }
        activeRestoreSessions.clear();
    }

    @Override
    public int count() {
        return keyRangeWriter().count();
    }

    @Override
    public void done() {
        keyRangeWriter().done();
        for (IKVRangeRestoreSession session : activeRestoreSessions) {
            session.done();
        }
        activeRestoreSessions.clear();
    }

    @Override
    public void reset() {
        keyRangeWriter().reset();
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
