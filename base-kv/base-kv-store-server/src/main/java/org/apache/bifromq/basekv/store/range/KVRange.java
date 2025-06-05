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
import static org.apache.bifromq.basekv.utils.BoundaryUtil.NULL_BOUNDARY;

import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import org.apache.bifromq.basekv.store.api.IKVRangeReader;
import org.apache.bifromq.basekv.store.api.IKVReader;
import org.apache.bifromq.basekv.store.api.IKVWriter;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Getter;
import lombok.SneakyThrows;

public class KVRange extends AbstractKVRangeMetadata implements IKVRange {
    @Getter
    private final ICPableKVSpace kvSpace;
    private final ConcurrentLinkedQueue<IKVCloseableReader> sharedDataReaders = new ConcurrentLinkedQueue<>();
    private final BehaviorSubject<KVRangeMeta> metaSubject;

    public KVRange(KVRangeId id, ICPableKVSpace kvSpace) {
        super(id, kvSpace);
        this.kvSpace = kvSpace;
        metaSubject = BehaviorSubject.createDefault(
            new IKVRange.KVRangeMeta(-1L,
                State.newBuilder().setType(State.StateType.NoUse).build(),
                NULL_BOUNDARY,
                ClusterConfig.getDefaultInstance()));
        kvSpace.metadata()
            .map(metadataMap -> {
                long version = version(metadataMap.get(METADATA_VER_BYTES));
                State state = state(metadataMap.get(METADATA_STATE_BYTES));
                Boundary boundary = boundary(metadataMap.get(METADATA_RANGE_BOUND_BYTES));
                ClusterConfig clusterConfig = clusterConfig(metadataMap.get(METADATA_CLUSTER_CONFIG_BYTES));
                return new IKVRange.KVRangeMeta(version, state, boundary, clusterConfig);
            })
            .subscribe(metaSubject);
    }

    public KVRange(KVRangeId id, ICPableKVSpace kvSpace, KVRangeSnapshot snapshot) {
        this(id, kvSpace);
        toReseter(snapshot).done();
    }

    @Override
    public final long version() {
        return metaSubject.getValue().ver();
    }

    @Override
    public final State state() {
        return metaSubject.getValue().state();
    }

    @Override
    public final Boundary boundary() {
        return metaSubject.getValue().boundary();
    }

    @Override
    public ClusterConfig clusterConfig() {
        return metaSubject.getValue().clusterConfig();
    }

    @Override
    public Observable<KVRangeMeta> metadata() {
        return metaSubject;
    }

    @Override
    public KVRangeSnapshot checkpoint() {
        String checkpointId = kvSpace.checkpoint();
        IKVRangeReader kvRangeCheckpoint = new KVRangeCheckpoint(id, kvSpace.openCheckpoint(checkpointId).get());
        KVRangeSnapshot.Builder builder = KVRangeSnapshot.newBuilder()
            .setVer(kvRangeCheckpoint.version())
            .setId(id)
            .setCheckpointId(checkpointId)
            .setLastAppliedIndex(kvRangeCheckpoint.lastAppliedIndex())
            .setState(kvRangeCheckpoint.state())
            .setBoundary(kvRangeCheckpoint.boundary())
            .setClusterConfig(kvRangeCheckpoint.clusterConfig());
        return builder.build();
    }

    @Override
    public boolean hasCheckpoint(KVRangeSnapshot checkpoint) {
        assert checkpoint.getId().equals(id);
        return checkpoint.hasCheckpointId() && kvSpace.openCheckpoint(checkpoint.getCheckpointId()).isPresent();
    }

    @Override
    public IKVRangeCheckpointReader open(KVRangeSnapshot checkpoint) {
        return new KVRangeCheckpoint(id, kvSpace.openCheckpoint(checkpoint.getCheckpointId()).get());
    }

    @SneakyThrows
    @Override
    public final IKVReader borrowDataReader() {
        IKVReader reader = sharedDataReaders.poll();
        if (reader == null) {
            return newDataReader();
        }
        return reader;
    }

    @Override
    public final void returnDataReader(IKVReader borrowed) {
        sharedDataReaders.add((IKVCloseableReader) borrowed);
    }

    @Override
    public IKVCloseableReader newDataReader() {
        return new KVReader(kvSpace, this);
    }

    @Override
    public IKVRangeWriter<?> toWriter() {
        return new KVRangeWriter(id, kvSpace.toWriter());
    }

    @Override
    public IKVRangeWriter<?> toWriter(IKVLoadRecorder recorder) {
        return new LoadRecordableKVRangeWriter(id, kvSpace.toWriter(), recorder);
    }

    @Override
    public IKVReseter toReseter(KVRangeSnapshot snapshot) {
        IKVRangeWriter<?> rangeWriter = toWriter();
        IKVWriter kvWriter = rangeWriter
            .resetVer(snapshot.getVer())
            .lastAppliedIndex(snapshot.getLastAppliedIndex())
            .state(snapshot.getState())
            .boundary(snapshot.getBoundary())
            .clusterConfig(snapshot.getClusterConfig())
            .kvWriter();
        kvWriter.clear(boundary());
        return new IKVReseter() {
            @Override
            public void put(ByteString key, ByteString value) {
                kvWriter.put(key, value);
            }

            @Override
            public IKVRange abort() {
                rangeWriter.abort();
                return KVRange.this;
            }

            @Override
            public IKVRange done() {
                rangeWriter.done();
                return KVRange.this;
            }
        };
    }

    @Override
    public void close() {
        IKVCloseableReader reader;
        while ((reader = sharedDataReaders.poll()) != null) {
            reader.close();
        }
        metaSubject.onComplete();
    }

    @Override
    public void destroy() {
        kvSpace.destroy();
    }
}
