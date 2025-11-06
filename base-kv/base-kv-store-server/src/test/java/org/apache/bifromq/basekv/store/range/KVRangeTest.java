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

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVRangeReader;
import org.apache.bifromq.basekv.store.api.IKVRangeRefreshableReader;
import org.apache.bifromq.basekv.store.api.IKVWriter;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.testng.annotations.Test;

public class KVRangeTest extends AbstractKVRangeTest {
    @Test
    public void init() {
        KVRangeId id = KVRangeIdUtil.generate();
        ICPableKVSpace keyRange = kvEngine.createIfMissing(KVRangeIdUtil.toString(id));
        // no snapshot specified
        IKVRange accessor = new KVRange(id, keyRange);
        assertEquals(accessor.id(), id);
        assertEquals(accessor.currentVer(), -1);
        assertEquals(accessor.currentLastAppliedIndex(), -1);
        assertEquals(accessor.currentState().getType(), State.StateType.NoUse);
        assertEquals(accessor.currentClusterConfig(), ClusterConfig.getDefaultInstance());
    }

    @Test
    public void initWithSnapshot() {
        ClusterConfig initConfig = ClusterConfig.newBuilder().addVoters("storeA").build();
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setBoundary(FULL_BOUNDARY)
            .setClusterConfig(initConfig)
            .build();
        ICPableKVSpace keyRange = kvEngine.createIfMissing(KVRangeIdUtil.toString(snapshot.getId()));
        IKVRange accessor = new KVRange(snapshot.getId(), keyRange, snapshot);
        IKVRangeWriter<?> rangeWriter = accessor.toWriter();
        IKVWriter writer = rangeWriter.kvWriter();
        ByteString key = ByteString.copyFromUtf8("Hello");
        ByteString value = ByteString.copyFromUtf8("World");
        writer.put(key, value);
        rangeWriter.done();

        try (IKVRangeRefreshableReader reader = accessor.newReader()) {
            assertEquals(reader.get(key).get(), value);
        }
        assertEquals(accessor.currentVer(), snapshot.getVer());
        assertEquals(accessor.currentBoundary(), snapshot.getBoundary());
        assertEquals(accessor.currentLastAppliedIndex(), snapshot.getLastAppliedIndex());
        assertEquals(accessor.currentState(), snapshot.getState());
        assertEquals(accessor.currentClusterConfig(), snapshot.getClusterConfig());
    }

    @Test
    public void metadata() {
        ClusterConfig initConfig = ClusterConfig.newBuilder().addVoters("storeA").build();
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setBoundary(FULL_BOUNDARY)
            .setClusterConfig(initConfig)
            .build();
        ICPableKVSpace keyRange = kvEngine.createIfMissing(KVRangeIdUtil.toString(snapshot.getId()));
        IKVRange accessor = new KVRange(snapshot.getId(), keyRange, snapshot);
        assertEquals(accessor.currentVer(), snapshot.getVer());
        assertEquals(accessor.currentBoundary(), snapshot.getBoundary());
        assertEquals(accessor.currentState(), snapshot.getState());
        assertEquals(accessor.currentClusterConfig(), snapshot.getClusterConfig());
        accessor.toWriter().ver(2).done();
        assertEquals(accessor.currentVer(), 2);
    }

    @Test
    public void checkpoint() {
        ClusterConfig initConfig = ClusterConfig.newBuilder().addVoters("storeA").build();
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setBoundary(FULL_BOUNDARY)
            .setClusterConfig(initConfig)
            .build();
        ICPableKVSpace keyRange = kvEngine.createIfMissing(KVRangeIdUtil.toString(snapshot.getId()));
        IKVRange accessor = new KVRange(snapshot.getId(), keyRange, snapshot);
        assertFalse(accessor.hasCheckpoint(snapshot));

        KVRangeSnapshot snap = accessor.checkpoint();
        assertTrue(accessor.hasCheckpoint(snap));
        assertTrue(snap.hasCheckpointId());
        assertEquals(snap.getId(), snapshot.getId());
        assertEquals(snap.getVer(), snapshot.getVer());
        assertEquals(snap.getLastAppliedIndex(), snapshot.getLastAppliedIndex());
        assertEquals(snap.getState(), snapshot.getState());
        assertEquals(snap.getBoundary(), snapshot.getBoundary());
        assertEquals(snap.getClusterConfig(), snapshot.getClusterConfig());

        try (IKVRangeReader rangeCP = accessor.open(snap)) {
            assertEquals(snap.getVer(), rangeCP.version());
            assertEquals(snap.getLastAppliedIndex(), rangeCP.lastAppliedIndex());
            assertEquals(snap.getState(), rangeCP.state());
            assertEquals(snap.getBoundary(), rangeCP.boundary());
            assertEquals(snap.getClusterConfig(), rangeCP.clusterConfig());
        }
    }

    @Test
    public void openCheckpoint() {
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setBoundary(FULL_BOUNDARY)
            .build();
        ICPableKVSpace keyRange = kvEngine.createIfMissing(KVRangeIdUtil.toString(snapshot.getId()));
        IKVRange accessor = new KVRange(snapshot.getId(), keyRange, snapshot);
        snapshot = accessor.checkpoint();

        ByteString key = ByteString.copyFromUtf8("Key");
        ByteString val = ByteString.copyFromUtf8("Value");
        IKVRangeWriter<?> writer = accessor.toWriter();
        writer.kvWriter().put(key, val);
        writer.done();

        try (IKVRangeReader reader = accessor.open(snapshot); IKVIterator itr = reader.iterator()) {
            itr.seekToFirst();
            assertFalse(itr.isValid());
        }

        snapshot = accessor.checkpoint();
        try (IKVRangeReader reader = accessor.open(snapshot); IKVIterator itr = reader.iterator()) {
            itr.seekToFirst();
            assertTrue(itr.isValid());
            assertEquals(itr.key(), key);
            assertEquals(itr.value(), val);
            itr.next();
            assertFalse(itr.isValid());
        }
    }

    @Test
    public void readWrite() {
        Boundary boundary = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("c"))
            .build();
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setBoundary(boundary)
            .build();
        ICPableKVSpace keyRange = kvEngine.createIfMissing(KVRangeIdUtil.toString(snapshot.getId()));
        IKVRange accessor = new KVRange(snapshot.getId(), keyRange, snapshot);
        IKVRangeWriter<?> rangeWriter = accessor.toWriter();
        ByteString key = ByteString.copyFromUtf8("aKey");
        ByteString val = ByteString.copyFromUtf8("Value");
        rangeWriter.kvWriter().put(key, val);
        rangeWriter.done();
        try (IKVRangeRefreshableReader rangeReader = accessor.newReader(); IKVIterator kvItr = rangeReader.iterator()) {
            assertTrue(rangeReader.exist(key));
            kvItr.seek(key);
            assertTrue(kvItr.isValid());
            assertEquals(kvItr.key(), key);
            assertEquals(kvItr.value(), val);

            // make a range change
            Boundary newBoundary = boundary.toBuilder().setStartKey(ByteString.copyFromUtf8("b")).build();
            accessor.toWriter().ver(1).boundary(newBoundary).done();
            assertEquals(accessor.currentVer(), 1);
            assertEquals(accessor.currentBoundary(), newBoundary);
            assertEquals(rangeReader.boundary(), newBoundary);
        }
    }

    @Test
    public void resetFromCheckpoint() {
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setBoundary(FULL_BOUNDARY)
            .build();
        ICPableKVSpace keyRange = kvEngine.createIfMissing(KVRangeIdUtil.toString(snapshot.getId()));
        IKVRange accessor = new KVRange(snapshot.getId(), keyRange, snapshot);

        snapshot = accessor.checkpoint();
        IKVRangeWriter<?> rangeWriter = accessor.toWriter();
        ByteString key = ByteString.copyFromUtf8("aKey");
        ByteString val = ByteString.copyFromUtf8("Value");
        rangeWriter.kvWriter().put(key, val);
        rangeWriter.done();

        accessor.startRestore(snapshot, (c, b) -> {}).done();
        try (IKVRangeRefreshableReader rangeReader = accessor.newReader()) {
            assertFalse(rangeReader.exist(key));
        }
    }

    @Test
    public void destroy() {
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setBoundary(FULL_BOUNDARY)
            .build();
        ICPableKVSpace keyRange = kvEngine.createIfMissing(KVRangeIdUtil.toString(snapshot.getId()));
        IKVRange accessor = new KVRange(snapshot.getId(), keyRange, snapshot);

        IKVRangeWriter<?> rangeWriter = accessor.toWriter();
        ByteString key = ByteString.copyFromUtf8("aKey");
        ByteString val = ByteString.copyFromUtf8("Value");
        rangeWriter.kvWriter().put(key, val);
        rangeWriter.done();

        accessor.destroy();

        keyRange = kvEngine.createIfMissing(KVRangeIdUtil.toString(snapshot.getId()));
        accessor = new KVRange(snapshot.getId(), keyRange, snapshot);
        try (IKVRangeRefreshableReader rangeReader = accessor.newReader()) {
            assertEquals(accessor.currentVer(), 0);
            assertFalse(rangeReader.exist(key));
        }
    }
}
