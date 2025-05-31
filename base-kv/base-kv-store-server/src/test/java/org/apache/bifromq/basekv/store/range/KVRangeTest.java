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

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVRangeReader;
import org.apache.bifromq.basekv.store.api.IKVReader;
import org.apache.bifromq.basekv.store.api.IKVWriter;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

public class KVRangeTest extends AbstractKVRangeTest {
    @Test
    public void init() {
        KVRangeId id = KVRangeIdUtil.generate();
        ICPableKVSpace keyRange = kvEngine.createIfMissing(KVRangeIdUtil.toString(id));
        // no snapshot specified
        IKVRange accessor = new KVRange(id, keyRange);
        assertEquals(accessor.id(), id);
        assertEquals(accessor.version(), -1);
        assertEquals(accessor.lastAppliedIndex(), -1);
        assertEquals(accessor.state().getType(), State.StateType.NoUse);
        assertEquals(accessor.clusterConfig(), ClusterConfig.getDefaultInstance());
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

        assertEquals(accessor.newDataReader().get(key).get(), value);
        assertEquals(accessor.version(), snapshot.getVer());
        assertEquals(accessor.boundary(), snapshot.getBoundary());
        assertEquals(accessor.lastAppliedIndex(), snapshot.getLastAppliedIndex());
        assertEquals(accessor.state(), snapshot.getState());
        assertEquals(accessor.clusterConfig(), snapshot.getClusterConfig());
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
        IKVRange.KVRangeMeta metadata = accessor.metadata().blockingFirst();
        assertEquals(metadata.ver(), snapshot.getVer());
        assertEquals(metadata.boundary(), snapshot.getBoundary());
        assertEquals(metadata.state(), snapshot.getState());
        assertEquals(metadata.clusterConfig(), snapshot.getClusterConfig());
        accessor.toWriter().resetVer(2).done();
        metadata = accessor.metadata().blockingFirst();
        assertEquals(metadata.ver(), 2);
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

        IKVRangeReader rangeCP = accessor.open(snap);
        assertEquals(snap.getId(), rangeCP.id());
        assertEquals(snap.getVer(), rangeCP.version());
        assertEquals(snap.getLastAppliedIndex(), rangeCP.lastAppliedIndex());
        assertEquals(snap.getState(), rangeCP.state());
        assertEquals(snap.getBoundary(), rangeCP.boundary());
        assertEquals(snap.getClusterConfig(), rangeCP.clusterConfig());
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

        IKVIterator itr = accessor.open(snapshot).newDataReader().iterator();
        itr.seekToFirst();
        assertFalse(itr.isValid());

        snapshot = accessor.checkpoint();
        itr = accessor.open(snapshot).newDataReader().iterator();
        itr.seekToFirst();
        assertTrue(itr.isValid());
        assertEquals(itr.key(), key);
        assertEquals(itr.value(), val);
        itr.next();
        assertFalse(itr.isValid());
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
        IKVReader rangeReader = accessor.newDataReader();
        IKVIterator kvItr = rangeReader.iterator();
        ByteString key = ByteString.copyFromUtf8("aKey");
        ByteString val = ByteString.copyFromUtf8("Value");
        rangeWriter.kvWriter().put(key, val);
        rangeWriter.done();
        assertTrue(rangeReader.exist(key));
        kvItr.seek(key);
        assertFalse(kvItr.isValid());
        rangeReader.refresh();
        kvItr.seek(key);
        assertTrue(kvItr.isValid());
        assertEquals(kvItr.key(), key);
        assertEquals(kvItr.value(), val);

        // make a range change
        Boundary newBoundary = boundary.toBuilder().setStartKey(ByteString.copyFromUtf8("b")).build();
        accessor.toWriter().resetVer(1).boundary(newBoundary).done();
        assertEquals(accessor.version(), 1);
        assertEquals(accessor.boundary(), newBoundary);
        assertEquals(rangeReader.boundary(), newBoundary);
    }

    @Test
    public void borrowReader() {
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
        IKVReader rangeReader1 = accessor.borrowDataReader();
        IKVReader rangeReader2 = accessor.borrowDataReader();
        assertNotSame(rangeReader1, rangeReader2);
        ByteString key = ByteString.copyFromUtf8("aKey");
        ByteString val = ByteString.copyFromUtf8("Value");

        assertFalse(rangeReader1.exist(key));
        assertFalse(rangeReader2.exist(key));

        rangeWriter.kvWriter().put(key, val);
        rangeWriter.done();

        assertTrue(rangeReader1.exist(key));
        assertTrue(rangeReader2.exist(key));

        accessor.returnDataReader(rangeReader2);
        assertSame(rangeReader2, accessor.borrowDataReader());
        assertTrue(rangeReader2.exist(key));
    }

    @Test
    public void borrowReaderConcurrently() {
        Boundary range = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("c"))
            .build();
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setBoundary(range)
            .build();
        ICPableKVSpace keyRange = kvEngine.createIfMissing(KVRangeIdUtil.toString(snapshot.getId()));
        IKVRange accessor = new KVRange(snapshot.getId(), keyRange, snapshot);
        IKVReader rangeReader1 = accessor.borrowDataReader();
        IKVReader rangeReader2 = accessor.borrowDataReader();
        accessor.returnDataReader(rangeReader1);
        accessor.returnDataReader(rangeReader2);
        AtomicReference<IKVReader> t1Borrowed = new AtomicReference<>();
        AtomicReference<IKVReader> t2Borrowed = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean(false);
        Thread t1 = new Thread(() -> {
            if (stop.get()) {
                return;
            }
            t1Borrowed.set(accessor.borrowDataReader());
            IKVReader t1Reader = t1Borrowed.getAndSet(null);
            accessor.returnDataReader(t1Reader);
        });
        Thread t2 = new Thread(() -> {
            if (stop.get()) {
                return;
            }
            t2Borrowed.set(accessor.borrowDataReader());
            IKVReader t2Reader = t2Borrowed.getAndSet(null);
            accessor.returnDataReader(t2Reader);
        });
        t1.start();
        t2.start();
        AtomicBoolean success = new AtomicBoolean(true);
        try {
            await().atMost(Duration.ofSeconds(5)).until(() -> {
                IKVReader t1Reader = t1Borrowed.get();
                IKVReader t2Reader = t2Borrowed.get();
                return t2Reader != null && t1Reader == t2Reader; // this should not be true
            });
            success.set(false);
        } catch (Throwable e) {

        } finally {
            stop.set(true);
        }
        assertTrue(success.get());
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

        accessor.toReseter(snapshot).done();
        IKVReader rangeReader = accessor.newDataReader();
        assertFalse(rangeReader.exist(key));
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
        IKVReader rangeReader = accessor.newDataReader();
        assertEquals(accessor.version(), 0);
        assertFalse(accessor.newDataReader().exist(key));
    }
}
