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
import static org.testng.Assert.fail;

import io.reactivex.rxjava3.core.Maybe;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.testng.annotations.Test;

public class KVRangeMetadataTest extends AbstractKVRangeTest {
    @Test
    public void initWithNoData() {
        KVRangeId id = KVRangeIdUtil.generate();
        ICPableKVSpace keyRange = kvEngine.createIfMissing(KVRangeIdUtil.toString(id));
        IKVRange accessor = new KVRange(id, keyRange);
        assertEquals(accessor.id(), id);
        assertEquals(accessor.currentVer(), -1);
        assertEquals(accessor.currentLastAppliedIndex(), -1);
        assertEquals(accessor.currentState().getType(), State.StateType.NoUse);
    }

    @Test
    public void initExistingRange() {
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
        IKVRange accessor = new KVRange(snapshot.getId(), keyRange);
        IKVRangeRestoreSession restoreSession = accessor.startRestore(snapshot, (c, b) -> {});
        restoreSession.done();

        assertEquals(accessor.currentVer(), snapshot.getVer());
        assertEquals(accessor.currentBoundary(), snapshot.getBoundary());
        assertEquals(accessor.currentLastAppliedIndex(), snapshot.getLastAppliedIndex());
        assertEquals(accessor.currentState(), snapshot.getState());
        assertEquals(accessor.currentClusterConfig(), snapshot.getClusterConfig());
    }

    @Test
    public void initWithNoDataAndDestroy() {
        try {
            KVRangeId rangeId = KVRangeIdUtil.generate();
            ICPableKVSpace kvSpace = kvEngine.createIfMissing(KVRangeIdUtil.toString(rangeId));
            IKVRange kvRange = new KVRange(rangeId, kvSpace);
            Maybe<State> stateMayBe = kvRange.state().firstElement();
            kvSpace.destroy();
            assertEquals(stateMayBe.timeout(5, TimeUnit.SECONDS).blockingGet().getType(), State.StateType.NoUse);
        } catch (Throwable e) {
            fail();
        }
    }

    @Test
    public void lastAppliedIndex() {
        KVRangeId id = KVRangeIdUtil.generate();
        long ver = 10;
        long lastAppliedIndex = 10;
        State state = State.newBuilder().setType(State.StateType.Normal).build();
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(id)
            .setVer(ver)
            .setLastAppliedIndex(lastAppliedIndex)
            .setState(state)
            .setBoundary(FULL_BOUNDARY)
            .build();
        ICPableKVSpace keyRange = kvEngine.createIfMissing(KVRangeIdUtil.toString(snapshot.getId()));
        IKVRange accessor = new KVRange(snapshot.getId(), keyRange);
        IKVRangeRestoreSession restoreSession = accessor.startRestore(snapshot, (c, b) -> {});
        restoreSession.done();

        lastAppliedIndex = 11;
        accessor.toWriter().lastAppliedIndex(lastAppliedIndex).done();
        assertEquals(accessor.currentLastAppliedIndex(), lastAppliedIndex);
    }
}

