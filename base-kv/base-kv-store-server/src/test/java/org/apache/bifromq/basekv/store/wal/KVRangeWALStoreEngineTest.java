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

package org.apache.bifromq.basekv.store.wal;

import static org.apache.bifromq.basekv.localengine.StructUtil.toValue;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_ROOT_DIR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.MockableTest;
import org.apache.bifromq.basekv.TestUtil;
import org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.raft.IRaftStateStore;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeWALStoreEngineTest extends MockableTest {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR = "testDB_cp";
    public Path dbRootDir;
    private String dbPath;
    private Struct engineConf;

    @SneakyThrows
    @Override
    protected void doSetup(Method method) {
        dbRootDir = Files.createTempDirectory("");
        dbPath = Paths.get(dbRootDir.toString(), DB_NAME).toString();
        engineConf = RocksDBDefaultConfigs.WAL.toBuilder().putFields(DB_ROOT_DIR, toValue(dbPath)).build();
    }

    protected void doTearDown(Method method) {
        if (dbRootDir != null) {
            TestUtil.deleteDir(dbRootDir.toString());
            dbRootDir.toFile().delete();
        }
    }

    @Test
    public void startAndStop() {
        try {
            KVRangeWALStorageEngine stateStorageEngine =
                new KVRangeWALStorageEngine("testcluster", null, "rocksdb", engineConf);
            stateStorageEngine.start();
            if (engineConf != null) {
                assertTrue((new File(dbPath)).isDirectory());
            }
            assertTrue(stateStorageEngine.allKVRangeIds().isEmpty());
            stateStorageEngine.stop();
        } catch (Exception e) {
            log.error("Failed to init", e);
            fail();
        }
    }

    @Test
    public void newRaftStateStorage() {
        try {
            KVRangeId testId = KVRangeIdUtil.generate();
            KVRangeWALStorageEngine stateStorageEngine =
                new KVRangeWALStorageEngine("testcluster", null, "rocksdb", engineConf);
            stateStorageEngine.start();
            Snapshot snapshot = Snapshot.newBuilder()
                .setIndex(0)
                .setTerm(0)
                .setClusterConfig(ClusterConfig.newBuilder()
                    .addVoters(stateStorageEngine.id())
                    .build())
                .build();
            IRaftStateStore stateStorage = stateStorageEngine.create(testId, snapshot);
            assertEquals(stateStorage.local(), stateStorageEngine.id());
            assertEquals(stateStorage.lastIndex(), 0);
            assertEquals(stateStorage.firstIndex(), 1);
            assertFalse(stateStorage.currentVoting().isPresent());
            assertEquals(stateStorage.latestSnapshot(), snapshot);
            assertEquals(stateStorage.latestClusterConfig(), snapshot.getClusterConfig());

            assertTrue(stateStorageEngine.has(testId));
            assertEquals(stateStorageEngine.get(testId), stateStorage);
            assertEquals(stateStorageEngine.allKVRangeIds().size(), 1);
            assertTrue(stateStorageEngine.allKVRangeIds().contains(testId));
            stateStorageEngine.stop();
        } catch (Exception e) {
            log.error("Failed to init", e);
            fail();
        }
    }

    @Test
    public void loadExistingRaftStateStorage() {
        KVRangeId testId1 = KVRangeIdUtil.generate();
        KVRangeId testId2 = KVRangeIdUtil.next(testId1);
        KVRangeWALStorageEngine stateStorageEngine =
            new KVRangeWALStorageEngine("testcluster", null, "rocksdb", engineConf);
        stateStorageEngine.start();
        Snapshot snapshot = Snapshot.newBuilder()
            .setIndex(0)
            .setTerm(0)
            .setClusterConfig(ClusterConfig.newBuilder()
                .addVoters(stateStorageEngine.id())
                .build())
            .build();
        IRaftStateStore walStore1 = stateStorageEngine.create(testId1, snapshot);
        IRaftStateStore walStore2 = stateStorageEngine.create(testId2, snapshot);
        assertEquals(stateStorageEngine.allKVRangeIds().size(), 2);
        walStore1.append(Collections.singletonList(LogEntry.newBuilder()
            .setData(ByteString.copyFromUtf8("Hello"))
            .setIndex(1)
            .build()), true);
        assertEquals(walStore1.lastIndex(), 1);
        stateStorageEngine.stop();

        stateStorageEngine = new KVRangeWALStorageEngine("testcluster", null, "rocksdb", engineConf);
        stateStorageEngine.start();
        assertEquals(stateStorageEngine.allKVRangeIds().size(), 2);
        IRaftStateStore stateStorage = stateStorageEngine.get(testId1);
        assertEquals(stateStorage.local(), stateStorageEngine.id());
        assertEquals(stateStorage.lastIndex(), 1);
        assertEquals(stateStorage.firstIndex(), 1);
        assertFalse(stateStorage.currentVoting().isPresent());
        assertEquals(stateStorage.latestSnapshot(), snapshot);
        assertEquals(stateStorage.latestClusterConfig(), snapshot.getClusterConfig());
    }

    @Test
    public void destroyRaftStateStorage() {
        KVRangeId testId1 = KVRangeIdUtil.generate();
        KVRangeId testId2 = KVRangeIdUtil.next(testId1);
        KVRangeWALStorageEngine stateStorageEngine =
            new KVRangeWALStorageEngine("testcluster", null, "rocksdb", engineConf);
        stateStorageEngine.start();
        Snapshot snapshot = Snapshot.newBuilder()
            .setIndex(0)
            .setTerm(0)
            .setClusterConfig(ClusterConfig.newBuilder()
                .addVoters(stateStorageEngine.id())
                .build())
            .build();
        IKVRangeWALStore walStore1 = stateStorageEngine.create(testId1, snapshot);
        IKVRangeWALStore walStore2 = stateStorageEngine.create(testId2, snapshot);
        walStore1.destroy();
        assertEquals(stateStorageEngine.allKVRangeIds().size(), 1);
        assertFalse(stateStorageEngine.has(testId1));
        assertTrue(stateStorageEngine.has(testId2));
        stateStorageEngine.stop();

        stateStorageEngine = new KVRangeWALStorageEngine("testcluster", null, "rocksdb", engineConf);
        stateStorageEngine.start();
        assertEquals(stateStorageEngine.allKVRangeIds().size(), 1);
        assertTrue(stateStorageEngine.has(testId2));
    }

    @Test
    public void destroyAndCreate() {
        KVRangeId testId1 = KVRangeIdUtil.generate();
        KVRangeWALStorageEngine stateStorageEngine =
            new KVRangeWALStorageEngine("testcluster", null, "rocksdb", engineConf);
        stateStorageEngine.start();
        Snapshot snapshot = Snapshot.newBuilder()
            .setIndex(0)
            .setTerm(0)
            .setClusterConfig(ClusterConfig.newBuilder()
                .addVoters(stateStorageEngine.id())
                .build())
            .build();
        IKVRangeWALStore walStore1 = stateStorageEngine.create(testId1, snapshot);
        walStore1.destroy();
        assertEquals(stateStorageEngine.allKVRangeIds().size(), 0);
        assertFalse(stateStorageEngine.has(testId1));

        walStore1 = stateStorageEngine.create(testId1, snapshot);
        assertEquals(stateStorageEngine.allKVRangeIds().size(), 1);
        assertTrue(stateStorageEngine.has(testId1));
    }
}
