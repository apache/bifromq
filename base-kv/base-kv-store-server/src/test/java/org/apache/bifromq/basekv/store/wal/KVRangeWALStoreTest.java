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

package org.apache.bifromq.basekv.store.wal;

import org.apache.bifromq.basekv.TestUtil;
import org.apache.bifromq.basekv.localengine.rocksdb.RocksDBWALableKVEngineConfigurator;
import org.apache.bifromq.basekv.raft.BasicStateStoreTest;
import org.apache.bifromq.basekv.raft.IRaftStateStore;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class KVRangeWALStoreTest extends BasicStateStoreTest {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR = "testDB_cp";
    private KVRangeWALStorageEngine stateStorageEngine;
    public Path dbRootDir;

    @BeforeMethod
    public void setup() throws IOException {
        RocksDBWALableKVEngineConfigurator walConfigurator;
        dbRootDir = Files.createTempDirectory("");
        walConfigurator = RocksDBWALableKVEngineConfigurator.builder()
            .dbRootDir(Paths.get(dbRootDir.toString(), DB_NAME).toString())
            .build();
        stateStorageEngine = new KVRangeWALStorageEngine("testcluster", null, walConfigurator);
        stateStorageEngine.start();
    }

    @AfterMethod
    public void teardown() {
        stateStorageEngine.stop();
        if (dbRootDir != null) {
            TestUtil.deleteDir(dbRootDir.toString());
            dbRootDir.toFile().delete();
        }
    }

    @Override
    protected String localId() {
        return stateStorageEngine.id();
    }

    @Override
    protected IRaftStateStore createStorage(String id, Snapshot snapshot) {
        return stateStorageEngine.create(KVRangeIdUtil.generate(), snapshot);
    }
}
