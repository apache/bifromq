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

package org.apache.bifromq.basekv.localengine.rocksdb;

import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_CHECKPOINT_ROOT_DIR;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_ROOT_DIR;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.reactivex.rxjava3.disposables.Disposable;
import java.io.File;
import java.nio.file.Paths;
import lombok.SneakyThrows;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVEngine;
import org.apache.bifromq.basekv.localengine.IKVSpace;
import org.testng.annotations.Test;

public class RocksDBCPableKVEngineTest extends AbstractRocksDBCPableEngineTest {
    private final String DB_NAME = "testDB";
    private final String DB_CHECKPOINT_DIR = "testDB_cp";
    private Struct conf;

    @SneakyThrows
    @Override
    protected void beforeStart() {
        super.beforeStart();
        conf = RocksDBDefaultConfigs.CP.toBuilder()
            .putFields(DB_ROOT_DIR, Value.newBuilder().setStringValue(Paths.get(dbRootDir.toString(), DB_NAME).toString()).build())
            .putFields(DB_CHECKPOINT_ROOT_DIR, Value.newBuilder().setStringValue(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR).toString()).build())
            .build();
    }

    @SneakyThrows
    @Override
    protected IKVEngine<? extends ICPableKVSpace> newEngine() {
        return new RocksDBCPableKVEngine(null, conf);
    }

    @Test
    public void removeCheckpointFileWhenDestroy() {
        String rangeId = "test_range1";
        File cpDir = Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR, rangeId).toFile();
        IKVSpace range = engine.createIfMissing(rangeId);
        assertTrue(engine.spaces().containsKey(rangeId));
        assertTrue(cpDir.exists());
        Disposable disposable = range.metadata().subscribe();

        range.destroy();
        assertFalse(cpDir.exists());
        assertTrue(disposable.isDisposed());
        assertTrue(engine.spaces().isEmpty());
        assertFalse(engine.spaces().containsKey(rangeId));
    }
}
