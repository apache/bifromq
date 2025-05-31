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

package org.apache.bifromq.basekv.localengine.rocksdb;

import static org.testng.Assert.assertEquals;

import org.apache.bifromq.basekv.localengine.ICPableKVEngineConfigurator;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVEngine;
import org.apache.bifromq.basekv.localengine.KVEngineFactory;
import org.apache.bifromq.basekv.localengine.MockableTest;
import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.UUID;
import lombok.SneakyThrows;
import org.testng.annotations.Test;

public class OverrideIdentityTest extends MockableTest {
    private final String DB_NAME = "testDB";
    private final String DB_CHECKPOINT_DIR = "testDB_cp";
    private IKVEngine<? extends ICPableKVSpace> engine;
    public Path dbRootDir;

    @SneakyThrows
    protected void doSetup(Method method) {
        dbRootDir = Files.createTempDirectory("");
    }

    @SneakyThrows
    protected void doTeardown(Method method) {
        Files.walk(dbRootDir)
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);
    }

    @Test
    public void testOverrideIdentity() {
        String overrideIdentity = UUID.randomUUID().toString();
        ICPableKVEngineConfigurator configurator = RocksDBCPableKVEngineConfigurator.builder()
            .dbRootDir(Paths.get(dbRootDir.toString(), DB_NAME).toString())
            .dbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR).toString())
            .build();
        engine = KVEngineFactory.createCPable(overrideIdentity, configurator);
        engine.start();
        assertEquals(engine.id(), overrideIdentity);
        engine.stop();
        // restart without overrideIdentity specified
        configurator = RocksDBCPableKVEngineConfigurator.builder()
            .dbRootDir(Paths.get(dbRootDir.toString(), DB_NAME).toString())
            .dbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR).toString())
            .build();

        engine = KVEngineFactory.createCPable(null, configurator);
        engine.start();

        assertEquals(engine.id(), overrideIdentity);
        engine.stop();
        // restart with different overrideIdentity specified
        String another = UUID.randomUUID().toString();
        configurator = RocksDBCPableKVEngineConfigurator.builder()
            .dbRootDir(Paths.get(dbRootDir.toString(), DB_NAME).toString())
            .dbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR).toString())
            .build();

        engine = KVEngineFactory.createCPable(another, configurator);
        engine.start();

        assertEquals(engine.id(), overrideIdentity);
        engine.stop();
    }

    @Test
    public void testCanOnlyOverrideWhenInit() {
        ICPableKVEngineConfigurator configurator = RocksDBCPableKVEngineConfigurator.builder()
            .dbRootDir(Paths.get(dbRootDir.toString(), DB_NAME).toString())
            .dbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR).toString())
            .build();

        engine = KVEngineFactory.createCPable(null, configurator);
        engine.start();
        String identity = engine.id();
        engine.stop();
        // restart with overrideIdentity specified
        String overrideIdentity = UUID.randomUUID().toString();
        configurator = RocksDBCPableKVEngineConfigurator.builder()
            .dbRootDir(Paths.get(dbRootDir.toString(), DB_NAME).toString())
            .dbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR).toString())
            .build();

        engine = KVEngineFactory.createCPable(overrideIdentity, configurator);
        engine.start();

        assertEquals(engine.id(), identity);
        engine.stop();
    }
}
