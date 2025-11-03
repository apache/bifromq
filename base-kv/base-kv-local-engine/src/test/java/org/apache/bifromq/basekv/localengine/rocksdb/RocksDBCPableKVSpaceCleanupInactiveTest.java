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

import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBCPableKVSpace.ACTIVE_GEN_POINTER;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBKVEngine.IDENTITY_FILE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RocksDBCPableKVSpaceCleanupInactiveTest {
    private Path tmpRoot;
    private RocksDBCPableKVEngine engine;

    @BeforeMethod
    public void setup() throws Exception {
        tmpRoot = Files.createTempDirectory("kvspace_cleanup_");
    }

    @AfterMethod
    public void tearDown() throws Exception {
        try {
            if (engine != null) {
                engine.stop();
            }
        } catch (Throwable ignore) {
        }
        if (tmpRoot != null) {
            Files.walk(tmpRoot)
                .sorted((a, b) -> b.getNameCount() - a.getNameCount())
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (Throwable ignore) {
                    }
                });
        }
    }

    @Test
    public void testCleanInactiveOnStartup() throws Exception {
        String spaceId = "space_cleanup";
        Path dbRoot = tmpRoot.resolve("data");
        Path cpRoot = tmpRoot.resolve("cp");
        Path spaceRoot = dbRoot.resolve(spaceId);
        Files.createDirectories(spaceRoot);
        Files.createDirectories(cpRoot);
        // Ensure engine identity exists when root is non-empty
        Files.writeString(dbRoot.resolve(IDENTITY_FILE), "test-id");

        String active = UUID.randomUUID().toString();
        String inactive = UUID.randomUUID().toString();
        Files.createDirectories(spaceRoot.resolve(active));
        Files.createDirectories(spaceRoot.resolve(inactive));
        Files.writeString(spaceRoot.resolve(ACTIVE_GEN_POINTER), active);
        // An unrelated file under root should be removed as well
        Files.writeString(spaceRoot.resolve("leftover.txt"), "x");

        Struct conf = RocksDBDefaultConfigs.CP.toBuilder()
            .putFields(RocksDBDefaultConfigs.DB_ROOT_DIR, Value.newBuilder().setStringValue(dbRoot.toString()).build())
            .putFields(RocksDBDefaultConfigs.DB_CHECKPOINT_ROOT_DIR, Value.newBuilder().setStringValue(cpRoot.toString()).build())
            .build();
        engine = new RocksDBCPableKVEngine(null, conf);
        engine.start("tag", "value");

        // After open, only active gen and pointer should remain
        long dirs = Files.list(spaceRoot).filter(Files::isDirectory).count();
        long files = Files.list(spaceRoot).filter(Files::isRegularFile).count();
        assertEquals(dirs, 1);
        assertEquals(files, 1);
        assertTrue(Files.exists(spaceRoot.resolve(active)));
        assertTrue(Files.exists(spaceRoot.resolve(ACTIVE_GEN_POINTER)));
    }
}
