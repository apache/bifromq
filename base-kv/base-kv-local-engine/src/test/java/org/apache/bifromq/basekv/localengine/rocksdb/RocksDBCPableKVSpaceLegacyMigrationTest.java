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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceReader;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RocksDBCPableKVSpaceLegacyMigrationTest {
    private Path tmpRoot;
    private RocksDBCPableKVEngine engine;

    @BeforeMethod
    public void setup() throws Exception {
        // Prepare a temp root for db and checkpoint
        tmpRoot = Files.createTempDirectory("kvspace_legacy_migration_");
    }

    @AfterMethod
    public void tearDown() throws Exception {
        try {
            if (engine != null) {
                engine.stop();
            }
        } catch (Throwable ignore) {
        }
        // Cleanup temp dir recursively
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
    public void testLegacyLayoutMigratedInConstructor() throws Exception {
        String spaceId = "space1";
        Path dbRoot = tmpRoot.resolve("data");
        Path cpRoot = tmpRoot.resolve("cp");
        Files.createDirectories(dbRoot);
        Files.createDirectories(cpRoot);
        // Ensure engine identity exists when root is non-empty
        Files.writeString(dbRoot.resolve(IDENTITY_FILE), "test-id");

        // Simulate legacy layout: create a valid RocksDB directly under <dbRoot>/<spaceId>
        Path legacyRoot = dbRoot.resolve(spaceId);
        Files.createDirectories(legacyRoot);
        try (Options options = new Options().setCreateIfMissing(true)) {
            try (RocksDB db = RocksDB.open(options, legacyRoot.toString())) {
                // put a tiny entry to ensure DB files created
                db.put("k".getBytes(), "v".getBytes());
            }
        }

        // Build engine and start: constructor of space should migrate legacy layout immediately
        Struct conf = RocksDBDefaultConfigs.CP.toBuilder()
            .putFields(RocksDBDefaultConfigs.DB_ROOT_DIR, Value.newBuilder().setStringValue(dbRoot.toString()).build())
            .putFields(RocksDBDefaultConfigs.DB_CHECKPOINT_ROOT_DIR, Value.newBuilder().setStringValue(cpRoot.toString()).build())
            .build();
        engine = new RocksDBCPableKVEngine(null, conf);
        engine.start("tag", "value");

        Map<String, ? extends ICPableKVSpace> spaces = engine.spaces();
        ICPableKVSpace space = spaces.get(spaceId);
        assertNotNull(space);

        // Verify pointer file created and points to a uuid directory
        Path pointer = legacyRoot.resolve(ACTIVE_GEN_POINTER);
        assertTrue(Files.exists(pointer));
        String uuid = Files.readString(pointer).trim();
        assertFalse(uuid.isEmpty());
        Path currentGenDir = legacyRoot.resolve(uuid);
        assertTrue(Files.isDirectory(currentGenDir));

        // Write and read to ensure DB works after migration
        var writer = space.toWriter();
        writer.put(ByteString.copyFromUtf8("a"), ByteString.copyFromUtf8("b"));
        writer.done();
        try (IKVSpaceReader reader = space.reader()) {
            var val = reader.get(ByteString.copyFromUtf8("a"));
            assertTrue(val.isPresent());
            assertEquals(val.get().toStringUtf8(), "b");
        }
    }
}
