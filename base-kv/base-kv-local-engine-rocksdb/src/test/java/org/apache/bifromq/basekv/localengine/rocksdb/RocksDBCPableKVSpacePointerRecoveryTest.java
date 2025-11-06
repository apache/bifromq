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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceReader;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RocksDBCPableKVSpacePointerRecoveryTest {
    private Path tmpRoot;
    private RocksDBCPableKVEngine engine;

    @BeforeMethod
    public void setup() throws Exception {
        tmpRoot = Files.createTempDirectory("kvspace_ptr_recover_");
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
    public void testPointerPointsToMissingDir() throws Exception {
        String spaceId = "space_ptr_missing";
        Path dbRoot = tmpRoot.resolve("data");
        Path cpRoot = tmpRoot.resolve("cp");
        Files.createDirectories(dbRoot.resolve(spaceId));
        Files.createDirectories(cpRoot);
        // Ensure engine identity exists when root is non-empty
        Files.writeString(dbRoot.resolve(IDENTITY_FILE), "test-id");

        // Write pointer to a non-existing uuid
        String missingUUID = UUID.randomUUID().toString();
        Files.writeString(dbRoot.resolve(spaceId).resolve(ACTIVE_GEN_POINTER), missingUUID);

        Struct conf = RocksDBDefaultConfigs.CP.toBuilder()
            .putFields(RocksDBDefaultConfigs.DB_ROOT_DIR, Value.newBuilder().setStringValue(dbRoot.toString()).build())
            .putFields(RocksDBDefaultConfigs.DB_CHECKPOINT_ROOT_DIR, Value.newBuilder().setStringValue(cpRoot.toString()).build())
            .build();
        engine = new RocksDBCPableKVEngine(null, conf);
        engine.start("tag", "value");

        ICPableKVSpace space = engine.spaces().get(spaceId);
        assertNotNull(space);

        String current = Files.readString(dbRoot.resolve(spaceId).resolve(ACTIVE_GEN_POINTER)).trim();
        assertFalse(current.isEmpty());
        assertNotEquals(missingUUID, current);
        assertTrue(Files.isDirectory(dbRoot.resolve(spaceId).resolve(current)));

        // simple rw
        space.toWriter().put(ByteString.copyFromUtf8("k"), ByteString.copyFromUtf8("v")).done();
        try (IKVSpaceReader reader = space.reader()) {
            assertEquals(reader.get(ByteString.copyFromUtf8("k")).get().toStringUtf8(), "v");
        }
    }

    @Test
    public void testPointerValidRemains() throws Exception {
        String spaceId = "space_ptr_valid";
        Path dbRoot = tmpRoot.resolve("data");
        Path cpRoot = tmpRoot.resolve("cp");
        Files.createDirectories(dbRoot);
        Files.createDirectories(cpRoot);
        Path spaceRoot = dbRoot.resolve(spaceId);
        Files.createDirectories(spaceRoot);
        // Ensure engine identity exists when root is non-empty
        Files.writeString(dbRoot.resolve(IDENTITY_FILE), "test-id");
        String uuid = UUID.randomUUID().toString();
        Files.createDirectories(spaceRoot.resolve(uuid));
        Files.writeString(spaceRoot.resolve(ACTIVE_GEN_POINTER), uuid);

        Struct conf = RocksDBDefaultConfigs.CP.toBuilder()
            .putFields(RocksDBDefaultConfigs.DB_ROOT_DIR, Value.newBuilder().setStringValue(dbRoot.toString()).build())
            .putFields(RocksDBDefaultConfigs.DB_CHECKPOINT_ROOT_DIR, Value.newBuilder().setStringValue(cpRoot.toString()).build())
            .build();
        engine = new RocksDBCPableKVEngine(null, conf);
        engine.start("tag", "value");

        String after = Files.readString(spaceRoot.resolve(ACTIVE_GEN_POINTER)).trim();
        assertEquals(after, uuid);

        ICPableKVSpace space = engine.spaces().get(spaceId);
        space.toWriter().put(ByteString.copyFromUtf8("a"), ByteString.copyFromUtf8("b")).done();
        try (IKVSpaceReader reader = space.reader()) {
            assertEquals(reader.get(ByteString.copyFromUtf8("a")).get().toStringUtf8(), "b");
        }
    }
}
