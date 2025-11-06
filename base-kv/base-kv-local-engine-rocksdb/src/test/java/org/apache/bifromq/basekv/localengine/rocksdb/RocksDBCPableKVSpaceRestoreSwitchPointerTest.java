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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceRefreshableReader;
import org.apache.bifromq.basekv.localengine.IRestoreSession;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RocksDBCPableKVSpaceRestoreSwitchPointerTest {
    private Path tmpRoot;
    private RocksDBCPableKVEngine engine;

    @BeforeMethod
    public void setup() throws Exception {
        tmpRoot = Files.createTempDirectory("kvspace_restore_");
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
    public void testPointerUpdatedAfterRestoreReplace() throws Exception {
        String spaceId = "space_restore";
        Path dbRoot = tmpRoot.resolve("data");
        Path cpRoot = tmpRoot.resolve("cp");
        Files.createDirectories(dbRoot.resolve(spaceId));
        Files.createDirectories(cpRoot);
        // Ensure engine identity exists when root is non-empty
        Files.writeString(dbRoot.resolve(IDENTITY_FILE), "test-id");

        Struct conf = RocksDBDefaultConfigs.CP.toBuilder()
            .putFields(RocksDBDefaultConfigs.DB_ROOT_DIR, Value.newBuilder().setStringValue(dbRoot.toString()).build())
            .putFields(RocksDBDefaultConfigs.DB_CHECKPOINT_ROOT_DIR, Value.newBuilder().setStringValue(cpRoot.toString()).build())
            .build();
        engine = new RocksDBCPableKVEngine(null, conf);
        engine.start("tag", "value");

        ICPableKVSpace space = engine.spaces().get(spaceId);

        Path pointer = dbRoot.resolve(spaceId).resolve(ACTIVE_GEN_POINTER);
        String before = Files.readString(pointer).trim();

        IRestoreSession session = space.startRestore((c, b) -> {});
        session.put(ByteString.copyFromUtf8("k1"), ByteString.copyFromUtf8("v1"));
        session.done();

        String after = Files.readString(pointer).trim();
        assertNotEquals(after, before);
        try (IKVSpaceRefreshableReader reader = space.reader()) {
            assertTrue(reader.get(ByteString.copyFromUtf8("k1")).isPresent());
        }
    }
}
