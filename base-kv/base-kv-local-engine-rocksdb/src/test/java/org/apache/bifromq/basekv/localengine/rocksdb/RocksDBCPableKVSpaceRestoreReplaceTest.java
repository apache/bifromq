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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceReader;
import org.apache.bifromq.basekv.localengine.IRestoreSession;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RocksDBCPableKVSpaceRestoreReplaceTest {
    private Path tmpRoot;
    private RocksDBCPableKVEngine engine;

    @BeforeMethod
    public void setup() throws Exception {
        tmpRoot = Files.createTempDirectory("kvspace_restore_replace_");
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
    public void testReplaceReplacesAllAndUpdatesPointer() throws Exception {
        String spaceId = "space_replace";
        Path dbRoot = tmpRoot.resolve("data");
        Path cpRoot = tmpRoot.resolve("cp");
        Files.createDirectories(dbRoot);
        Files.createDirectories(cpRoot);

        Struct conf = RocksDBDefaultConfigs.CP.toBuilder()
            .putFields(RocksDBDefaultConfigs.DB_ROOT_DIR, Value.newBuilder().setStringValue(dbRoot.toString()).build())
            .putFields(RocksDBDefaultConfigs.DB_CHECKPOINT_ROOT_DIR, Value.newBuilder().setStringValue(cpRoot.toString()).build())
            .build();
        engine = new RocksDBCPableKVEngine(null, conf);
        engine.start("tag", "value");

        ICPableKVSpace space = engine.createIfMissing(spaceId);
        // prepare existing data
        ByteString k1 = ByteString.copyFromUtf8("k1");
        ByteString v1 = ByteString.copyFromUtf8("v1");
        ByteString m1 = ByteString.copyFromUtf8("m1");
        ByteString mv1 = ByteString.copyFromUtf8("mv1");
        space.toWriter().put(k1, v1).metadata(m1, mv1).done();
        try (IKVSpaceReader reader = space.reader()) {
            assertTrue(reader.exist(k1));
        }

        Path pointer = dbRoot.resolve(spaceId).resolve(ACTIVE_GEN_POINTER);
        String before = Files.readString(pointer).trim();

        // restore with replace
        IRestoreSession session = space.startRestore((c, b) -> {});
        ByteString k2 = ByteString.copyFromUtf8("k2");
        ByteString v2 = ByteString.copyFromUtf8("v2");
        ByteString m2 = ByteString.copyFromUtf8("m2");
        ByteString mv2 = ByteString.copyFromUtf8("mv2");
        session.put(k2, v2);
        session.metadata(m2, mv2);
        session.done();

        // old key removed, new key present
        try (IKVSpaceReader reader = space.reader()) {
            assertFalse(reader.exist(k1));
            assertTrue(reader.exist(k2));
            assertTrue(reader.metadata(m2).isPresent());
            assertFalse(reader.metadata(m1).isPresent());
        }

        String after = Files.readString(pointer).trim();
        assertNotEquals(after, before);

        // idempotent overwrite in replace mode
        IRestoreSession s2 = space.startRestore((c, b) -> {});
        s2.put(k2, ByteString.copyFromUtf8("v3"));
        s2.put(k2, ByteString.copyFromUtf8("v4"));
        s2.done();
        try (IKVSpaceReader reader = space.reader()) {
            assertEquals(reader.get(k2).get().toStringUtf8(), "v4");
        }
    }
}
