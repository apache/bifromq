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

public class RocksDBCPableKVSpaceRestoreOverlayTest {
    private Path tmpRoot;
    private RocksDBCPableKVEngine engine;

    @BeforeMethod
    public void setup() throws Exception {
        tmpRoot = Files.createTempDirectory("kvspace_restore_overlay_");
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
    public void testOverlayKeepsExistingAndOverrides() throws Exception {
        String spaceId = "space_overlay";
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

        // pre-existing data
        ByteString keyA = ByteString.copyFromUtf8("A");
        ByteString oldA = ByteString.copyFromUtf8("oldA");
        ByteString keyB = ByteString.copyFromUtf8("B");
        ByteString oldB = ByteString.copyFromUtf8("oldB");
        ByteString mx = ByteString.copyFromUtf8("x");
        ByteString mvx = ByteString.copyFromUtf8("mx");
        space.toWriter().put(keyA, oldA).put(keyB, oldB).metadata(mx, mvx).done();

        Path pointer = dbRoot.resolve(spaceId).resolve(ACTIVE_GEN_POINTER);
        String before = Files.readString(pointer).trim();

        IRestoreSession session = space.startReceiving((c, b) -> {});
        ByteString newA = ByteString.copyFromUtf8("newA");
        ByteString keyC = ByteString.copyFromUtf8("C");
        ByteString valC = ByteString.copyFromUtf8("valC");
        ByteString my = ByteString.copyFromUtf8("y");
        ByteString mvy = ByteString.copyFromUtf8("my");
        session.put(keyA, newA).put(keyC, valC).metadata(my, mvy);
        session.done();

        // A overridden, B kept, C added
        try (IKVSpaceReader reader = space.reader()) {
            assertEquals(reader.get(keyA).get().toStringUtf8(), "newA");
            assertEquals(reader.get(keyB).get().toStringUtf8(), "oldB");
            assertEquals(reader.get(keyC).get().toStringUtf8(), "valC");
            assertTrue(reader.metadata(mx).isPresent());
            assertTrue(reader.metadata(my).isPresent());
        }

        String after = Files.readString(pointer).trim();
        assertNotEquals(after, before);
    }
}
