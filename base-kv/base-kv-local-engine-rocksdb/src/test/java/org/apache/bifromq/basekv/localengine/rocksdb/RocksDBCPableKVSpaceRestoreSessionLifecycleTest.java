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

import static org.testng.Assert.assertThrows;
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

public class RocksDBCPableKVSpaceRestoreSessionLifecycleTest {
    private Path tmpRoot;
    private RocksDBCPableKVEngine engine;

    @BeforeMethod
    public void setup() throws Exception {
        tmpRoot = Files.createTempDirectory("kvspace_restore_lifecycle_");
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
    public void testSessionClosedAfterDoneOrAbort() throws Exception {
        String spaceId = "space_lifecycle";
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

        // done closes session
        IRestoreSession s1 = space.startRestore((c, b) -> {});
        s1.put(ByteString.copyFromUtf8("k"), ByteString.copyFromUtf8("v"));
        s1.done();
        try (IKVSpaceReader reader = space.reader()) {
            assertTrue(reader.get(ByteString.copyFromUtf8("k")).isPresent());
        }
        assertThrows(IllegalStateException.class, () -> s1.put(ByteString.copyFromUtf8("k2"), ByteString.copyFromUtf8("v2")));
        assertThrows(IllegalStateException.class, () -> s1.metadata(ByteString.copyFromUtf8("m"), ByteString.copyFromUtf8("mv")));

        // abort closes session
        IRestoreSession s2 = space.startRestore((c, b) -> {});
        s2.put(ByteString.copyFromUtf8("x"), ByteString.copyFromUtf8("y"));
        s2.abort();
        assertThrows(IllegalStateException.class, () -> s2.put(ByteString.copyFromUtf8("x2"), ByteString.copyFromUtf8("y2")));
        assertThrows(IllegalStateException.class, () -> s2.metadata(ByteString.copyFromUtf8("m2"), ByteString.copyFromUtf8("mv2")));
    }
}
