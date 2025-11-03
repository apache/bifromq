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

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IRestoreSession;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RocksDBCPableKVSpaceRestoreRestartCleanupTest {
    private Path tmpRoot;
    private RocksDBCPableKVEngine engine;

    @BeforeMethod
    public void setup() throws Exception {
        tmpRoot = Files.createTempDirectory("kvspace_restore_restart_");
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
    public void testRestartCleansInactiveGenerations() throws Exception {
        String spaceId = "space_restart_cleanup";
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

        // initial write
        space.toWriter().put(ByteString.copyFromUtf8("k"), ByteString.copyFromUtf8("v")).done();
        Path spaceRoot = dbRoot.resolve(spaceId);
        String before = Files.readString(spaceRoot.resolve(ACTIVE_GEN_POINTER)).trim();

        // restore replace to switch generation
        IRestoreSession session = space.startRestore((c, b) -> {});
        session.put(ByteString.copyFromUtf8("k2"), ByteString.copyFromUtf8("v2"));
        session.done();
        String after = Files.readString(spaceRoot.resolve(ACTIVE_GEN_POINTER)).trim();
        assertNotEquals(after, before);

        // stop/start to trigger cleanInactiveOnStartup
        engine.stop();
        engine = new RocksDBCPableKVEngine(null, conf);
        engine.start("tag", "value");

        long dirCount;
        long fileCount;
        try (Stream<Path> s = Files.list(spaceRoot)) {
            dirCount = s.filter(Files::isDirectory).count();
        }
        try (Stream<Path> s = Files.list(spaceRoot)) {
            fileCount = s.filter(Files::isRegularFile).count();
        }
        assertEquals(dirCount, 1);
        assertEquals(fileCount, 1);
    }
}
