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
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.Stream;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IRestoreSession;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RocksDBCPableKVSpaceRuntimeCleanerTest {
    private Path tmpRoot;
    private RocksDBCPableKVEngine engine;

    @BeforeMethod
    public void setup() throws Exception {
        tmpRoot = Files.createTempDirectory("kvspace_runtime_cleaner_");
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
    public void testCleanerDeletesInactiveGenerationAtRuntime() throws Exception {
        String spaceId = "space_runtime_cleaner";
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
        // write something to current generation
        space.toWriter().put(ByteString.copyFromUtf8("k"), ByteString.copyFromUtf8("v")).done();

        Path spaceRoot = dbRoot.resolve(spaceId);
        Path pointer = spaceRoot.resolve(ACTIVE_GEN_POINTER);
        String before = Files.readString(pointer).trim();
        assertTrue(Files.exists(spaceRoot.resolve(before)));

        // Replace restore to force generation switch
        IRestoreSession session = space.startRestore((c, b) -> {});
        session.put(ByteString.copyFromUtf8("k2"), ByteString.copyFromUtf8("v2"));
        session.done();

        String after = Files.readString(pointer).trim();
        assertNotEquals(after, before);
        // New generation directory should exist immediately
        assertTrue(Files.exists(spaceRoot.resolve(after)));

        // Await eventual cleanup by Cleaner/GC to avoid relying on immediate deletion
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            // Hint GC to speed up Cleaner, but do not rely on it
            System.gc();
            System.runFinalization();

            // Old generation directory should eventually be deleted at runtime by cleaner
            assertTrue(Files.notExists(spaceRoot.resolve(before)));

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
        });
    }
}
