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

import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IRestoreSession;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RocksDBCPableKVSpaceRestoreFlushListenerTest {
    private Path tmpRoot;
    private RocksDBCPableKVEngine engine;

    @BeforeMethod
    public void setup() throws Exception {
        tmpRoot = Files.createTempDirectory("kvspace_restore_flush_");
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
    public void testBulkRestoreReportsFlushes() throws Exception {
        String spaceId = "space_flush_bulk";
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
        AtomicInteger callbackCount = new AtomicInteger();
        AtomicLong totalEntries = new AtomicLong();
        AtomicLong totalBytes = new AtomicLong();
        IRestoreSession session = space.startRestore((c, b) -> {
            // collect flush metrics
            callbackCount.incrementAndGet();
            totalEntries.addAndGet(c);
            totalBytes.addAndGet(b);
        });

        int n = 6000; // exceed default entry budget to trigger flush
        for (int i = 0; i < n; i++) {
            ByteString k = ByteString.copyFromUtf8("k" + i);
            ByteString v = ByteString.copyFromUtf8("v" + i);
            session.put(k, v);
        }
        session.done();

        assertTrue(callbackCount.get() >= 1, "should flush at least once during bulk restore");
        assertTrue(totalEntries.get() > 0, "reported entries should be positive");
        assertTrue(totalBytes.get() > 0, "reported bytes should be positive");
    }

    @Test
    public void testOverlayRestoreReportsFlushes() throws Exception {
        String spaceId = "space_flush_overlay";
        Path dbRoot = tmpRoot.resolve("data2");
        Path cpRoot = tmpRoot.resolve("cp2");
        Files.createDirectories(dbRoot);
        Files.createDirectories(cpRoot);

        Struct conf = RocksDBDefaultConfigs.CP.toBuilder()
            .putFields(RocksDBDefaultConfigs.DB_ROOT_DIR, Value.newBuilder().setStringValue(dbRoot.toString()).build())
            .putFields(RocksDBDefaultConfigs.DB_CHECKPOINT_ROOT_DIR, Value.newBuilder().setStringValue(cpRoot.toString()).build())
            .build();
        engine = new RocksDBCPableKVEngine(null, conf);
        engine.start("tag", "value");

        ICPableKVSpace space = engine.createIfMissing(spaceId);
        // pre-write something to ensure active DB is not empty
        space.toWriter().put(ByteString.copyFromUtf8("pre"), ByteString.copyFromUtf8("p")).done();

        AtomicInteger callbackCount = new AtomicInteger();
        IRestoreSession session = space.startReceiving((c, b) -> callbackCount.incrementAndGet());

        int n = 4000;
        for (int i = 0; i < n; i++) {
            ByteString k = ByteString.copyFromUtf8("ok" + i);
            ByteString v = ByteString.copyFromUtf8("ov" + i);
            session.put(k, v);
        }
        session.done();

        assertTrue(callbackCount.get() >= 1, "should flush at least once during overlay restore");
    }
}
