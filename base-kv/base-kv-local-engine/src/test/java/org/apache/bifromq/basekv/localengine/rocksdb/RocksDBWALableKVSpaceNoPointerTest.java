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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RocksDBWALableKVSpaceNoPointerTest {
    private Path tmpRoot;
    private RocksDBWALableKVEngine engine;

    @BeforeMethod
    public void setup() throws Exception {
        tmpRoot = Files.createTempDirectory("kvspace_wal_");
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
    public void testNoPointerCreatedForWALable() throws Exception {
        String spaceId = "wal_space";
        Path dbRoot = tmpRoot.resolve("data");
        Files.createDirectories(dbRoot);

        Struct conf = RocksDBDefaultConfigs.WAL.toBuilder()
            .putFields(RocksDBDefaultConfigs.DB_ROOT_DIR, Value.newBuilder().setStringValue(dbRoot.toString()).build())
            .build();
        engine = new RocksDBWALableKVEngine(null, conf);
        engine.start("tag", "value");
        engine.createIfMissing(spaceId);

        Path pointer = dbRoot.resolve(spaceId).resolve(ACTIVE_GEN_POINTER);
        assertFalse(Files.exists(pointer));
        // DB files should be directly under space root
        assertTrue(Files.list(dbRoot.resolve(spaceId)).anyMatch(Files::isRegularFile));
    }
}
