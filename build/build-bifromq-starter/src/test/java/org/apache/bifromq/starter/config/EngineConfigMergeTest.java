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

package org.apache.bifromq.starter.config;

import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.COMPACT_MIN_TOMBSTONE_KEYS;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.COMPACT_MIN_TOMBSTONE_RANGES;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.MANUAL_COMPACTION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.bifromq.starter.config.model.EngineConfig;
import org.apache.bifromq.starter.config.model.dist.DistWorkerConfig;
import org.apache.bifromq.starter.utils.ConfigFileUtil;
import org.testng.annotations.Test;

public class EngineConfigMergeTest {
    // Verify EngineConfig fields merge instead of overwrite
    @Test
    public void testInboxEngineConfigMerge() throws Exception {
        String yaml = "inboxServiceConfig:\n" +
            "  store:\n" +
            "    dataEngineConfig:\n" +
            "      type: \"rocksdb\"\n" +
            "      compactMinTombstoneKeys: 50000000\n" +
            "    walEngineConfig:\n" +
            "      type: \"rocksdb\"\n" +
            "      compactMinTombstoneKeys: 50000000\n";

        File f = File.createTempFile("bifromq-starter-config", ".yaml");
        Files.writeString(f.toPath(), yaml, StandardCharsets.UTF_8);

        StandaloneConfig cfg = ConfigFileUtil.build(f, StandaloneConfig.class);

        EngineConfig data = cfg.getInboxServiceConfig().getStore().getDataEngineConfig();
        assertEquals(data.getType(), "rocksdb");
        assertEquals(((Number) data.get(COMPACT_MIN_TOMBSTONE_KEYS)).longValue(), 50000000L);
        assertTrue((Boolean) data.get(MANUAL_COMPACTION));
        assertEquals(((Number) data.get(COMPACT_MIN_TOMBSTONE_RANGES)).intValue(), 100);

        EngineConfig wal = cfg.getInboxServiceConfig().getStore().getWalEngineConfig();
        assertEquals(wal.getType(), "rocksdb");
        assertEquals(((Number) wal.get(COMPACT_MIN_TOMBSTONE_KEYS)).longValue(), 50000000L);
        assertTrue((Boolean) wal.get(MANUAL_COMPACTION));
        assertEquals(((Number) wal.get(COMPACT_MIN_TOMBSTONE_RANGES)).intValue(), 2);
    }

    // Verify merge also applies to retain and dist worker configs
    @Test
    public void testRetainAndDistEngineConfigMerge() throws Exception {
        String yaml = "retainServiceConfig:\n" +
            "  store:\n" +
            "    dataEngineConfig:\n" +
            "      compactMinTombstoneKeys: 12345\n" +
            "    walEngineConfig:\n" +
            "      compactMinTombstoneKeys: 67890\n" +
            "distServiceConfig:\n" +
            "  worker:\n" +
            "    dataEngineConfig:\n" +
            "      compactMinTombstoneKeys: 111\n" +
            "    walEngineConfig:\n" +
            "      compactMinTombstoneKeys: 222\n";

        File f = File.createTempFile("bifromq-starter-config", ".yaml");
        Files.writeString(f.toPath(), yaml, StandardCharsets.UTF_8);

        StandaloneConfig cfg = ConfigFileUtil.build(f, StandaloneConfig.class);

        EngineConfig rData = cfg.getRetainServiceConfig().getStore().getDataEngineConfig();
        assertEquals(((Number) rData.get(COMPACT_MIN_TOMBSTONE_KEYS)).intValue(), 12345);
        assertTrue((Boolean) rData.get(MANUAL_COMPACTION));
        assertEquals(((Number) rData.get(COMPACT_MIN_TOMBSTONE_RANGES)).intValue(), 2);

        EngineConfig rWal = cfg.getRetainServiceConfig().getStore().getWalEngineConfig();
        assertEquals(((Number) rWal.get(COMPACT_MIN_TOMBSTONE_KEYS)).intValue(), 67890);
        assertTrue((Boolean) rWal.get(MANUAL_COMPACTION));
        assertEquals(((Number) rWal.get(COMPACT_MIN_TOMBSTONE_RANGES)).intValue(), 2);

        DistWorkerConfig workerCfg = cfg.getDistServiceConfig().getWorker();
        assertNotNull(workerCfg);
        EngineConfig dData = workerCfg.getDataEngineConfig();
        assertEquals(((Number) dData.get(COMPACT_MIN_TOMBSTONE_KEYS)).intValue(), 111);
        assertTrue((Boolean) dData.get(MANUAL_COMPACTION));
        assertEquals(((Number) dData.get(COMPACT_MIN_TOMBSTONE_RANGES)).intValue(), 2);

        EngineConfig dWal = workerCfg.getWalEngineConfig();
        assertEquals(((Number) dWal.get(COMPACT_MIN_TOMBSTONE_KEYS)).intValue(), 222);
        assertTrue((Boolean) dWal.get(MANUAL_COMPACTION));
        assertEquals(((Number) dWal.get(COMPACT_MIN_TOMBSTONE_RANGES)).intValue(), 2);
    }
}

