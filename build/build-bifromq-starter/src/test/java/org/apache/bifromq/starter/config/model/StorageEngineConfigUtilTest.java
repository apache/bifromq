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

package org.apache.bifromq.starter.config.model;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class StorageEngineConfigUtilTest {

    @Test
    public void replaceToMemoryWhenCurrentIsRocksDB() {
        StorageEngineConfig current = new RocksDBEngineConfig();
        Map<String, Object> conf = new HashMap<>();
        conf.put("type", "memory");

        StorageEngineConfig updated = StorageEngineConfigUtil.updateOrReplace(current, conf);
        assertTrue(updated instanceof InMemEngineConfig);
    }

    @Test
    public void replaceToMemoryWhenCurrentIsNull() {
        StorageEngineConfig current = null;
        Map<String, Object> conf = new HashMap<>();
        conf.put("type", "memory");

        StorageEngineConfig updated = StorageEngineConfigUtil.updateOrReplace(current, conf);
        assertTrue(updated instanceof InMemEngineConfig);
    }

    @Test
    public void mergeWhenTypeEqualRocksdb() {
        RocksDBEngineConfig current = new RocksDBEngineConfig();
        // default manualCompaction is false in RocksDBEngineConfig
        Map<String, Object> conf = new HashMap<>();
        conf.put("type", "rocksdb");
        conf.put("compactMinTombstoneKeys", 60000);

        StorageEngineConfig updated = StorageEngineConfigUtil.updateOrReplace(current, conf);
        RocksDBEngineConfig rocks = (RocksDBEngineConfig) updated;
        assertEquals(rocks.getCompactMinTombstoneKeys(), 60000);
        // Unspecified fields remain unchanged
        assertFalse(rocks.isManualCompaction());
    }

    @Test
    public void mergeWithoutTypeWhenCurrentExists() {
        RocksDBEngineConfig current = new RocksDBEngineConfig();
        Map<String, Object> conf = new HashMap<>();
        conf.put("compactMinTombstoneRanges", 3);

        StorageEngineConfig updated = StorageEngineConfigUtil.updateOrReplace(current, conf);
        RocksDBEngineConfig rocks = (RocksDBEngineConfig) updated;
        assertEquals(rocks.getCompactMinTombstoneRanges(), 3);
        // Unspecified fields remain unchanged
        assertFalse(rocks.isManualCompaction());
    }

    @Test
    public void defaultToRocksDBWhenNoTypeAndNoCurrent() {
        StorageEngineConfig current = null;
        Map<String, Object> conf = new HashMap<>();
        conf.put("compactMinTombstoneKeys", 12345);

        StorageEngineConfig updated = StorageEngineConfigUtil.updateOrReplace(current, conf);
        RocksDBEngineConfig rocks = (RocksDBEngineConfig) updated;
        assertEquals(rocks.getCompactMinTombstoneKeys(), 12345);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void invalidTypeThrows() {
        StorageEngineConfig current = new RocksDBEngineConfig();
        Map<String, Object> conf = new HashMap<>();
        conf.put("type", "unknownEngine");
        StorageEngineConfigUtil.updateOrReplace(current, conf);
    }
}

