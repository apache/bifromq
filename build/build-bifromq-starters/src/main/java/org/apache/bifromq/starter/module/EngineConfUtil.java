/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.starter.module;

import org.apache.bifromq.basekv.localengine.ICPableKVEngineConfigurator;
import org.apache.bifromq.basekv.localengine.IWALableKVEngineConfigurator;
import org.apache.bifromq.basekv.localengine.memory.InMemKVEngineConfigurator;
import org.apache.bifromq.basekv.localengine.rocksdb.RocksDBCPableKVEngineConfigurator;
import org.apache.bifromq.basekv.localengine.rocksdb.RocksDBWALableKVEngineConfigurator;
import org.apache.bifromq.starter.config.model.InMemEngineConfig;
import org.apache.bifromq.starter.config.model.RocksDBEngineConfig;
import org.apache.bifromq.starter.config.model.StorageEngineConfig;
import java.nio.file.Path;
import java.nio.file.Paths;

public class EngineConfUtil {
    public static final String USER_DIR_PROP = "user.dir";
    public static final String DATA_DIR_PROP = "DATA_DIR";

    public static ICPableKVEngineConfigurator buildDataEngineConf(StorageEngineConfig config, String name) {
        if (config instanceof InMemEngineConfig) {
            return InMemKVEngineConfigurator.builder()
                .build();
        } else {
            Path dataRootDir;
            Path dataCheckpointRootDir;
            RocksDBEngineConfig rocksDBConfig = (RocksDBEngineConfig) config;
            if (Paths.get(rocksDBConfig.getDataPathRoot()).isAbsolute()) {
                dataRootDir = Paths.get(rocksDBConfig.getDataPathRoot(), name);
                dataCheckpointRootDir =
                    Paths.get(rocksDBConfig.getDataPathRoot(), name + "_cp");
            } else {
                String userDir = System.getProperty(USER_DIR_PROP);
                String dataDir = System.getProperty(DATA_DIR_PROP, userDir);
                dataRootDir = Paths.get(dataDir, rocksDBConfig.getDataPathRoot(), name);
                dataCheckpointRootDir =
                    Paths.get(dataDir, rocksDBConfig.getDataPathRoot(), name + "_cp");
            }
            return RocksDBCPableKVEngineConfigurator.builder()
                .dbRootDir(dataRootDir.toString())
                .dbCheckpointRootDir(dataCheckpointRootDir.toString())
                .heuristicCompaction(rocksDBConfig.isManualCompaction())
                .compactMinTombstoneKeys(rocksDBConfig.getCompactMinTombstoneKeys())
                .compactMinTombstoneRanges(rocksDBConfig.getCompactMinTombstoneRanges())
                .compactTombstoneKeysRatio(rocksDBConfig.getCompactTombstoneRatio())

                .blockCacheSize(rocksDBConfig.getBlockCacheSize())
                .writeBufferSize(rocksDBConfig.getWriteBufferSize())
                .maxWriteBufferNumber(rocksDBConfig.getMaxWriteBufferNumber())
                .minWriteBufferNumberToMerge(rocksDBConfig.getMinWriteBufferNumberToMerge())
                .minBlobSize(rocksDBConfig.getMinBlobSize())
                .increaseParallelism(rocksDBConfig.getIncreaseParallelism())
                .maxBackgroundJobs(rocksDBConfig.getMaxBackgroundJobs())
                .level0FileNumCompactionTrigger(rocksDBConfig.getLevel0FileNumCompactionTrigger())
                .level0SlowdownWritesTrigger(rocksDBConfig.getLevel0SlowdownWritesTrigger())
                .level0StopWritesTrigger(rocksDBConfig.getLevel0StopWritesTrigger())
                .maxBytesForLevelBase(rocksDBConfig.getMaxBytesForLevelBase())
                .targetFileSizeBase(rocksDBConfig.getTargetFileSizeBase())
                .build();
        }
    }

    public static IWALableKVEngineConfigurator buildWALEngineConf(StorageEngineConfig config, String name) {
        if (config instanceof InMemEngineConfig) {
            return InMemKVEngineConfigurator.builder()
                .build();
        } else {
            Path dataRootDir;
            RocksDBEngineConfig rocksDBConfig = (RocksDBEngineConfig) config;
            if (Paths.get(rocksDBConfig.getDataPathRoot()).isAbsolute()) {
                dataRootDir = Paths.get(rocksDBConfig.getDataPathRoot(), name);
            } else {
                String userDir = System.getProperty(USER_DIR_PROP);
                String dataDir = System.getProperty(DATA_DIR_PROP, userDir);
                dataRootDir = Paths.get(dataDir, rocksDBConfig.getDataPathRoot(), name);
            }
            return RocksDBWALableKVEngineConfigurator.builder()
                .dbRootDir(dataRootDir.toString())
                .heuristicCompaction(rocksDBConfig.isManualCompaction())
                .compactMinTombstoneKeys(rocksDBConfig.getCompactMinTombstoneKeys())
                .compactMinTombstoneRanges(rocksDBConfig.getCompactMinTombstoneRanges())
                .compactTombstoneKeysRatio(rocksDBConfig.getCompactTombstoneRatio())
                .asyncWALFlush(rocksDBConfig.isAsyncWALFlush())
                .fsyncWAL(rocksDBConfig.isFsyncWAL())

                .blockCacheSize(rocksDBConfig.getBlockCacheSize())
                .writeBufferSize(rocksDBConfig.getWriteBufferSize())
                .maxWriteBufferNumber(rocksDBConfig.getMaxWriteBufferNumber())
                .minWriteBufferNumberToMerge(rocksDBConfig.getMinWriteBufferNumberToMerge())
                .minBlobSize(rocksDBConfig.getMinBlobSize())
                .increaseParallelism(rocksDBConfig.getIncreaseParallelism())
                .maxBackgroundJobs(rocksDBConfig.getMaxBackgroundJobs())
                .level0FileNumCompactionTrigger(rocksDBConfig.getLevel0FileNumCompactionTrigger())
                .level0SlowdownWritesTrigger(rocksDBConfig.getLevel0SlowdownWritesTrigger())
                .level0StopWritesTrigger(rocksDBConfig.getLevel0StopWritesTrigger())
                .maxBytesForLevelBase(rocksDBConfig.getMaxBytesForLevelBase())
                .targetFileSizeBase(rocksDBConfig.getTargetFileSizeBase())
                .build();
        }
    }
}
