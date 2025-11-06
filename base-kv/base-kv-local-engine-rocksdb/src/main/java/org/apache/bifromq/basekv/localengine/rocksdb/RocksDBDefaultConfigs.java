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

import static org.apache.bifromq.basekv.localengine.StructUtil.toValue;

import com.google.protobuf.Struct;
import org.apache.bifromq.baseenv.EnvProvider;
import org.rocksdb.StatsLevel;
import org.rocksdb.util.SizeUnit;

/**
 * Default configuration constants for RocksDB engines.
 */
public final class RocksDBDefaultConfigs {
    public static final String DB_ROOT_DIR = "dbRootDir";
    public static final String DB_CHECKPOINT_ROOT_DIR = "dbCheckpointRootDir";
    public static final String ENABLE_STATS = "enableStats";
    public static final String STATS_LEVEL = "statsLevel";
    public static final String MANUAL_COMPACTION = "manualCompaction";
    public static final String COMPACT_MIN_TOMBSTONE_KEYS = "compactMinTombstoneKeys";
    public static final String COMPACT_MIN_TOMBSTONE_RANGES = "compactMinTombstoneRanges";
    public static final String COMPACT_TOMBSTONE_RATIO = "compactTombstoneRatio";
    public static final String BLOCK_CACHE_SIZE = "blockCacheSize";
    public static final String WRITE_BUFFER_SIZE = "writeBufferSize";
    public static final String MAX_WRITE_BUFFER_NUMBER = "maxWriteBufferNumber";
    public static final String MIN_WRITE_BUFFER_NUMBER_TO_MERGE = "minWriteBufferNumberToMerge";
    public static final String MIN_BLOB_SIZE = "minBlobSize";
    public static final String INCREASE_PARALLELISM = "increaseParallelism";
    public static final String MAX_BACKGROUND_JOBS = "maxBackgroundJobs";
    public static final String LEVEL0_FILE_NUM_COMPACTION_TRIGGER = "level0FileNumCompactionTrigger";
    public static final String LEVEL0_SLOWDOWN_WRITES_TRIGGER = "level0SlowdownWritesTrigger";
    public static final String LEVEL0_STOP_WRITES_TRIGGER = "level0StopWritesTrigger";
    public static final String MAX_BYTES_FOR_LEVEL_BASE = "maxBytesForLevelBase";
    public static final String TARGET_FILE_SIZE_BASE = "targetFileSizeBase";
    public static final String ASYNC_WAL_FLUSH = "asyncWALFlush";
    public static final String FSYNC_WAL = "fsyncWAL";
    public static final Struct CP;
    public static final Struct WAL;

    static {
        // Build CP with all defaults
        Struct.Builder configBuilder = Struct.newBuilder();
        configBuilder.putFields(DB_ROOT_DIR, toValue(""));
        configBuilder.putFields(ENABLE_STATS, toValue(false));
        configBuilder.putFields(STATS_LEVEL, toValue(StatsLevel.EXCEPT_DETAILED_TIMERS.name()));
        configBuilder.putFields(MANUAL_COMPACTION, toValue(true));
        configBuilder.putFields(COMPACT_MIN_TOMBSTONE_KEYS, toValue(200000));
        configBuilder.putFields(COMPACT_MIN_TOMBSTONE_RANGES, toValue(100000));
        configBuilder.putFields(COMPACT_TOMBSTONE_RATIO, toValue(0.3));
        configBuilder.putFields(BLOCK_CACHE_SIZE, toValue(32 * SizeUnit.MB));
        configBuilder.putFields(WRITE_BUFFER_SIZE, toValue(128 * SizeUnit.MB));
        configBuilder.putFields(MAX_WRITE_BUFFER_NUMBER, toValue(6));
        configBuilder.putFields(MIN_WRITE_BUFFER_NUMBER_TO_MERGE, toValue(2));
        configBuilder.putFields(MIN_BLOB_SIZE, toValue(2 * SizeUnit.KB));
        configBuilder.putFields(INCREASE_PARALLELISM,
            toValue(Math.max(EnvProvider.INSTANCE.availableProcessors() / 4, 2)));
        configBuilder.putFields(MAX_BACKGROUND_JOBS,
            toValue(Math.max(EnvProvider.INSTANCE.availableProcessors() / 4, 2)));
        configBuilder.putFields(LEVEL0_FILE_NUM_COMPACTION_TRIGGER, toValue(8));
        configBuilder.putFields(LEVEL0_SLOWDOWN_WRITES_TRIGGER, toValue(20));
        configBuilder.putFields(LEVEL0_STOP_WRITES_TRIGGER, toValue(24));
        configBuilder.putFields(MAX_BYTES_FOR_LEVEL_BASE, toValue(128 * 2 * 8 * SizeUnit.MB));
        configBuilder.putFields(TARGET_FILE_SIZE_BASE, toValue(128 * 2 * SizeUnit.MB));
        Struct sharedConfig = configBuilder.build();
        CP = sharedConfig.toBuilder()
            .putFields(DB_CHECKPOINT_ROOT_DIR, toValue(""))
            .build();

        // Build WAL based on CP
        WAL = sharedConfig.toBuilder()
            .putFields(ASYNC_WAL_FLUSH, toValue(true))
            .putFields(FSYNC_WAL, toValue(false))
            .build();
    }
}
