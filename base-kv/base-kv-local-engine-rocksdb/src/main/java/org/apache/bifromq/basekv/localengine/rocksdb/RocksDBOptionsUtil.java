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

import static org.apache.bifromq.basekv.localengine.StructUtil.boolVal;
import static org.apache.bifromq.basekv.localengine.StructUtil.numVal;
import static org.apache.bifromq.basekv.localengine.StructUtil.strVal;
import static org.apache.bifromq.basekv.localengine.rocksdb.AutoCleaner.autoRelease;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.ASYNC_WAL_FLUSH;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.BLOCK_CACHE_SIZE;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.ENABLE_STATS;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.INCREASE_PARALLELISM;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.LEVEL0_FILE_NUM_COMPACTION_TRIGGER;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.LEVEL0_SLOWDOWN_WRITES_TRIGGER;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.LEVEL0_STOP_WRITES_TRIGGER;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.MAX_BACKGROUND_JOBS;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.MAX_BYTES_FOR_LEVEL_BASE;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.MAX_WRITE_BUFFER_NUMBER;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.MIN_BLOB_SIZE;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.MIN_WRITE_BUFFER_NUMBER_TO_MERGE;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.STATS_LEVEL;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.TARGET_FILE_SIZE_BASE;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.WRITE_BUFFER_SIZE;

import com.google.protobuf.Struct;
import java.util.EnumSet;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.DataBlockIndexType;
import org.rocksdb.Env;
import org.rocksdb.HistogramType;
import org.rocksdb.IndexType;
import org.rocksdb.LRUCache;
import org.rocksdb.PrepopulateBlobCache;
import org.rocksdb.RateLimiter;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.util.SizeUnit;

/**
 * Build RocksDB options from Struct configuration. Keys align with provider schema.
 */
final class RocksDBOptionsUtil {
    private static DBOptions buildDBOptions(Struct conf) {
        DBOptions opts = new DBOptions();
        opts.setEnv(Env.getDefault())
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            .setAvoidUnnecessaryBlockingIO(true)
            .setMaxManifestFileSize(64 * SizeUnit.MB)
            // info log file settings
            .setMaxLogFileSize(128 * SizeUnit.MB)
            .setKeepLogFileNum(4)
            // wal file settings
            .setRecycleLogFileNum(4)
            .setWalSizeLimitMB(0)
            .setWalTtlSeconds(0)
            .setEnablePipelinedWrite(true)
            .setTwoWriteQueues(false)
            .setRateLimiter(autoRelease(new RateLimiter(512 * SizeUnit.MB,
                RateLimiter.DEFAULT_REFILL_PERIOD_MICROS,
                RateLimiter.DEFAULT_FAIRNESS,
                RateLimiter.DEFAULT_MODE, true), opts))
            .setMaxOpenFiles(256)
            .setIncreaseParallelism((int) numVal(conf, INCREASE_PARALLELISM))
            .setMaxBackgroundJobs((int) numVal(conf, MAX_BACKGROUND_JOBS));
        // Atomic flush not used in current scenarios
        opts.setAtomicFlush(false);

        if (boolVal(conf, ENABLE_STATS)) {
            EnumSet<HistogramType> ignoreTypes = EnumSet.allOf(HistogramType.class);
            ignoreTypes.remove(HistogramType.DB_GET);
            ignoreTypes.remove(HistogramType.DB_WRITE);
            ignoreTypes.remove(HistogramType.DB_SEEK);
            ignoreTypes.remove(HistogramType.SST_READ_MICROS);
            ignoreTypes.remove(HistogramType.SST_WRITE_MICROS);
            ignoreTypes.remove(HistogramType.BLOB_DB_GET_MICROS);
            ignoreTypes.remove(HistogramType.BLOB_DB_WRITE_MICROS);
            ignoreTypes.remove(HistogramType.FLUSH_TIME);
            ignoreTypes.remove(HistogramType.COMPACTION_TIME);
            Statistics statistics = new Statistics(ignoreTypes);
            String level = strVal(conf, STATS_LEVEL);
            statistics.setStatsLevel(StatsLevel.valueOf(level));
            opts.setStatistics(statistics);
        }
        return opts;
    }

    static DBOptions buildCPableDBOption(Struct conf) {
        DBOptions dbOptions = buildDBOptions(conf);
        dbOptions.setRecycleLogFileNum(0)
            .setAllowConcurrentMemtableWrite(true)
            .setBytesPerSync(1048576);
        return dbOptions;
    }

    static DBOptions buildWALableDBOption(Struct conf) {
        DBOptions dbOptions = buildDBOptions(conf);
        dbOptions.setManualWalFlush(boolVal(conf, ASYNC_WAL_FLUSH))
            .setBytesPerSync(1048576)
            .setAllowConcurrentMemtableWrite(true);
        return dbOptions;
    }

    static ColumnFamilyDescriptor buildCPableCFDesc(String name, Struct conf) {
        ColumnFamilyOptions cfOptions = buildCFOptions(conf);
        cfOptions.setCompressionType(CompressionType.NO_COMPRESSION);
        return new ColumnFamilyDescriptor(name.getBytes(), cfOptions);
    }

    static ColumnFamilyDescriptor buildWAlableCFDesc(String name, Struct conf) {
        return new ColumnFamilyDescriptor(name.getBytes(), buildCFOptions(conf));
    }

    private static ColumnFamilyOptions buildCFOptions(Struct conf) {
        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
        cfOptions
            // immutable options start
            .setMergeOperatorName("uint64add")
            .setTableFormatConfig(
                new BlockBasedTableConfig() //
                    // Begin to use partitioned index filters
                    // https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters#how-to-use-it
                    .setIndexType(IndexType.kTwoLevelIndexSearch) //
                    .setFilterPolicy(autoRelease(new BloomFilter(16, false), cfOptions))
                    .setPartitionFilters(true) //
                    .setMetadataBlockSize(8 * SizeUnit.KB) //
                    .setCacheIndexAndFilterBlocks(true) //
                    .setPinTopLevelIndexAndFilter(true)
                    .setCacheIndexAndFilterBlocksWithHighPriority(true) //
                    .setPinL0FilterAndIndexBlocksInCache(true) //
                    // To speed up point-lookup
                    // https://rocksdb.org/blog/2018/08/23/data-block-hash-index.html
                    .setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash)
                    .setDataBlockHashTableUtilRatio(0.75)
                    // End of partitioned index filters settings.
                    .setBlockSize(4 * SizeUnit.KB)//
                    .setBlockCache(autoRelease(new LRUCache((long) numVal(conf, BLOCK_CACHE_SIZE), 8), cfOptions)))
            // https://github.com/facebook/rocksdb/pull/5744
            .setForceConsistencyChecks(true)
            .setCompactionStyle(CompactionStyle.LEVEL)
            .setPrepopulateBlobCache(PrepopulateBlobCache.PREPOPULATE_BLOB_FLUSH_ONLY)
            // mutable options start
            .setCompressionType(CompressionType.LZ4_COMPRESSION)
            .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
            // Flushing options:
            // write_buffer_size sets the size of a single mem_table. Once mem_table exceeds
            // this size, it is marked immutable and a new one is created.
            .setWriteBufferSize((long) numVal(conf, WRITE_BUFFER_SIZE))
            // Flushing options:
            // max_write_buffer_number sets the maximum number of mem_tables, both active
            // and immutable.  If the active mem_table fills up and the total number of
            // mem_tables is larger than max_write_buffer_number we stall further writes.
            // This may happen if the flush process is slower than the write rate.
            .setMaxWriteBufferNumber((int) numVal(conf, MAX_WRITE_BUFFER_NUMBER))
            // Flushing options:
            // min_write_buffer_number_to_merge is the minimum number of mem_tables to be
            // merged before flushing to storage. For example, if this option is set to 2,
            // immutable mem_tables are only flushed when there are two of them - a single
            // immutable mem_table will never be flushed.  If multiple mem_tables are merged
            // together, less data may be written to storage since two updates are merged to
            // a single key. However, every Get() must traverse all immutable mem_tables
            // linearly to check if the key is there. Setting this option too high may hurt
            // read performance.
            .setMinWriteBufferNumberToMerge((int) numVal(conf, MIN_WRITE_BUFFER_NUMBER_TO_MERGE))
            // Level Style Compaction:
            // level0_file_num_compaction_trigger -- Once level 0 reaches this number of
            // files, L0->L1 compaction is triggered. We can therefore estimate level 0
            // size in stable state as
            // write_buffer_size * min_write_buffer_number_to_merge * level0_file_num_compaction_trigger.
            .setLevel0FileNumCompactionTrigger((int) numVal(conf, LEVEL0_FILE_NUM_COMPACTION_TRIGGER))
            // Level Style Compaction:
            // max_bytes_for_level_base and max_bytes_for_level_multiplier
            //  -- max_bytes_for_level_base is total size of level 1. As mentioned, we
            // recommend that this be around the size of level 0. Each subsequent level
            // is max_bytes_for_level_multiplier larger than previous one. The default
            // is 10 and we do not recommend changing that.
            .setMaxBytesForLevelBase((long) numVal(conf, MAX_BYTES_FOR_LEVEL_BASE))
            // Level Style Compaction:
            // target_file_size_base and target_file_size_multiplier
            //  -- Files in level 1 will have target_file_size_base bytes. Each next
            // level's file size will be target_file_size_multiplier bigger than previous
            // one. However, by default target_file_size_multiplier is 1, so files in all
            // L1..LMax levels are equal. Increasing target_file_size_base will reduce total
            // number of database files, which is generally a good thing. We recommend setting
            // target_file_size_base to be max_bytes_for_level_base / 10, so that there are
            // 10 files in level 1.
            .setTargetFileSizeBase((long) numVal(conf, TARGET_FILE_SIZE_BASE))
            // If prefix_extractor is set and memtable_prefix_bloom_size_ratio is not 0,
            // create prefix bloom for memtable with the size of
            // write_buffer_size * memtable_prefix_bloom_size_ratio.
            // If it is larger than 0.25, it is santinized to 0.25.
            .setMemtablePrefixBloomSizeRatio(0.125)
            // Soft limit on number of level-0 files. We start slowing down writes at this
            // point. A value 0 means that no writing slow down will be triggered by number
            // of files in level-0.
            .setLevel0SlowdownWritesTrigger((int) numVal(conf, LEVEL0_SLOWDOWN_WRITES_TRIGGER))
            // Maximum number of level-0 files.  We stop writes at this point.
            .setLevel0StopWritesTrigger((int) numVal(conf, LEVEL0_STOP_WRITES_TRIGGER))
            .setLevelCompactionDynamicLevelBytes(false)
            // enable blob files
            .setEnableBlobFiles(true)
            .setPrepopulateBlobCache(PrepopulateBlobCache.PREPOPULATE_BLOB_FLUSH_ONLY)
            .setMinBlobSize((long) numVal(conf, MIN_BLOB_SIZE))
            .enableBlobGarbageCollection();
        return cfOptions;
    }
}
