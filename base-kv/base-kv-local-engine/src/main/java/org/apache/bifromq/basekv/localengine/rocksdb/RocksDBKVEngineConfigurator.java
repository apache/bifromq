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

import static org.apache.bifromq.basekv.localengine.rocksdb.AutoCleaner.autoRelease;
import static java.lang.Math.max;

import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.basekv.localengine.IKVEngineConfigurator;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ColumnFamilyOptionsInterface;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.DBOptionsInterface;
import org.rocksdb.DataBlockIndexType;
import org.rocksdb.Env;
import org.rocksdb.IndexType;
import org.rocksdb.LRUCache;
import org.rocksdb.MutableColumnFamilyOptionsInterface;
import org.rocksdb.MutableDBOptionsInterface;
import org.rocksdb.PrepopulateBlobCache;
import org.rocksdb.RateLimiter;
import org.rocksdb.util.SizeUnit;

@NoArgsConstructor
@Getter
@Setter
@Accessors(chain = true, fluent = true)
@SuperBuilder(toBuilder = true)
public abstract class RocksDBKVEngineConfigurator<T extends RocksDBKVEngineConfigurator<T>>
    implements IKVEngineConfigurator {
    private String dbRootDir;
    @Builder.Default
    private boolean heuristicCompaction = false;
    @Builder.Default
    private int compactMinTombstoneKeys = 50000;
    @Builder.Default
    private int compactMinTombstoneRanges = 10000;
    @Builder.Default
    private double compactTombstoneKeysRatio = 0.3;
    @Builder.Default
    private long blockCacheSize = 32 * SizeUnit.MB;
    @Builder.Default
    private long writeBufferSize = 128 * SizeUnit.MB;
    @Builder.Default
    private int maxWriteBufferNumber = 6;
    @Builder.Default
    private int minWriteBufferNumberToMerge = 2;
    @Builder.Default
    private long minBlobSize = 2 * SizeUnit.KB;
    @Builder.Default
    private int increaseParallelism = max(EnvProvider.INSTANCE.availableProcessors() / 4, 2);
    @Builder.Default
    private int maxBackgroundJobs = max(EnvProvider.INSTANCE.availableProcessors() / 4, 2);
    @Builder.Default
    private int level0FileNumCompactionTrigger = 8;
    @Builder.Default
    private int level0SlowdownWritesTrigger = 20;
    @Builder.Default
    private int level0StopWritesTrigger = 24;
    @Builder.Default
    private long maxBytesForLevelBase = 512 * SizeUnit.MB;
    @Builder.Default
    private long targetFileSizeBase = 64 * SizeUnit.MB;

    public DBOptions dbOptions() {
        DBOptions targetOption = new DBOptions();
        configDBOptions((DBOptionsInterface<DBOptions>) targetOption);
        configDBOptions((MutableDBOptionsInterface<DBOptions>) targetOption);
        // we don't need atomic flush in both use cases
        targetOption.setAtomicFlush(false);
        return targetOption;
    }

    public ColumnFamilyOptions cfOptions(String name) {
        ColumnFamilyOptions targetOption = new ColumnFamilyOptions();
        configCFOptions(name, (ColumnFamilyOptionsInterface<ColumnFamilyOptions>) targetOption);
        configCFOptions(name, (MutableColumnFamilyOptionsInterface<ColumnFamilyOptions>) targetOption);
        return targetOption;
    }

    protected void configDBOptions(DBOptionsInterface<DBOptions> targetOption) {
        targetOption
            .setEnv(Env.getDefault())
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            .setAvoidUnnecessaryBlockingIO(true)
            .setMaxManifestFileSize(64 * SizeUnit.MB)
            // log file settings
            .setRecycleLogFileNum(4)
            .setMaxLogFileSize(128 * SizeUnit.MB)
            .setKeepLogFileNum(4)
            // wal file settings
            .setWalSizeLimitMB(0)
            .setWalTtlSeconds(0)
            .setEnablePipelinedWrite(true)
            .setTwoWriteQueues(false)
            .setRateLimiter(autoRelease(new RateLimiter(512 * SizeUnit.MB,
                RateLimiter.DEFAULT_REFILL_PERIOD_MICROS,
                RateLimiter.DEFAULT_FAIRNESS,
                RateLimiter.DEFAULT_MODE, true), targetOption));
    }

    protected void configDBOptions(MutableDBOptionsInterface<DBOptions> targetOption) {
        targetOption
            .setMaxOpenFiles(256)
            .setIncreaseParallelism(increaseParallelism)
            .setMaxBackgroundJobs(maxBackgroundJobs);
    }

    protected void configCFOptions(String name, ColumnFamilyOptionsInterface<ColumnFamilyOptions> targetOption) {
        targetOption
            .setMergeOperatorName("uint64add")
            .setTableFormatConfig(
                new BlockBasedTableConfig() //
                    // Begin to use partitioned index filters
                    // https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters#how-to-use-it
                    .setIndexType(IndexType.kTwoLevelIndexSearch) //
                    .setFilterPolicy(autoRelease(new BloomFilter(16, false), targetOption))
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
                    .setBlockCache(autoRelease(new LRUCache(blockCacheSize, 8), targetOption)))
            // https://github.com/facebook/rocksdb/pull/5744
            .setForceConsistencyChecks(true)
            .setCompactionStyle(CompactionStyle.LEVEL)
            .setPrepopulateBlobCache(PrepopulateBlobCache.PREPOPULATE_BLOB_FLUSH_ONLY);
    }

    protected void configCFOptions(String name, MutableColumnFamilyOptionsInterface<ColumnFamilyOptions> targetOption) {
        targetOption
            .setCompressionType(CompressionType.LZ4_COMPRESSION)
            .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
            // Flushing options:
            // write_buffer_size sets the size of a single mem_table. Once mem_table exceeds
            // this size, it is marked immutable and a new one is created.
            .setWriteBufferSize(writeBufferSize)
            // Flushing options:
            // max_write_buffer_number sets the maximum number of mem_tables, both active
            // and immutable.  If the active mem_table fills up and the total number of
            // mem_tables is larger than max_write_buffer_number we stall further writes.
            // This may happen if the flush process is slower than the write rate.
            .setMaxWriteBufferNumber(maxWriteBufferNumber)
            // Flushing options:
            // min_write_buffer_number_to_merge is the minimum number of mem_tables to be
            // merged before flushing to storage. For example, if this option is set to 2,
            // immutable mem_tables are only flushed when there are two of them - a single
            // immutable mem_table will never be flushed.  If multiple mem_tables are merged
            // together, less data may be written to storage since two updates are merged to
            // a single key. However, every Get() must traverse all immutable mem_tables
            // linearly to check if the key is there. Setting this option too high may hurt
            // read performance.
            .setMinWriteBufferNumberToMerge(minWriteBufferNumberToMerge)
            // Level Style Compaction:
            // level0_file_num_compaction_trigger -- Once level 0 reaches this number of
            // files, L0->L1 compaction is triggered. We can therefore estimate level 0
            // size in stable state as
            // write_buffer_size * min_write_buffer_number_to_merge * level0_file_num_compaction_trigger.
            .setLevel0FileNumCompactionTrigger(level0FileNumCompactionTrigger)
            // Level Style Compaction:
            // max_bytes_for_level_base and max_bytes_for_level_multiplier
            //  -- max_bytes_for_level_base is total size of level 1. As mentioned, we
            // recommend that this be around the size of level 0. Each subsequent level
            // is max_bytes_for_level_multiplier larger than previous one. The default
            // is 10 and we do not recommend changing that.
            .setMaxBytesForLevelBase(maxBytesForLevelBase)
            // Level Style Compaction:
            // target_file_size_base and target_file_size_multiplier
            //  -- Files in level 1 will have target_file_size_base bytes. Each next
            // level's file size will be target_file_size_multiplier bigger than previous
            // one. However, by default target_file_size_multiplier is 1, so files in all
            // L1..LMax levels are equal. Increasing target_file_size_base will reduce total
            // number of database files, which is generally a good thing. We recommend setting
            // target_file_size_base to be max_bytes_for_level_base / 10, so that there are
            // 10 files in level 1.
            .setTargetFileSizeBase(targetFileSizeBase)
            // If prefix_extractor is set and memtable_prefix_bloom_size_ratio is not 0,
            // create prefix bloom for memtable with the size of
            // write_buffer_size * memtable_prefix_bloom_size_ratio.
            // If it is larger than 0.25, it is santinized to 0.25.
            .setMemtablePrefixBloomSizeRatio(0.125)
            // Soft limit on number of level-0 files. We start slowing down writes at this
            // point. A value 0 means that no writing slow down will be triggered by number
            // of files in level-0.
            .setLevel0SlowdownWritesTrigger(level0SlowdownWritesTrigger)
            // Maximum number of level-0 files.  We stop writes at this point.
            .setLevel0StopWritesTrigger(level0StopWritesTrigger)
            .setLevelCompactionDynamicLevelBytes(false)
            // enable blob files
            .setEnableBlobFiles(true)
            .setMinBlobSize(minBlobSize())
            .enableBlobGarbageCollection();
    }

    public String dbRootDir() {
        return this.dbRootDir;
    }

    public boolean heuristicCompaction() {
        return this.heuristicCompaction;
    }

    public int compactMinTombstoneKeys() {
        return this.compactMinTombstoneKeys;
    }

    public int compactMinTombstoneRanges() {
        return this.compactMinTombstoneRanges;
    }

    public double compactTombstoneKeysRatio() {
        return this.compactTombstoneKeysRatio;
    }

    public T dbRootDir(String dbRootDir) {
        this.dbRootDir = dbRootDir;
        return (T) this;
    }

    public T heuristicCompaction(boolean heuristicCompaction) {
        this.heuristicCompaction = heuristicCompaction;
        return (T) this;
    }

    public T compactMinTombstoneKeys(int compactMinTombstoneKeys) {
        this.compactMinTombstoneKeys = compactMinTombstoneKeys;
        return (T) this;
    }

    public T compactMinTombstoneRanges(int compactMinTombstoneRanges) {
        this.compactMinTombstoneRanges = compactMinTombstoneRanges;
        return (T) this;
    }

    public T compactTombstoneKeysRatio(double compactTombstoneKeysRatio) {
        this.compactTombstoneKeysRatio = compactTombstoneKeysRatio;
        return (T) this;
    }
}
