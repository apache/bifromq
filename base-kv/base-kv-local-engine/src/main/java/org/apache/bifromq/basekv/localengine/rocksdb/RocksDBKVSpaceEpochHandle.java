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

import static org.apache.bifromq.basekv.localengine.metrics.KVSpaceMeters.getFunctionCounter;
import static org.apache.bifromq.basekv.localengine.metrics.KVSpaceMeters.getFunctionTimer;
import static org.apache.bifromq.basekv.localengine.metrics.KVSpaceMeters.getGauge;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBHelper.deleteDir;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBHelper.openDBInDir;

import com.google.protobuf.Struct;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.bifromq.basekv.localengine.rocksdb.metrics.RocksDBKVSpaceMetric;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.HistogramType;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.slf4j.Logger;

abstract class RocksDBKVSpaceEpochHandle implements IRocksDBKVSpaceEpochHandle {
    protected final Logger logger;
    final DBOptions dbOptions;
    final ColumnFamilyDescriptor cfDesc;
    final RocksDB db;
    final ColumnFamilyHandle cf;
    final File dir;
    final Checkpoint checkpoint;

    RocksDBKVSpaceEpochHandle(File dir, Struct conf, Logger logger) {
        this.dbOptions = buildDBOptions(conf);
        this.cfDesc = buildCFDescriptor(conf);
        RocksDBHelper.RocksDBHandle dbHandle = openDBInDir(dir, dbOptions, cfDesc);
        this.db = dbHandle.db();
        this.cf = dbHandle.cf();
        this.dir = dir;
        this.checkpoint = Checkpoint.create(db());
        this.logger = logger;
    }

    @Override
    public RocksDB db() {
        return db;
    }

    @Override
    public ColumnFamilyHandle cf() {
        return cf;
    }

    protected abstract DBOptions buildDBOptions(Struct conf);

    protected abstract ColumnFamilyDescriptor buildCFDescriptor(Struct conf);

    protected record ClosableResources(String id,
                                       String genId,
                                       DBOptions dbOptions,
                                       ColumnFamilyDescriptor cfDesc,
                                       ColumnFamilyHandle cfHandle,
                                       RocksDB db,
                                       Checkpoint checkpoint,
                                       File dir,
                                       Predicate<String> isRetired,
                                       SpaceMetrics metrics,
                                       Logger log) implements Runnable {
        @Override
        public void run() {
            // Ensure no metric suppliers call into RocksDB during close
            try (AutoCloseable guard = metrics.beginClose()) {
                metrics.close();
                log.debug("Clean up generation[{}] of kvspace[{}]", genId, id);
                // Close checkpoint before DB resources
                checkpoint.close();
                try {
                    db.destroyColumnFamilyHandle(cfHandle);
                } catch (Throwable e) {
                    log.error("Failed to destroy column family handle of generation[{}] for kvspace[{}]", genId, id, e);
                }
                try {
                    db.close();
                } catch (Throwable e) {
                    log.error("Failed to close RocksDB of generation[{}] for kvspace[{}]", genId, id, e);
                }
                cfDesc.getOptions().close();
                dbOptions.close();
                if (isRetired.test(genId)) {
                    log.debug("delete retired generation[{}] of kvspace[{}] in path: {}", genId, id,
                        dir.getAbsolutePath());
                    try {
                        deleteDir(dir.toPath());
                    } catch (Throwable e) {
                        log.error("Failed to clean retired generation at path:{}", dir, e);
                    }
                }
            } catch (Exception ignored) {
                // ignore
            }
        }
    }

    protected static class SpaceMetrics {
        private final ReentrantReadWriteLock rw = new ReentrantReadWriteLock();
        private final Logger logger;
        private final Gauge blockCacheSizeGauge;
        private final Gauge tableReaderSizeGauge;
        private final Gauge memTableSizeGauges;
        private final Gauge pinedMemorySizeGauge;
        private final Gauge totalSSTFileSizeGauge;
        private final Gauge liveSSTFileSizeGauge;
        private final Gauge liveDataSizeGauge;
        private final Gauge estimateNumKeysGauge;
        private final Gauge pendingCompactionBytesGauge;
        private final Gauge numRunningCompactionsGauge;
        private final Gauge numRunningFlushesGauge;
        private final Gauge compactionPendingGauge;
        private final Gauge memtableFlushPendingGauge;
        private final Gauge backgroundErrorsGauge;
        private final FunctionCounter bytesReadCounter;
        private final FunctionCounter bytesWrittenCounter;
        private final FunctionCounter blockCacheHitCounter;
        private final FunctionCounter blockCacheMissCounter;
        private final FunctionCounter blobCacheHitCounter;
        private final FunctionCounter blobCacheMissCounter;
        private final FunctionCounter bloomUsefulCounter;
        private final FunctionTimer getLatencyTimer;
        private final FunctionTimer writeLatencyTimer;
        private final FunctionTimer seekLatencyTimer;
        private final FunctionTimer blobGetLatencyTimer;
        private final FunctionTimer blobWriteLatencyTimer;
        private final FunctionTimer sstReadLatencyTimer;
        private final FunctionTimer sstWriteLatencyTimer;
        private final FunctionTimer flushLatencyTimer;
        private final FunctionTimer compactionLatencyTimer;
        private final FunctionTimer writeStallTimer;
        private final Statistics statistics;
        private volatile boolean closed = false;

        SpaceMetrics(String id,
                     RocksDB db,
                     DBOptions dbOptions,
                     ColumnFamilyHandle cfHandle,
                     ColumnFamilyOptions cfOptions,
                     Tags metricTags,
                     Logger logger) {
            this.logger = logger;
            this.statistics = dbOptions.statistics();
            blockCacheSizeGauge = getGauge(id, RocksDBKVSpaceMetric.BlockCache, () -> {
                BlockBasedTableConfig cfg = (BlockBasedTableConfig) cfOptions.tableFormatConfig();
                if (!cfg.noBlockCache()) {
                    return safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.block-cache-usage"));
                }
                return 0L;
            }, metricTags);
            tableReaderSizeGauge = getGauge(id, RocksDBKVSpaceMetric.TableReader,
                () -> safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.estimate-table-readers-mem")), metricTags);
            memTableSizeGauges = getGauge(id, RocksDBKVSpaceMetric.MemTable,
                () -> safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.cur-size-all-mem-tables")), metricTags);
            pinedMemorySizeGauge = getGauge(id, RocksDBKVSpaceMetric.PinnedMem, () -> {
                BlockBasedTableConfig cfg = (BlockBasedTableConfig) cfOptions.tableFormatConfig();
                if (!cfg.noBlockCache()) {
                    return safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.block-cache-pinned-usage"));
                }
                return 0L;
            }, metricTags);
            totalSSTFileSizeGauge = getGauge(id, RocksDBKVSpaceMetric.StateTotalSSTSize,
                () -> safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.total-sst-files-size")), metricTags);
            liveSSTFileSizeGauge = getGauge(id, RocksDBKVSpaceMetric.StateLiveSSTSize,
                () -> safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.live-sst-files-size")), metricTags);
            liveDataSizeGauge = getGauge(id, RocksDBKVSpaceMetric.StateLiveDataSize,
                () -> safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.estimate-live-data-size")), metricTags);
            estimateNumKeysGauge = getGauge(id, RocksDBKVSpaceMetric.StateEstimateNumKeys,
                () -> safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.estimate-num-keys")), metricTags);
            pendingCompactionBytesGauge = getGauge(id, RocksDBKVSpaceMetric.StatePendingCompactionBytes,
                () -> safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.estimate-pending-compaction-bytes")),
                metricTags);
            numRunningCompactionsGauge = getGauge(id, RocksDBKVSpaceMetric.StateRunningCompactions,
                () -> safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.num-running-compactions")), metricTags);
            numRunningFlushesGauge = getGauge(id, RocksDBKVSpaceMetric.StateRunningFlushes,
                () -> safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.num-running-flushes")), metricTags);
            compactionPendingGauge = getGauge(id, RocksDBKVSpaceMetric.StateCompactionPending,
                () -> safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.compaction-pending")), metricTags);
            memtableFlushPendingGauge = getGauge(id, RocksDBKVSpaceMetric.StateMemTableFlushPending,
                () -> safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.mem-table-flush-pending")), metricTags);
            backgroundErrorsGauge = getGauge(id, RocksDBKVSpaceMetric.StateBackgroundErrors,
                () -> safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.background-errors")), metricTags);

            // Statistics-based meters
            if (statistics != null) {
                bytesReadCounter = getFunctionCounter(id, RocksDBKVSpaceMetric.IOBytesReadCounter, statistics,
                    stats -> safeGetDouble(stats, (s) -> (double) s.getTickerCount(TickerType.BYTES_READ)),
                    metricTags);
                bytesWrittenCounter = getFunctionCounter(id, RocksDBKVSpaceMetric.IOBytesWrittenCounter, statistics,
                    stats -> safeGetDouble(stats, (s) -> (double) stats.getTickerCount(TickerType.BYTES_WRITTEN)),
                    metricTags);
                blockCacheHitCounter = getFunctionCounter(id, RocksDBKVSpaceMetric.BlockCacheHitCounter, statistics,
                    stats -> safeGetDouble(stats, (s) -> (double) stats.getTickerCount(TickerType.BLOCK_CACHE_HIT)),
                    metricTags);
                blockCacheMissCounter = getFunctionCounter(id, RocksDBKVSpaceMetric.BlockCacheMissCounter, statistics,
                    stats -> safeGetDouble(stats, (s) -> (double) stats.getTickerCount(TickerType.BLOCK_CACHE_MISS)),
                    metricTags);
                blobCacheHitCounter = getFunctionCounter(id, RocksDBKVSpaceMetric.BlobDBCacheHitCounter, statistics,
                    stats -> safeGetDouble(stats, (s) -> (double) stats.getTickerCount(TickerType.BLOB_DB_CACHE_HIT)),
                    metricTags);
                blobCacheMissCounter = getFunctionCounter(id, RocksDBKVSpaceMetric.BlobDBCacheMissCounter, statistics,
                    stats -> safeGetDouble(stats, (s) -> (double) stats.getTickerCount(TickerType.BLOB_DB_CACHE_MISS)),
                    metricTags);
                bloomUsefulCounter = getFunctionCounter(id, RocksDBKVSpaceMetric.BloomUsefulCounter, statistics,
                    stats -> safeGetDouble(stats, (s) -> (double) stats.getTickerCount(TickerType.BLOOM_FILTER_USEFUL)),
                    metricTags);

                getLatencyTimer = getFunctionTimer(id, RocksDBKVSpaceMetric.GetLatency, statistics,
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.DB_GET).getCount()),
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.DB_GET).getSum()),
                    TimeUnit.MICROSECONDS,
                    metricTags);
                writeLatencyTimer = getFunctionTimer(id, RocksDBKVSpaceMetric.WriteLatency, statistics,
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.DB_WRITE).getCount()),
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.DB_WRITE).getSum()),
                    TimeUnit.MICROSECONDS,
                    metricTags);
                seekLatencyTimer = getFunctionTimer(id, RocksDBKVSpaceMetric.SeekLatency, statistics,
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.DB_SEEK).getCount()),
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.DB_SEEK).getSum()),
                    TimeUnit.MICROSECONDS,
                    metricTags);
                blobGetLatencyTimer = getFunctionTimer(id, RocksDBKVSpaceMetric.GetLatency, statistics,
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.BLOB_DB_GET_MICROS).getCount()),
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.BLOB_DB_GET_MICROS).getSum()),
                    TimeUnit.MICROSECONDS,
                    metricTags);
                blobWriteLatencyTimer = getFunctionTimer(id, RocksDBKVSpaceMetric.WriteLatency, statistics,
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.BLOB_DB_WRITE_MICROS).getCount()),
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.BLOB_DB_WRITE_MICROS).getSum()),
                    TimeUnit.MICROSECONDS,
                    metricTags);
                sstReadLatencyTimer = getFunctionTimer(id, RocksDBKVSpaceMetric.SSTReadLatency, statistics,
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.SST_READ_MICROS).getCount()),
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.SST_READ_MICROS).getSum()),
                    TimeUnit.MICROSECONDS,
                    metricTags);
                sstWriteLatencyTimer = getFunctionTimer(id, RocksDBKVSpaceMetric.SSTWriteLatency, statistics,
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.SST_WRITE_MICROS).getCount()),
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.SST_WRITE_MICROS).getSum()),
                    TimeUnit.MICROSECONDS,
                    metricTags);
                flushLatencyTimer = getFunctionTimer(id, RocksDBKVSpaceMetric.FlushLatency, statistics,
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.FLUSH_TIME).getCount()),
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.FLUSH_TIME).getSum()),
                    TimeUnit.MICROSECONDS,
                    metricTags);
                compactionLatencyTimer = getFunctionTimer(id, RocksDBKVSpaceMetric.CompactionLatency, statistics,
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.COMPACTION_TIME).getCount()),
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.COMPACTION_TIME).getSum()),
                    TimeUnit.MICROSECONDS,
                    metricTags);
                writeStallTimer = getFunctionTimer(id, RocksDBKVSpaceMetric.WriteStall, statistics,
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.WRITE_STALL).getCount()),
                    stats -> safeGetLong(stats, s -> s.getHistogramData(HistogramType.WRITE_STALL).getSum()),
                    TimeUnit.MICROSECONDS,
                    metricTags);
            } else {
                bytesReadCounter = null;
                bytesWrittenCounter = null;
                blockCacheHitCounter = null;
                blockCacheMissCounter = null;
                blobCacheHitCounter = null;
                blobCacheMissCounter = null;
                bloomUsefulCounter = null;
                getLatencyTimer = null;
                writeLatencyTimer = null;
                seekLatencyTimer = null;
                blobGetLatencyTimer = null;
                blobWriteLatencyTimer = null;
                sstReadLatencyTimer = null;
                sstWriteLatencyTimer = null;
                flushLatencyTimer = null;
                compactionLatencyTimer = null;
                writeStallTimer = null;
            }
        }

        private <T> double safeGetDouble(T obj, Function<T, Double> func) {
            return safeGet(obj, func, 0d);
        }

        private <T> long safeGetLong(T obj, Function<T, Long> func) {
            return safeGet(obj, func, 0L);
        }

        private <T, R> R safeGet(T obj, Function<T, R> func, R defVal) {
            ReentrantReadWriteLock.ReadLock rl = rw.readLock();
            rl.lock();
            try {
                if (closed) {
                    return defVal;
                }
                return func.apply(obj);
            } catch (Throwable t) {
                logger.warn("Unable to read RocksDB metric", t);
                return defVal;
            } finally {
                rl.unlock();
            }
        }

        private long safeGet(RocksDBLongGetter action) {
            ReentrantReadWriteLock.ReadLock rl = rw.readLock();
            rl.lock();
            try {
                if (closed) {
                    return 0L;
                }
                return action.get();
            } catch (Throwable t) {
                logger.warn("Unable to read RocksDB metric", t);
                return 0L;
            } finally {
                rl.unlock();
            }
        }

        AutoCloseable beginClose() {
            ReentrantReadWriteLock.WriteLock wl = rw.writeLock();
            wl.lock();
            closed = true;
            return wl::unlock;
        }

        void close() {
            blockCacheSizeGauge.close();
            memTableSizeGauges.close();
            tableReaderSizeGauge.close();
            pinedMemorySizeGauge.close();
            totalSSTFileSizeGauge.close();
            liveSSTFileSizeGauge.close();
            liveDataSizeGauge.close();
            estimateNumKeysGauge.close();
            pendingCompactionBytesGauge.close();
            numRunningCompactionsGauge.close();
            numRunningFlushesGauge.close();
            compactionPendingGauge.close();
            memtableFlushPendingGauge.close();
            backgroundErrorsGauge.close();
            if (statistics != null) {
                bytesReadCounter.close();
                bytesWrittenCounter.close();
                blockCacheHitCounter.close();
                blockCacheMissCounter.close();
                blobCacheHitCounter.close();
                blobCacheMissCounter.close();
                bloomUsefulCounter.close();
                getLatencyTimer.close();
                writeLatencyTimer.close();
                seekLatencyTimer.close();
                blobGetLatencyTimer.close();
                blobWriteLatencyTimer.close();
                sstReadLatencyTimer.close();
                sstWriteLatencyTimer.close();
                flushLatencyTimer.close();
                compactionLatencyTimer.close();
                writeStallTimer.close();
                statistics.close();
            }
        }

        interface RocksDBLongGetter {
            long get() throws RocksDBException;
        }
    }
}
