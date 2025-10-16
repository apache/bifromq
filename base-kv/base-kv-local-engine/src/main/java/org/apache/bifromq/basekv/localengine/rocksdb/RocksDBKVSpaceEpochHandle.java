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

import static org.apache.bifromq.basekv.localengine.IKVEngine.DEFAULT_NS;
import static org.apache.bifromq.basekv.localengine.metrics.KVSpaceMeters.getGauge;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBHelper.deleteDir;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBHelper.openDBInDir;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import org.apache.bifromq.basekv.localengine.rocksdb.metrics.RocksDBKVSpaceMetric;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

abstract class RocksDBKVSpaceEpochHandle<C extends RocksDBKVEngineConfigurator<C>> implements
    IRocksDBKVSpaceEpochHandle {
    protected final Logger logger;
    final DBOptions dbOptions;
    final ColumnFamilyDescriptor cfDesc;
    final RocksDB db;
    final ColumnFamilyHandle cf;
    final File dir;
    final Checkpoint checkpoint;

    RocksDBKVSpaceEpochHandle(File dir, C configurator, Logger logger) {
        this.dbOptions = configurator.dbOptions();
        this.cfDesc = new ColumnFamilyDescriptor(DEFAULT_NS.getBytes(), configurator.cfOptions(DEFAULT_NS));
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
                checkpoint.close();
                cfDesc.getOptions().close();
                dbOptions.close();
                if (isRetired.test(genId)) {
                    log.debug("delete retired generation[{}] of kvspace[{}] in path: {}", genId, id,
                        dir.getAbsolutePath());
                    try {
                        deleteDir(dir.toPath());
                    } catch (IOException e) {
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
        private final Gauge blockCacheSizeGauge;
        private final Gauge tableReaderSizeGauge;
        private final Gauge memtableSizeGauges;
        private final Gauge pinedMemorySizeGauge;
        private final Logger logger;
        private volatile boolean closed = false;

        SpaceMetrics(String id,
                     RocksDB db,
                     ColumnFamilyHandle cfHandle,
                     ColumnFamilyOptions cfOptions,
                     Tags metricTags,
                     Logger logger) {
            this.logger = logger;
            blockCacheSizeGauge = getGauge(id, RocksDBKVSpaceMetric.BlockCache, () -> {
                BlockBasedTableConfig cfg = (BlockBasedTableConfig) cfOptions.tableFormatConfig();
                if (!cfg.noBlockCache()) {
                    return safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.block-cache-usage"));
                }
                return 0L;
            }, metricTags);
            tableReaderSizeGauge = getGauge(id, RocksDBKVSpaceMetric.TableReader, () ->
                safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.estimate-table-readers-mem")), metricTags);
            memtableSizeGauges = getGauge(id, RocksDBKVSpaceMetric.MemTable, () ->
                safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.cur-size-all-mem-tables")), metricTags);
            pinedMemorySizeGauge = getGauge(id, RocksDBKVSpaceMetric.PinnedMem, () -> {
                BlockBasedTableConfig cfg = (BlockBasedTableConfig) cfOptions.tableFormatConfig();
                if (!cfg.noBlockCache()) {
                    return safeGet(() -> db.getLongProperty(cfHandle, "rocksdb.block-cache-pinned-usage"));
                }
                return 0L;
            }, metricTags);
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
            memtableSizeGauges.close();
            tableReaderSizeGauge.close();
            pinedMemorySizeGauge.close();
        }

        interface RocksDBLongGetter {
            long get() throws RocksDBException;
        }
    }
}
