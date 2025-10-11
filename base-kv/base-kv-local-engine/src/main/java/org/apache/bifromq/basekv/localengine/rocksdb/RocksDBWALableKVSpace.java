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
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBHelper.openDBInDir;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.io.File;
import java.nio.file.Files;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.basekv.localengine.IKVSpaceWriter;
import org.apache.bifromq.basekv.localengine.IWALableKVSpace;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceMeters;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.localengine.rocksdb.metrics.RocksDBKVSpaceMetric;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;

public class RocksDBWALableKVSpace
    extends RocksDBKVSpace<RocksDBWALableKVEngine, RocksDBWALableKVSpace, RocksDBWALableKVEngineConfigurator>
    implements IWALableKVSpace {
    private final RocksDBWALableKVEngineConfigurator configurator;
    private final WriteOptions writeOptions;
    private final AtomicReference<CompletableFuture<Long>> flushFutureRef = new AtomicReference<>();
    private final ExecutorService flushExecutor;
    private final MetricManager metricMgr;
    private WALableKVSpaceDBHandle handle;

    public RocksDBWALableKVSpace(String id,
                                 RocksDBWALableKVEngineConfigurator configurator,
                                 RocksDBWALableKVEngine engine,
                                 Runnable onDestroy,
                                 KVSpaceOpMeters opMeters,
                                 Logger logger,
                                 String... tags) {
        super(id, configurator, engine, onDestroy, opMeters, logger, tags);
        this.configurator = configurator;
        writeOptions = new WriteOptions().setDisableWAL(false);
        if (!configurator.asyncWALFlush()) {
            writeOptions.setSync(configurator.fsyncWAL());
        }
        flushExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry, new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                EnvProvider.INSTANCE.newThreadFactory("kvspace-flusher-" + id)), "flusher", "kvspace",
            Tags.of(tags));
        metricMgr = new MetricManager(tags);
    }

    @Override
    protected void doOpen() {
        try {
            // WALable uses space root as DB directory
            Files.createDirectories(spaceRootDir().getAbsoluteFile().toPath());
            handle = new WALableKVSpaceDBHandle(id, spaceRootDir(), configurator, logger, tags);
        } catch (Throwable e) {
            throw new KVEngineException("Failed to open WALable KVSpace", e);
        }
    }

    @Override
    protected IKVSpaceDBInstance handle() {
        return handle;
    }

    @Override
    protected void doClose() {
        final CompletableFuture<Long> flushTaskFuture = Optional.ofNullable(flushFutureRef.get()).orElseGet(() -> {
            CompletableFuture<Long> lastOne = new CompletableFuture<>();
            flushExecutor.submit(() -> lastOne.complete(System.nanoTime()));
            return lastOne;
        });
        flushExecutor.shutdown();
        try {
            flushTaskFuture.join();
        } catch (Throwable e) {
            logger.debug("Flush error during closing", e);
        }
        writeOptions.close();
        metricMgr.close();
        // close handle
        handle.close();
        super.doClose();
    }

    @Override
    protected WriteOptions writeOptions() {
        return writeOptions;
    }

    @Override
    public CompletableFuture<Long> flush() {
        if (state() != State.Opening) {
            return CompletableFuture.failedFuture(new KVEngineException("KVSpace not open"));
        }
        if (!configurator.asyncWALFlush()) {
            return CompletableFuture.completedFuture(System.nanoTime());
        }
        CompletableFuture<Long> flushFuture;
        if (flushFutureRef.compareAndSet(null, flushFuture = new CompletableFuture<>())) {
            doFlush(flushFuture);
        } else {
            flushFuture = flushFutureRef.get();
            if (flushFuture == null) {
                // try again
                return flush();
            }
        }
        return flushFuture;
    }

    @Override
    public IKVSpaceWriter toWriter() {
        return new RocksDBKVSpaceWriter<>(id, handle, engine, writeOptions(), syncContext,
            writeStats.newRecorder(), this::updateMetadata, opMeters, logger);
    }

    private void doFlush(CompletableFuture<Long> onDone) {
        flushExecutor.submit(() -> {
            long flashStartAt = System.nanoTime();
            try {
                logger.trace("KVSpace[{}] flush wal start", id);
                try {
                    Timer.Sample start = Timer.start();
                    handle().db().flushWal(configurator.fsyncWAL());
                    start.stop(metricMgr.flushTimer);
                    logger.trace("KVSpace[{}] flush complete", id);
                } catch (Throwable e) {
                    logger.error("KVSpace[{}] flush error", id, e);
                    throw new KVEngineException("KVSpace flush error", e);
                }
                flushFutureRef.compareAndSet(onDone, null);
                onDone.complete(flashStartAt);
            } catch (Throwable e) {
                flushFutureRef.compareAndSet(onDone, null);
                onDone.completeExceptionally(new KVEngineException("KVSpace flush error", e));
            }
        });
    }

    private static class DBKVSpaceDBHandle implements IKVSpaceDBHandle {
        private final Logger logger;
        private final DBOptions dbOptions;
        private final ColumnFamilyDescriptor cfDesc;
        private final RocksDB db;
        private final ColumnFamilyHandle cfHandle;

        private DBKVSpaceDBHandle(File dir, RocksDBWALableKVEngineConfigurator configurator, Logger logger) {
            this.dbOptions = configurator.dbOptions();
            this.cfDesc = new ColumnFamilyDescriptor(DEFAULT_NS.getBytes(), configurator.cfOptions(DEFAULT_NS));
            RocksDBHelper.RocksDBHandle dbHandle = openDBInDir(dir, dbOptions, cfDesc);
            this.db = dbHandle.db();
            this.cfHandle = dbHandle.cf();
            this.logger = logger;
        }

        @Override
        public RocksDB db() {
            return db;
        }

        @Override
        public ColumnFamilyHandle cf() {
            return cfHandle;
        }

        public void close() {
            try {
                if (cfHandle != null) {
                    db.destroyColumnFamilyHandle(cfHandle);
                }
            } catch (Throwable t) {
                logger.warn("Failed to close cf handle", t);
            }
            try {
                if (db != null) {
                    db.close();
                }
            } catch (Throwable t) {
                logger.warn("Failed to close db", t);
            }
            cfDesc.getOptions().close();
            dbOptions.close();
        }
    }

    private class MetricManager {
        private final Timer flushTimer;

        MetricManager(String... metricTags) {
            flushTimer = KVSpaceMeters.getTimer(id, RocksDBKVSpaceMetric.FlushTimer, Tags.of(metricTags));
        }

        void close() {
            flushTimer.close();
        }
    }
}
