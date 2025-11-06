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
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.ASYNC_WAL_FLUSH;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.FSYNC_WAL;

import com.google.protobuf.Struct;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.nio.file.Files;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.basekv.localengine.IKVSpaceRefreshableReader;
import org.apache.bifromq.basekv.localengine.IKVSpaceWriter;
import org.apache.bifromq.basekv.localengine.IWALableKVSpace;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceMeters;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.localengine.rocksdb.metrics.RocksDBKVSpaceMetric;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;

class RocksDBWALableKVSpace extends RocksDBKVSpace implements IWALableKVSpace {
    private final WriteOptions writeOptions;
    private final AtomicReference<CompletableFuture<Long>> flushFutureRef = new AtomicReference<>();
    private final ExecutorService flushExecutor;
    private final MetricManager metricMgr;
    private RocksDBWALableKVSpaceEpochHandle handle;

    RocksDBWALableKVSpace(String id,
                          Struct conf,
                          RocksDBWALableKVEngine engine,
                          Runnable onDestroy,
                          KVSpaceOpMeters opMeters,
                          Logger logger,
                          String... tags) {
        super(id, conf, engine, onDestroy, opMeters, logger, tags);
        writeOptions = new WriteOptions().setDisableWAL(false);
        if (isSyncWALFlush()) {
            writeOptions.setSync(isFsyncWAL());
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
            handle = new RocksDBWALableKVSpaceEpochHandle(id, spaceRootDir(), this.conf, logger, tags);
            super.doOpen();
        } catch (Throwable e) {
            throw new KVEngineException("Failed to open WALable KVSpace", e);
        }
    }

    @Override
    protected RocksDBWALableKVSpaceEpochHandle handle() {
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
        if (isSyncWALFlush()) {
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
        return new RocksDBKVSpaceWriter(id, handle, engine, writeOptions(), syncContext,
            writeStats.newRecorder(), this::publishMetadata, opMeters, logger);
    }

    private void doFlush(CompletableFuture<Long> onDone) {
        flushExecutor.submit(() -> {
            long flashStartAt = System.nanoTime();
            try {
                logger.trace("KVSpace[{}] flush wal start", id);
                try {
                    Timer.Sample start = Timer.start();
                    handle().db().flushWal(isFsyncWAL());
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

    private boolean isSyncWALFlush() {
        return !boolVal(conf, ASYNC_WAL_FLUSH);
    }

    private boolean isFsyncWAL() {
        return boolVal(conf, FSYNC_WAL);
    }

    @Override
    public IKVSpaceRefreshableReader reader() {
        return new RocksDBKVSpaceReader(id, opMeters, logger, syncContext.refresher(), this::handle,
            this::currentMetadata, new IteratorOptions(false, 524288));
    }

    private class MetricManager {
        private final Timer flushTimer;

        MetricManager(String... metricTags) {
            flushTimer = KVSpaceMeters.getTimer(id, RocksDBKVSpaceMetric.ManualFlushTimer, Tags.of(metricTags));
        }

        void close() {
            flushTimer.close();
        }
    }
}
