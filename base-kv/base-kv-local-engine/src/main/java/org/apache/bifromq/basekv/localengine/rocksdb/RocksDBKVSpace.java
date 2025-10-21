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

import static org.apache.bifromq.basekv.localengine.metrics.KVSpaceMeters.getCounter;
import static org.apache.bifromq.basekv.localengine.metrics.KVSpaceMeters.getTimer;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBHelper.deleteDir;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBHelper.getMetadata;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.basekv.localengine.AbstractKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceRefreshableReader;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.localengine.SyncContext;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.localengine.rocksdb.metrics.RocksDBKVSpaceMetric;
import org.apache.bifromq.basekv.proto.Boundary;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;

abstract class RocksDBKVSpace<
    E extends RocksDBKVEngine<E, T, C, P>,
    T extends RocksDBKVSpace<E, T, C, P>,
    C extends RocksDBKVEngineConfigurator<C>,
    P extends RocksDBKVSpaceEpochHandle<C>
    > extends AbstractKVSpace<P> {

    protected final C configurator;
    protected final E engine;
    protected final ISyncContext syncContext;
    protected final IWriteStatsRecorder writeStats;
    private final File keySpaceDBDir;
    private final ExecutorService compactionExecutor;
    private final AtomicBoolean compacting;
    private final ISyncContext.IRefresher metadataRefresher;
    private SpaceMetrics spaceMetrics;
    private volatile long lastCompactAt;
    private volatile long nextCompactAt;

    @SneakyThrows
    public RocksDBKVSpace(String id,
                          C configurator,
                          E engine,
                          Runnable onDestroy,
                          KVSpaceOpMeters opMeters,
                          Logger logger,
                          String... tags) {
        super(id, onDestroy, opMeters, logger, tags);
        this.configurator = configurator;
        this.engine = engine;
        syncContext = new SyncContext();
        metadataRefresher = syncContext.refresher();
        compacting = new AtomicBoolean(false);
        compactionExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry, new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                EnvProvider.INSTANCE.newThreadFactory("kvspace-compactor-" + id)),
            "compactor", "kvspace", Tags.of(tags));
        this.writeStats = configurator.heuristicCompaction() ? new RocksDBKVSpaceCompactionTrigger(id,
            configurator.compactMinTombstoneKeys(),
            configurator.compactMinTombstoneRanges(),
            configurator.compactTombstoneKeysRatio(),
            this::scheduleCompact, tags) : NoopWriteStatsRecorder.INSTANCE;
        keySpaceDBDir = new File(configurator.dbRootDir(), id);
    }

    @Override
    protected void doOpen() {
        spaceMetrics = new SpaceMetrics(tags);
        reloadMetadata();
    }

    @Override
    public IKVSpaceRefreshableReader reader() {
        return new RocksDBKVSpaceReader(id, opMeters, logger, syncContext.refresher(), this::handle,
            this::currentMetadata);
    }

    // Load metadata from DB and publish, without refresher gating
    protected void reloadMetadata() {
        updateMetadata(getMetadata(handle()));
    }

    protected void publishMetadata(Map<ByteString, ByteString> metadataUpdates) {
        if (metadataUpdates.isEmpty()) {
            return;
        }
        metadataRefresher.runIfNeeded((genBumped) -> {
            Map<ByteString, ByteString> metaMap = Maps.newHashMap(currentMetadata());
            metaMap.putAll(metadataUpdates);
            updateMetadata(Collections.unmodifiableMap(metaMap));
        });
    }

    @Override
    protected void doClose() {
        logger.debug("Close key range[{}]", id);
        if (spaceMetrics != null) {
            spaceMetrics.close();
        }
    }

    @Override
    protected void doDestroy() {
        // Destroy the whole space root directory, including pointer file and all generations.
        try {
            if (keySpaceDBDir.exists()) {
                deleteDir(keySpaceDBDir.toPath());
            }
        } catch (IOException e) {
            logger.error("Failed to delete space root dir: {}", keySpaceDBDir, e);
        }
    }

    protected File spaceRootDir() {
        return keySpaceDBDir;
    }

    protected abstract P handle();

    protected abstract WriteOptions writeOptions();

    private void scheduleCompact() {
        if (state() != State.Opening) {
            return;
        }
        spaceMetrics.compactionSchedCounter.increment();
        if (compacting.compareAndSet(false, true)) {
            compactionExecutor.execute(spaceMetrics.compactionTimer.wrap(() -> {
                logger.debug("KeyRange[{}] compaction start", id);
                lastCompactAt = System.nanoTime();
                writeStats.reset();
                try (CompactRangeOptions options = new CompactRangeOptions()
                    .setBottommostLevelCompaction(CompactRangeOptions.BottommostLevelCompaction.kSkip)
                    .setExclusiveManualCompaction(false)) {
                    synchronized (compacting) {
                        if (state() == State.Opening) {
                            IRocksDBKVSpaceEpoch handle = handle();
                            handle.db().compactRange(handle.cf(), null, null, options);
                        }
                    }
                    logger.debug("KeyRange[{}] compacted", id);
                } catch (Throwable e) {
                    logger.error("KeyRange[{}] compaction error", id, e);
                } finally {
                    compacting.set(false);
                    if (nextCompactAt > lastCompactAt) {
                        scheduleCompact();
                    }
                }
            }));
        } else {
            nextCompactAt = System.nanoTime();
        }
    }

    @Override
    protected long doSize(Boundary boundary) {
        if (state() != State.Opening) {
            return 0;
        }
        return RocksDBHelper.sizeOfBoundary(handle(), boundary);
    }

    private class SpaceMetrics {
        private final Counter compactionSchedCounter;
        private final Timer compactionTimer;

        SpaceMetrics(Tags metricTags) {
            compactionSchedCounter = getCounter(id, RocksDBKVSpaceMetric.ManualCompactionCounter, metricTags);
            compactionTimer = getTimer(id, RocksDBKVSpaceMetric.ManualCompactionTimer, metricTags);
        }

        void close() {
            compactionSchedCounter.close();
            compactionTimer.close();
        }
    }
}
