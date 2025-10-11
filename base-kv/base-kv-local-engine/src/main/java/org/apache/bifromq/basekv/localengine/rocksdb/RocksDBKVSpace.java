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

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static io.reactivex.rxjava3.subjects.BehaviorSubject.createDefault;
import static java.util.Collections.emptyMap;
import static org.apache.bifromq.basekv.localengine.metrics.KVSpaceMeters.getCounter;
import static org.apache.bifromq.basekv.localengine.metrics.KVSpaceMeters.getTimer;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.META_SECTION_END;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.META_SECTION_START;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.fromMetaKey;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBHelper.deleteDir;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.basekv.localengine.IKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.localengine.KVSpaceDescriptor;
import org.apache.bifromq.basekv.localengine.SyncContext;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.localengine.rocksdb.metrics.RocksDBKVSpaceMetric;
import org.apache.bifromq.basekv.proto.Boundary;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;

abstract class RocksDBKVSpace<
    E extends RocksDBKVEngine<E, T, C>,
    T extends RocksDBKVSpace<E, T, C>,
    C extends RocksDBKVEngineConfigurator<C>
    >
    extends RocksDBKVSpaceReader implements IKVSpace {

    protected final C configurator;
    protected final E engine;
    protected final ISyncContext syncContext = new SyncContext();
    protected final IWriteStatsRecorder writeStats;
    protected final Tags tags;
    private final AtomicReference<State> state = new AtomicReference<>(State.Init);
    private final File keySpaceDBDir;
    private final ExecutorService compactionExecutor;
    private final Runnable onDestroy;
    private final AtomicBoolean compacting = new AtomicBoolean(false);
    private final BehaviorSubject<Map<ByteString, ByteString>> metadataSubject = createDefault(emptyMap());
    private final ISyncContext.IRefresher metadataRefresher = syncContext.refresher();
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
        super(id, opMeters, logger);
        this.configurator = configurator;
        this.onDestroy = onDestroy;
        this.engine = engine;
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
        this.tags = Tags.of(tags).and("spaceId", id);
    }

    public void open() {
        if (state.compareAndSet(State.Init, State.Opening)) {
            doOpen();
            spaceMetrics = new SpaceMetrics(tags);
            reloadMetadata();
        }
    }

    protected abstract void doOpen();

    public Observable<Map<ByteString, ByteString>> metadata() {
        return metadataSubject;
    }

    protected abstract WriteOptions writeOptions();

    protected Optional<ByteString> doMetadata(ByteString metaKey) {
        return metadataRefresher.call(() -> {
            Map<ByteString, ByteString> metaMap = metadataSubject.getValue();
            return Optional.ofNullable(metaMap.get(metaKey));
        });
    }

    @Override
    public KVSpaceDescriptor describe() {
        return new KVSpaceDescriptor(id, collectStats());
    }

    private Map<String, Double> collectStats() {
        Map<String, Double> stats = new HashMap<>();
        stats.put("size", (double) size());
        // TODO: more stats
        return stats;
    }

    // Load metadata from DB and publish, without refresher gating
    protected void reloadMetadata() {
        try (RocksDBKVEngineIterator metaItr =
                 new RocksDBKVEngineIterator(handle(), null, META_SECTION_START, META_SECTION_END)) {
            Map<ByteString, ByteString> metaMap = new HashMap<>();
            for (metaItr.seekToFirst(); metaItr.isValid(); metaItr.next()) {
                metaMap.put(fromMetaKey(metaItr.key()), unsafeWrap(metaItr.value()));
            }
            metadataSubject.onNext(Collections.unmodifiableMap(metaMap));
        }
    }

    protected void updateMetadata(Map<ByteString, ByteString> metadataUpdates) {
        if (metadataUpdates.isEmpty()) {
            return;
        }
        metadataRefresher.runIfNeeded((genBumped) -> {
            Map<ByteString, ByteString> metaMap = Maps.newHashMap(metadataSubject.getValue());
            metaMap.putAll(metadataUpdates);
            metadataSubject.onNext(Collections.unmodifiableMap(metaMap));
        });
    }

    @Override
    public void destroy() {
        if (state.compareAndSet(State.Opening, State.Destroying)) {
            try {
                doDestroy();
            } catch (Throwable e) {
                throw new KVEngineException("Destroy KVRange error", e);
            } finally {
                onDestroy.run();
                state.set(State.Terminated);
            }
        }
    }

    public void close() {
        if (state.compareAndSet(State.Opening, State.Closing)) {
            try {
                doClose();
            } finally {
                state.set(State.Terminated);
            }
        }
    }

    protected State state() {
        return state.get();
    }

    protected void doClose() {
        logger.debug("Close key range[{}]", id);
        if (spaceMetrics != null) {
            spaceMetrics.close();
        }
        metadataSubject.onComplete();
    }

    protected void doDestroy() {
        doClose();
        // Destroy the whole space root directory, including pointer file and all generations.
        try {
            if (keySpaceDBDir.exists()) {
                deleteDir(keySpaceDBDir.toPath());
            }
        } catch (IOException e) {
            logger.error("Failed to delete space root dir: {}", keySpaceDBDir, e);
        }
    }

    @Override
    protected ISyncContext.IRefresher newRefresher() {
        return syncContext.refresher();
    }

    @Override
    protected IKVSpaceIterator doNewIterator() {
        return new RocksDBKVSpaceIterator(this::handle, Boundary.getDefaultInstance(), newRefresher());
    }

    @Override
    protected IKVSpaceIterator doNewIterator(Boundary subBoundary) {
        return new RocksDBKVSpaceIterator(this::handle, subBoundary, newRefresher());
    }

    protected File spaceRootDir() {
        return keySpaceDBDir;
    }

    private void scheduleCompact() {
        if (state.get() != State.Opening) {
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
                        if (state.get() == State.Opening) {
                            IKVSpaceDBInstance handle = handle();
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

    protected enum State {
        Init, Opening, Destroying, Closing, Terminated
    }

    private class SpaceMetrics {
        private final Counter compactionSchedCounter;
        private final Timer compactionTimer;

        SpaceMetrics(Tags metricTags) {
            compactionSchedCounter = getCounter(id, RocksDBKVSpaceMetric.CompactionCounter, metricTags);
            compactionTimer = getTimer(id, RocksDBKVSpaceMetric.CompactionTimer, metricTags);
        }

        void close() {
            compactionSchedCounter.close();
            compactionTimer.close();
        }
    }
}
