/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.basekv.localengine.rocksdb;

import static org.apache.bifromq.basekv.localengine.metrics.KVSpaceMeters.getGauge;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.LATEST_CP_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceCheckpoint;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.localengine.metrics.GeneralKVSpaceMetric;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceMeters;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.localengine.rocksdb.metrics.RocksDBKVSpaceMetric;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.rocksdb.Checkpoint;
import org.rocksdb.FlushOptions;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;

public class RocksDBCPableKVSpace
    extends RocksDBKVSpace<RocksDBCPableKVEngine, RocksDBCPableKVSpace, RocksDBCPableKVEngineConfigurator>
    implements ICPableKVSpace {
    private static final String CP_SUFFIX = ".cp";
    private final RocksDBCPableKVEngine engine;
    private final File cpRootDir;
    private final WriteOptions writeOptions;
    private final Checkpoint checkpoint;
    private final AtomicReference<String> latestCheckpointId = new AtomicReference<>();
    private final Cache<String, IRocksDBKVSpaceCheckpoint> checkpoints;
    private final MetricManager metricMgr;
    // keep a strong ref to latest checkpoint
    private IKVSpaceCheckpoint latestCheckpoint;

    @SneakyThrows
    public RocksDBCPableKVSpace(String id,
                                RocksDBCPableKVEngineConfigurator configurator,
                                RocksDBCPableKVEngine engine,
                                Runnable onDestroy,
                                KVSpaceOpMeters opMeters,
                                Logger logger,
                                String... tags) {
        super(id, configurator, engine, onDestroy, opMeters, logger, tags);
        this.engine = engine;
        cpRootDir = new File(configurator.dbCheckpointRootDir(), id);
        this.checkpoint = Checkpoint.create(db);
        checkpoints = Caffeine.newBuilder().weakValues().build();
        writeOptions = new WriteOptions().setDisableWAL(true);
        Files.createDirectories(cpRootDir.getAbsoluteFile().toPath());
        metricMgr = new MetricManager(tags);
    }

    @Override
    protected WriteOptions writeOptions() {
        return writeOptions;
    }

    @Override
    public String checkpoint() {
        return metricMgr.checkpointTimer.record(() -> {
            synchronized (this) {
                IRocksDBKVSpaceCheckpoint cp = doCheckpoint();
                checkpoints.put(cp.cpId(), cp);
                latestCheckpoint = cp;
                return cp.cpId();
            }
        });
    }

    @Override
    public Optional<IKVSpaceCheckpoint> openCheckpoint(String checkpointId) {
        return Optional.ofNullable(checkpoints.getIfPresent(checkpointId));
    }

    @Override
    protected void doClose() {
        logger.debug("Flush RocksDBCPableKVSpace[{}] before closing", id);
        try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            db.flush(flushOptions);
        } catch (Throwable e) {
            logger.error("Flush RocksDBCPableKVSpace[{}] error", id, e);
        }
        metricMgr.close();
        checkpoints.asMap().forEach((cpId, cp) -> cp.close());
        checkpoint.close();
        writeOptions.close();
        super.doClose();
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
        try {
            deleteDir(cpRootDir.toPath());
        } catch (IOException e) {
            logger.error("Failed to delete checkpoint root dir: {}", cpRootDir, e);
        }
    }

    @Override
    protected void doLoad() {
        loadLatestCheckpoint();
        super.doLoad();
    }

    private IRocksDBKVSpaceCheckpoint doCheckpoint() {
        String cpId = genCheckpointId();
        File cpDir = Paths.get(cpRootDir.getAbsolutePath(), cpId).toFile();
        try {
            logger.debug("KVSpace[{}] checkpoint start: checkpointId={}", id, cpId);
            db.put(cfHandle, LATEST_CP_KEY, cpId.getBytes());
            checkpoint.createCheckpoint(cpDir.toString());
            latestCheckpointId.set(cpId);
            return new RocksDBKVSpaceCheckpoint(id, cpId, cpDir, this::isLatest, opMeters, logger);
        } catch (Throwable e) {
            throw new KVEngineException("Checkpoint key range error", e);
        }
    }

    @SneakyThrows
    private IRocksDBKVSpaceCheckpoint doLoadLatestCheckpoint() {
        byte[] cpIdBytes = db.get(cfHandle, LATEST_CP_KEY);
        if (cpIdBytes != null) {
            try {
                String cpId = new String(cpIdBytes, UTF_8);
                File cpDir = Paths.get(cpRootDir.getAbsolutePath(), cpId).toFile();
                // cleanup obsolete checkpoints
                for (String obsoleteId : obsoleteCheckpoints(cpId)) {
                    try {
                        cleanCheckpoint(obsoleteId);
                    } catch (Throwable e) {
                        logger.error("Clean checkpoint[{}] for kvspace[{}] error", obsoleteId, id, e);
                    }
                }
                logger.debug("Load latest checkpoint[{}] of kvspace[{}] in engine[{}] at path[{}]",
                    cpId, id, engine.id(), cpDir);
                latestCheckpointId.set(cpId);
                return new RocksDBKVSpaceCheckpoint(id, cpId, cpDir, this::isLatest, opMeters, logger);
            } catch (Throwable e) {
                logger.warn("Failed to load latest checkpoint, checkpoint now", e);
            }
        }
        return doCheckpoint();
    }

    @SneakyThrows
    private void loadLatestCheckpoint() {
        IRocksDBKVSpaceCheckpoint checkpoint = doLoadLatestCheckpoint();
        assert !checkpoints.asMap().containsKey(checkpoint.cpId());
        checkpoints.put(checkpoint.cpId(), checkpoint);
        latestCheckpoint = checkpoint;
    }

    private String genCheckpointId() {
        // we need generate global unique checkpoint id, since it will be used in raft snapshot
        return UUID.randomUUID() + CP_SUFFIX;
    }

    private boolean isLatest(String cpId) {
        return cpId.equals(latestCheckpointId.get());
    }

    private File checkpointDir(String cpId) {
        return Paths.get(cpRootDir.getAbsolutePath(), cpId).toFile();
    }

    private Iterable<String> obsoleteCheckpoints(String skipId) {
        File[] cpDirList = cpRootDir.listFiles();
        if (cpDirList == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(cpDirList)
            .filter(File::isDirectory)
            .map(File::getName)
            .filter(cpId -> !skipId.equals(cpId))
            .collect(Collectors.toList());
    }

    private void cleanCheckpoint(String cpId) {
        logger.debug("Delete checkpoint[{}] of kvspace[{}]", cpId, id);
        try {
            deleteDir(checkpointDir(cpId).toPath());
        } catch (IOException e) {
            logger.error("Failed to clean checkpoint[{}] for kvspace[{}] at path:{}", cpId, id, checkpointDir(cpId));
        }
    }

    private class MetricManager {
        private final Gauge checkpointGauge; // hold a strong reference
        private final Timer checkpointTimer;

        MetricManager(String... metricTags) {
            Tags tags = Tags.of(metricTags);
            checkpointGauge = getGauge(id, GeneralKVSpaceMetric.CheckpointNumGauge, checkpoints::estimatedSize, tags);
            checkpointTimer = KVSpaceMeters.getTimer(id, RocksDBKVSpaceMetric.CheckpointTimer, tags);
        }

        void close() {
            checkpointGauge.close();
            checkpointTimer.close();
        }
    }
}
