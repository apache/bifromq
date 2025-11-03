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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bifromq.basekv.localengine.StructUtil.strVal;
import static org.apache.bifromq.basekv.localengine.metrics.KVSpaceMeters.getGauge;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.LATEST_CP_KEY;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_CHECKPOINT_ROOT_DIR;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBHelper.deleteDir;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceCheckpoint;
import org.apache.bifromq.basekv.localengine.IKVSpaceMigratableWriter;
import org.apache.bifromq.basekv.localengine.IKVSpaceRefreshableReader;
import org.apache.bifromq.basekv.localengine.IRestoreSession;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.localengine.RestoreMode;
import org.apache.bifromq.basekv.localengine.metrics.GeneralKVSpaceMetric;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceMeters;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.localengine.rocksdb.metrics.RocksDBKVSpaceMetric;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;

class RocksDBCPableKVSpace extends RocksDBKVSpace implements ICPableKVSpace {
    public static final String ACTIVE_GEN_POINTER = "ACTIVE";
    private static final String CP_SUFFIX = ".cp";
    private final RocksDBCPableKVEngine engine;
    private final File cpRootDir;
    private final WriteOptions writeOptions;
    private final AtomicReference<String> latestCheckpointId = new AtomicReference<>();
    private final Cache<String, IRocksDBKVSpaceCheckpoint> checkpoints;
    private final MetricManager metricMgr;
    private final AtomicReference<RocksDBCPableKVSpaceEpochHandle> active = new AtomicReference<>();
    private File currentDBDir;
    // keep a strong ref to latest checkpoint
    private IKVSpaceCheckpoint latestCheckpoint;

    @SneakyThrows
    RocksDBCPableKVSpace(String id,
                         Struct conf,
                         RocksDBCPableKVEngine engine,
                         Runnable onDestroy,
                         KVSpaceOpMeters opMeters,
                         Logger logger,
                         String... tags) {
        super(id, conf, engine, onDestroy, opMeters, logger, tags);
        this.engine = engine;
        cpRootDir = new File(strVal(conf, DB_CHECKPOINT_ROOT_DIR), id);
        checkpoints = Caffeine.newBuilder().weakValues().build();
        writeOptions = new WriteOptions().setDisableWAL(true);
        Files.createDirectories(cpRootDir.getAbsoluteFile().toPath());
        metricMgr = new MetricManager(tags);
        // Ensure space root exists and initialize current generation directory.
        // The legacy layout migration is done at constructor time to keep open() path clean.
        Files.createDirectories(spaceRootDir().getAbsoluteFile().toPath());
        initOrMigrateCurrentDBDir();
    }

    @Override
    protected WriteOptions writeOptions() {
        return writeOptions;
    }

    @Override
    protected RocksDBCPableKVSpaceEpochHandle handle() {
        return active.get();
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
        IRocksDBKVSpaceCheckpoint cp = checkpoints.getIfPresent(checkpointId);
        return Optional.ofNullable(cp);
    }

    @Override
    public IRestoreSession startRestore(IRestoreSession.FlushListener flushListener) {
        return new RestoreSession(RestoreMode.Replace, flushListener, logger);
    }

    @Override
    public IRestoreSession startReceiving(IRestoreSession.FlushListener flushListener) {
        return new RestoreSession(RestoreMode.Overlay, flushListener, logger);
    }

    @Override
    public IKVSpaceMigratableWriter toWriter() {
        return new RocksDBKVSpaceMigratableWriter(id, active.get(), engine, writeOptions(), syncContext,
            writeStats.newRecorder(), this::publishMetadata, opMeters, logger);
    }

    @Override
    protected void doClose() {
        logger.debug("Flush RocksDBCPableKVSpace[{}] before closing", id);
        try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            active.get().db.flush(flushOptions);
        } catch (Throwable e) {
            logger.error("Flush RocksDBCPableKVSpace[{}] error", id, e);
        }
        metricMgr.close();
        checkpoints.asMap().forEach((cpId, cp) -> cp.close());
        writeOptions.close();
        RocksDBCPableKVSpaceEpochHandle h = active.get();
        if (h != null) {
            h.close();
        }
        super.doClose();
    }

    @Override
    protected void doDestroy() {
        try {
            deleteDir(cpRootDir.toPath());
        } catch (IOException e) {
            logger.error("Failed to delete checkpoint root dir: {}", cpRootDir, e);
        } finally {
            super.doDestroy();
        }
    }

    @Override
    protected void doOpen() {
        try {
            // Use currentDBDir initialized during construction
            this.active.set(newEpochHandle(currentDBDir));
            // cleanup inactive generations at startup
            cleanInactiveOnStartup();
            loadLatestCheckpoint();
            super.doOpen();
        } catch (Throwable e) {
            throw new KVEngineException("Failed to open CPable KVSpace", e);
        }
    }

    private RocksDBCPableKVSpaceEpochHandle newEpochHandle(File dir) {
        return new RocksDBCPableKVSpaceEpochHandle(id, dir, this.conf, this::isRetired, logger, tags);
    }

    private IRocksDBKVSpaceCheckpoint doCheckpoint() {
        String cpId = genCheckpointId();
        File cpDir = Paths.get(cpRootDir.getAbsolutePath(), cpId).toFile();
        try {
            logger.debug("KVSpace[{}] checkpoint start: checkpointId={}", id, cpId);
            RocksDBCPableKVSpaceEpochHandle currentHandle = active.get();
            currentHandle.db.put(currentHandle.cf, LATEST_CP_KEY, cpId.getBytes());
            currentHandle.checkpoint.createCheckpoint(cpDir.toString());
            latestCheckpointId.set(cpId);
            return new RocksDBKVSpaceCheckpoint(id, cpId, cpDir, this::isLatest, opMeters, logger);
        } catch (Throwable e) {
            throw new KVEngineException("Checkpoint key range error", e);
        }
    }

    @SneakyThrows
    private IRocksDBKVSpaceCheckpoint doLoadLatestCheckpoint() {
        RocksDBCPableKVSpaceEpochHandle currentHandle = active.get();
        byte[] cpIdBytes = currentHandle.db.get(currentHandle.cf, LATEST_CP_KEY);
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

    private boolean isRetired(String genId) {
        // A generation is retired if it is not the one pointed by ACTIVE_GEN_POINTER
        File pointer = new File(spaceRootDir(), ACTIVE_GEN_POINTER);
        try {
            if (!pointer.exists()) {
                // No pointer means no active generation defined; treat all as retired.
                return true;
            }
            String activeUuid = Files.readString(pointer.toPath()).trim();
            return !genId.equals(activeUuid);
        } catch (Throwable ignore) {
            // On any error, be conservative and treat as retired to avoid leak
            return true;
        }
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

    private void switchTo(RocksDBCPableKVSpaceEpochHandle handle) {
        syncContext.mutator().run(() -> {
            // inactive handle will be closed and cleaned up by cleaner
            active.set(handle);
            updateCurrentDBDir(handle.dir);
            reloadMetadata();
            return true;
        });
    }

    @SneakyThrows
    private void initOrMigrateCurrentDBDir() {
        File spaceRoot = spaceRootDir();
        File pointer = new File(spaceRoot, ACTIVE_GEN_POINTER);
        // If pointer exists, honor it; otherwise detect legacy layout and migrate.
        if (pointer.exists()) {
            try {
                String uuid = Files.readString(pointer.toPath()).trim();
                if (!uuid.isEmpty()) {
                    File dir = new File(spaceRoot, uuid);
                    if (dir.exists() && dir.isDirectory()) {
                        currentDBDir = dir;
                        return;
                    }
                }
            } catch (Throwable t) {
                // fall through to create a new generation
                logger.warn("Failed to read {} for {}, create new generation", ACTIVE_GEN_POINTER, id, t);
            }
            // pointer invalid or target missing -> create a new generation
            File newGen = new File(spaceRoot, UUID.randomUUID().toString());
            Files.createDirectories(newGen.toPath());
            updateCurrentDBDir(newGen);
            return;
        }

        // No pointer file, check if space root is empty: empty means fresh deploy; non-empty means legacy layout.
        String[] children = spaceRoot.list();
        if (children == null || children.length == 0) {
            // Fresh deployment, bootstrap new generation
            File newGen = new File(spaceRoot, UUID.randomUUID().toString());
            Files.createDirectories(newGen.toPath());
            updateCurrentDBDir(newGen);
            return;
        }

        // Legacy layout: move all existing contents under a new generation directory, then write pointer file.
        File newGen = new File(spaceRoot, UUID.randomUUID().toString());
        Files.createDirectories(newGen.toPath());
        File[] entries = spaceRoot.listFiles();
        if (entries != null) {
            for (File entry : entries) {
                if (entry.getName().equals(ACTIVE_GEN_POINTER)) {
                    continue;
                }
                try {
                    Path target = new File(newGen, entry.getName()).toPath();
                    Files.move(entry.toPath(), target);
                } catch (Throwable moveEx) {
                    logger.warn("Failed to move legacy entry {} for space[{}] by Files.move, fallback to rename",
                        entry.getAbsolutePath(), id, moveEx);
                    // Fallback to rename if Files.move fails for any reason
                    boolean renamed = entry.renameTo(new File(newGen, entry.getName()));
                    if (!renamed) {
                        logger.warn("Failed to move legacy entry {} for space[{}]", entry.getAbsolutePath(), id);
                    }
                }
            }
        }
        updateCurrentDBDir(newGen);
    }

    private void updateCurrentDBDir(File newDir) {
        this.currentDBDir = newDir;
        File pointer = new File(spaceRootDir(), ACTIVE_GEN_POINTER);
        try {
            Files.writeString(pointer.toPath(), newDir.getName());
        } catch (Throwable t) {
            logger.warn("Failed to update {} pointer for {}", ACTIVE_GEN_POINTER, id, t);
        }
    }

    @SneakyThrows
    private void cleanInactiveOnStartup() {
        File pointer = new File(spaceRootDir(), ACTIVE_GEN_POINTER);
        if (!pointer.exists()) {
            return;
        }
        String activeUuid;
        try {
            activeUuid = Files.readString(pointer.toPath()).trim();
        } catch (Throwable t) {
            return;
        }
        if (activeUuid.isEmpty()) {
            return;
        }
        File root = spaceRootDir();
        File[] children = root.listFiles(File::isDirectory);
        if (children != null) {
            for (File c : children) {
                if (!c.getName().equals(activeUuid)) {
                    deleteDir(c.toPath());
                }
            }
        }
        File[] files = root.listFiles(File::isFile);
        if (files != null) {
            for (File f : files) {
                if (!f.getName().equals(ACTIVE_GEN_POINTER)) {
                    Files.deleteIfExists(f.toPath());
                }
            }
        }
    }

    @Override
    public IKVSpaceRefreshableReader reader() {
        return new RocksDBKVSpaceReader(id, opMeters, logger, syncContext.refresher(), this::handle,
            this::currentMetadata, new IteratorOptions(true, 0));
    }

    private class RestoreSession implements IRestoreSession {
        private final File stagingDir;
        private final RocksDBKVSpaceWriterHelper helper;
        private final AdaptiveWriteBudget adaptiveWriteBudget = new AdaptiveWriteBudget();
        private final IRestoreSession.FlushListener flushListener;
        private final Logger logger;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final RocksDBCPableKVSpaceEpochHandle stagingHandle;
        private int ops = 0;
        private long bytes = 0;
        private long batchStartNanos = -1;

        RestoreSession(RestoreMode mode, FlushListener flushListener, Logger logger) {
            this.flushListener = flushListener;
            this.logger = logger;
            try {
                stagingDir = Paths.get(spaceRootDir().getAbsolutePath(), UUID.randomUUID().toString()).toFile();
                if (mode == RestoreMode.Overlay) {
                    active.get().checkpoint.createCheckpoint(stagingDir.toString());
                } else {
                    Files.createDirectories(stagingDir.toPath());
                }
                stagingHandle = new RocksDBCPableKVSpaceEpochHandle(id, stagingDir, RocksDBCPableKVSpace.this.conf,
                    RocksDBCPableKVSpace.this::isRetired, logger, tags);
                helper = new RocksDBKVSpaceWriterHelper(stagingHandle.db, writeOptions);
            } catch (Throwable t) {
                throw new KVEngineException("Begin restore failed", t);
            }
        }

        private void ensureOpen() {
            if (closed.get()) {
                throw new IllegalStateException("Restore session already closed");
            }
        }

        private void flushIfNeeded() {
            if (adaptiveWriteBudget.shouldFlush(ops, bytes)) {
                long start = batchStartNanos > 0 ? batchStartNanos : System.nanoTime();
                helper.flush();
                long latencyMillis = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
                adaptiveWriteBudget.recordFlush(ops, bytes, latencyMillis);
                flushListener.onFlush(ops, bytes);
                ops = 0;
                bytes = 0;
                batchStartNanos = -1;
            }
        }

        @Override
        public IRestoreSession put(ByteString key, ByteString value) {
            ensureOpen();
            try {
                helper.put(stagingHandle.cf, key, value);
                if (ops == 0 && bytes == 0) {
                    batchStartNanos = System.nanoTime();
                }
                ops++;
                bytes += key.size() + value.size();
                flushIfNeeded();
                return this;
            } catch (RocksDBException e) {
                throw new KVEngineException("Restore put failed", e);
            }
        }

        @Override
        public IRestoreSession metadata(ByteString metaKey, ByteString metaValue) {
            ensureOpen();
            try {
                helper.metadata(stagingHandle.cf, metaKey, metaValue);
                return this;
            } catch (RocksDBException e) {
                throw new KVEngineException("Restore metadata failed", e);
            }
        }

        @Override
        public void done() {
            if (closed.compareAndSet(false, true)) {
                try {
                    // Flush remaining data in current batch if any
                    if (ops > 0 || bytes > 0) {
                        long start = batchStartNanos > 0 ? batchStartNanos : System.nanoTime();
                        helper.flush();
                        long latencyMillis = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
                        adaptiveWriteBudget.recordFlush(ops, bytes, latencyMillis);
                        ops = 0;
                        bytes = 0;
                        batchStartNanos = -1;
                    }
                    helper.done();
                    // switch active to staging
                    switchTo(stagingHandle);
                } catch (Throwable t) {
                    throw new KVEngineException("Restore done failed", t);
                }
            }
        }

        @Override
        public void abort() {
            if (closed.compareAndSet(false, true)) {
                try {
                    helper.abort();
                } catch (Throwable t) {
                    logger.warn("Abort restore session failed", t);
                }
                try {
                    stagingHandle.close();
                } catch (Throwable t) {
                    logger.warn("Close staging RocksDB failed", t);
                }
                try {
                    deleteDir(stagingDir.toPath());
                } catch (Throwable t) {
                    logger.warn("Delete staging dir failed: {}", stagingDir, t);
                }
            }
        }

        @Override
        public int count() {
            return helper.count();
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
