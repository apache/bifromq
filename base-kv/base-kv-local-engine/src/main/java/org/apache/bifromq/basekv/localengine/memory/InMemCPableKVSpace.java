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

package org.apache.bifromq.basekv.localengine.memory;

import static com.google.protobuf.ByteString.unsignedLexicographicalComparator;
import static org.apache.bifromq.basekv.localengine.metrics.KVSpaceMeters.getGauge;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceCheckpoint;
import org.apache.bifromq.basekv.localengine.IKVSpaceMigratableWriter;
import org.apache.bifromq.basekv.localengine.IRestoreSession;
import org.apache.bifromq.basekv.localengine.RestoreMode;
import org.apache.bifromq.basekv.localengine.metrics.GeneralKVSpaceMetric;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.slf4j.Logger;

public class InMemCPableKVSpace extends InMemKVSpace<InMemCPableKVEngine, InMemCPableKVSpace>
    implements ICPableKVSpace {
    private final Cache<String, InMemKVSpaceCheckpoint> checkpoints;
    private final Gauge checkpointGauge;
    private volatile InMemKVSpaceCheckpoint latestCheckpoint;

    protected InMemCPableKVSpace(String id,
                                 InMemKVEngineConfigurator configurator,
                                 InMemCPableKVEngine engine,
                                 Runnable onDestroy,
                                 KVSpaceOpMeters opMeters,
                                 Logger logger,
                                 String... tags) {
        super(id, configurator, engine, onDestroy, opMeters, logger);
        checkpoints = Caffeine.newBuilder().weakValues().build();
        checkpointGauge = getGauge(id, GeneralKVSpaceMetric.CheckpointNumGauge, checkpoints::estimatedSize,
            Tags.of(tags));
    }

    @Override
    public String checkpoint() {
        synchronized (this) {
            return metadataRefresher.call(() -> {
                String cpId = UUID.randomUUID().toString();
                latestCheckpoint = new InMemKVSpaceCheckpoint(id, cpId, new HashMap<>(metadataMap), rangeData.clone(),
                    opMeters, logger);
                checkpoints.put(cpId, latestCheckpoint);
                return cpId;
            });
        }
    }

    @Override
    public Optional<IKVSpaceCheckpoint> openCheckpoint(String checkpointId) {
        return Optional.ofNullable(checkpoints.getIfPresent(checkpointId));
    }

    @Override
    public IRestoreSession startRestore(IRestoreSession.FlushListener flushListener) {
        return new RestoreSession(RestoreMode.Replace, flushListener);
    }

    @Override
    public IRestoreSession startReceiving(IRestoreSession.FlushListener flushListener) {
        return new RestoreSession(RestoreMode.Overlay, flushListener);
    }

    @Override
    public void close() {
        super.close();
        checkpointGauge.close();
    }

    @Override
    public IKVSpaceMigratableWriter toWriter() {
        return new InMemKVSpaceMigratableWriter<>(id, metadataMap, rangeData, engine, syncContext,
            metadataUpdated -> {
                if (metadataUpdated) {
                    this.loadMetadata();
                }
            }, opMeters, logger);
    }

    private class RestoreSession implements IRestoreSession {
        private final ConcurrentHashMap<ByteString, ByteString> stagedMetadata = new ConcurrentHashMap<>();
        private final ConcurrentSkipListMap<ByteString, ByteString> stagedData = new ConcurrentSkipListMap<>(
            unsignedLexicographicalComparator());
        private final RestoreMode mode;
        private final IRestoreSession.FlushListener flushListener;
        private final AtomicBoolean closed = new AtomicBoolean();
        private int ops = 0;
        private long bytes = 0;

        private RestoreSession(RestoreMode mode, FlushListener flushListener) {
            this.mode = mode;
            this.flushListener = flushListener;
        }

        private void ensureOpen() {
            if (closed.get()) {
                throw new IllegalStateException("Restore session already closed");
            }
        }

        @Override
        public IRestoreSession put(ByteString key, ByteString value) {
            ensureOpen();
            stagedData.put(key, value);
            ops++;
            bytes += key.size() + value.size();
            return this;
        }

        @Override
        public IRestoreSession metadata(ByteString metaKey, ByteString metaValue) {
            ensureOpen();
            stagedMetadata.put(metaKey, metaValue);
            ops++;
            bytes += metaKey.size() + metaValue.size();
            return this;
        }

        @Override
        public void done() {
            ensureOpen();
            if (closed.compareAndSet(false, true)) {
                // Replace mode ignores existing state; Overlay mode applies on top of current state
                syncContext().mutator().run(() -> {
                    if (mode == RestoreMode.Replace) {
                        metadataMap.clear();
                        rangeData.clear();
                    }
                    metadataMap.putAll(stagedMetadata);
                    rangeData.putAll(stagedData);
                    if (flushListener != null) {
                        flushListener.onFlush(ops, bytes);
                    }
                    loadMetadata();
                    // InMemCPableKVSpace is not generation aware
                    return false;
                });
            }
        }

        @Override
        public void abort() {
            if (closed.compareAndSet(false, true)) {
                stagedMetadata.clear();
                stagedData.clear();
            }
        }

        @Override
        public int count() {
            return ops;
        }
    }
}
