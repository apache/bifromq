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

package org.apache.bifromq.basekv.store.wal;

import static org.apache.bifromq.basekv.store.wal.KVRangeWALKeys.KEY_LATEST_SNAPSHOT_BYTES;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.basekv.localengine.IKVEngine;
import org.apache.bifromq.basekv.localengine.IWALableKVEngineConfigurator;
import org.apache.bifromq.basekv.localengine.IWALableKVSpace;
import org.apache.bifromq.basekv.localengine.KVEngineFactory;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import org.apache.bifromq.basekv.store.exception.KVRangeStoreException;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

/**
 * WALStore engine.
 */
public class KVRangeWALStorageEngine implements IKVRangeWALStoreEngine {
    private final Logger log;
    private final String clusterId;
    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private final Map<KVRangeId, KVRangeWALStore> instances = Maps.newConcurrentMap();
    private final IKVEngine<? extends IWALableKVSpace> kvEngine;

    public KVRangeWALStorageEngine(String clusterId,
                                   String overrideIdentity,
                                   IWALableKVEngineConfigurator configurator) {
        this.clusterId = clusterId;
        kvEngine = KVEngineFactory.createWALable(overrideIdentity, configurator);
        log = MDCLogger.getLogger(KVRangeWALStorageEngine.class, "clusterId", clusterId, "storeId", kvEngine.id());
    }

    @Override
    public void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            try {
                log.debug("Stopping WALStoreEngine");
                instances.values().forEach(KVRangeWALStore::stop);
                kvEngine.stop();
                state.set(State.STOPPED);
            } catch (Throwable e) {
                log.warn("Failed to stop WALStoreEngine", e);
            } finally {
                state.set(State.TERMINATED);
            }
        }
    }

    @Override
    public void start() {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            try {
                kvEngine.start("clusterId", clusterId, "storeId", id(), "type", "wal");
                loadExisting();
                state.set(State.STARTED);
            } catch (Throwable e) {
                state.set(State.TERMINATED);
                throw new KVRangeStoreException("Failed to start WALStoreEngine", e);
            }
        }
    }

    @Override
    public Set<KVRangeId> allKVRangeIds() {
        checkState();
        return Sets.newHashSet(instances.keySet());
    }

    @Override
    public String id() {
        return kvEngine.id();
    }

    @Override
    public IKVRangeWALStore create(KVRangeId kvRangeId, Snapshot initSnapshot) {
        checkState();
        instances.computeIfAbsent(kvRangeId, id -> {
            IWALableKVSpace kvSpace = kvEngine.createIfMissing(KVRangeIdUtil.toString(id));
            kvSpace.toWriter().put(KEY_LATEST_SNAPSHOT_BYTES, initSnapshot.toByteString())
                .done();
            kvSpace.flush().join();
            return new KVRangeWALStore(clusterId, kvEngine.id(), kvRangeId, kvSpace,
                store -> instances.remove(kvRangeId, store));
        });
        return instances.get(kvRangeId);
    }

    @Override
    public boolean has(KVRangeId kvRangeId) {
        checkState();
        return instances.containsKey(kvRangeId);
    }

    @Override
    public IKVRangeWALStore get(KVRangeId kvRangeId) {
        checkState();
        return instances.get(kvRangeId);
    }

    private void checkState() {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");
    }

    private void loadExisting() {
        kvEngine.spaces().forEach((String id, IWALableKVSpace kvSpace) -> {
            KVRangeId kvRangeId = KVRangeIdUtil.fromString(id);
            instances.put(kvRangeId,
                new KVRangeWALStore(clusterId, kvEngine.id(), kvRangeId, kvSpace,
                    store -> instances.remove(kvRangeId, store)));
            log.debug("WALStore loaded: kvRangeId={}", KVRangeIdUtil.toString(kvRangeId));

        });
    }

    private enum State {
        INIT, STARTING, STARTED, STOPPING, STOPPED, TERMINATED
    }
}
