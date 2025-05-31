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

package org.apache.bifromq.basekv.localengine.memory;

import static org.apache.bifromq.basekv.localengine.metrics.KVSpaceMeters.getGauge;

import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceCheckpoint;
import org.apache.bifromq.basekv.localengine.metrics.GeneralKVSpaceMetric;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
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
    public void close() {
        super.close();
        checkpointGauge.close();
    }
}
