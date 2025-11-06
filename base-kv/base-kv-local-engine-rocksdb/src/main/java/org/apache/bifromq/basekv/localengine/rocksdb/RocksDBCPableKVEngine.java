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

import static org.apache.bifromq.basekv.localengine.StructUtil.strVal;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_CHECKPOINT_ROOT_DIR;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_ROOT_DIR;

import com.google.protobuf.Struct;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.slf4j.Logger;

class RocksDBCPableKVEngine extends RocksDBKVEngine<RocksDBCPableKVSpace> {
    private final File cpRootDir;
    private MetricManager metricManager;

    public RocksDBCPableKVEngine(String overrideIdentity, Struct conf) {
        super(overrideIdentity, conf);
        cpRootDir = new File(strVal(conf, DB_CHECKPOINT_ROOT_DIR));
        try {
            Files.createDirectories(cpRootDir.getAbsoluteFile().toPath());
        } catch (Throwable e) {
            throw new KVEngineException("Failed to create checkpoint root folder", e);
        }
    }

    @Override
    protected RocksDBCPableKVSpace doBuildKVSpace(String spaceId,
                                                  Struct conf,
                                                  Runnable onDestroy,
                                                  KVSpaceOpMeters opMeters,
                                                  Logger logger,
                                                  String... tags) {
        return new RocksDBCPableKVSpace(spaceId, conf, this, onDestroy, opMeters, logger, tags);
    }

    @Override
    protected Struct defaultConf() {
        return RocksDBDefaultConfigs.CP;
    }

    @Override
    protected void validateSemantics(Struct conf) {
        try {
            Paths.get(strVal(conf, DB_ROOT_DIR));
            Paths.get(strVal(conf, DB_CHECKPOINT_ROOT_DIR));
        } catch (Throwable t) {
            throw new IllegalArgumentException("Invalid RocksDB data/checkpoint path in configuration", t);
        }
    }

    @Override
    protected void doStart(String... metricTags) {
        super.doStart(metricTags);
        metricManager = new MetricManager(metricTags);
    }

    @Override
    protected void doStop() {
        metricManager.close();
        super.doStop();
    }

    private class MetricManager {
        private final Gauge checkpointTotalSpaceGauge;
        private final Gauge checkpointsUsableSpaceGauge;

        MetricManager(String... metricTags) {
            Tags tags = Tags.of(metricTags);
            checkpointTotalSpaceGauge =
                Gauge.builder("basekv.le.rocksdb.total.checkpoints", cpRootDir::getTotalSpace)
                    .tags(tags)
                    .register(Metrics.globalRegistry);
            checkpointsUsableSpaceGauge = Gauge.builder("basekv.le.rocksdb.usable.checkpoints",
                    cpRootDir::getUsableSpace)
                .tags(tags)
                .register(Metrics.globalRegistry);
        }

        void close() {
            Metrics.globalRegistry.remove(checkpointTotalSpaceGauge);
            Metrics.globalRegistry.remove(checkpointsUsableSpaceGauge);
        }
    }
}
