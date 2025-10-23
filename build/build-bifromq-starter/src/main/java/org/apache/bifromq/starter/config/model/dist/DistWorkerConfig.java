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

package org.apache.bifromq.starter.config.model.dist;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.bifromq.starter.config.model.BalancerOptions;
import org.apache.bifromq.starter.config.model.RocksDBEngineConfig;
import org.apache.bifromq.starter.config.model.StorageEngineConfig;
import org.apache.bifromq.starter.config.model.StorageEngineConfigUtil;

@Getter
@Setter
public class DistWorkerConfig {
    private boolean enable = true;
    // 0 for doing tasks on calling threads
    private int workerThreads = 0;
    private int tickerThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / 20);
    private int maxWALFetchSize = 10 * 1024 * 1024; // 10MB
    private int compactWALThreshold = 256 * 1024 * 1024;
    private int minGCIntervalSeconds = 30; // every 30 s
    private int maxGCIntervalSeconds = 24 * 3600; // every day
    @JsonSetter(nulls = Nulls.SKIP)
    private StorageEngineConfig dataEngineConfig = new RocksDBEngineConfig()
        .setManualCompaction(true)
        .setCompactMinTombstoneKeys(2500)
        .setCompactMinTombstoneRanges(2);
    @JsonSetter(nulls = Nulls.SKIP)
    private StorageEngineConfig walEngineConfig = new RocksDBEngineConfig()
        .setManualCompaction(true)
        .setCompactMinTombstoneKeys(2500)
        .setCompactMinTombstoneRanges(2);
    @JsonSetter(nulls = Nulls.SKIP)
    private BalancerOptions balanceConfig = new BalancerOptions();
    @JsonSetter(nulls = Nulls.SKIP)
    private Map<String, String> attributes = new HashMap<>();

    public DistWorkerConfig() {
        balanceConfig.getBalancers().put("org.apache.bifromq.dist.worker.balance.RangeLeaderBalancerFactory",
            Struct.getDefaultInstance());
        balanceConfig.getBalancers().put("org.apache.bifromq.dist.worker.balance.ReplicaCntBalancerFactory",
            Struct.newBuilder()
                .putFields("votersPerRange", Value.newBuilder().setNumberValue(3).build())
                .putFields("learnersPerRange", Value.newBuilder().setNumberValue(-1).build())
                .build());
    }

    @JsonSetter("dataEngineConfig")
    public void setDataEngineConfig(Map<String, Object> conf) {
        dataEngineConfig = StorageEngineConfigUtil.updateOrReplace(dataEngineConfig, conf);
    }

    @JsonSetter("walEngineConfig")
    public void setWalEngineConfig(Map<String, Object> conf) {
        walEngineConfig = StorageEngineConfigUtil.updateOrReplace(walEngineConfig, conf);
    }
}
