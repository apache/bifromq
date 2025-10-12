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

package org.apache.bifromq.starter.config.model.retain;

import com.fasterxml.jackson.annotation.JsonMerge;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.starter.config.model.BalancerOptions;
import org.apache.bifromq.starter.config.model.RocksDBEngineConfig;
import org.apache.bifromq.starter.config.model.StorageEngineConfig;

@Getter
@Setter
public class RetainStoreConfig {
    private boolean enable = true;
    // 0 means use calling thread
    private int workerThreads = 0;
    private int tickerThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / 20);
    private int queryPipelinePerStore = 100;
    private int maxWALFetchSize = 50 * 1024 * 1024; // 50MB
    private int compactWALThreshold = 2500;
    private int gcIntervalSeconds = 600;
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private StorageEngineConfig dataEngineConfig = new RocksDBEngineConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private StorageEngineConfig walEngineConfig = new RocksDBEngineConfig()
        .setManualCompaction(true)
        .setCompactMinTombstoneKeys(2500)
        .setCompactMinTombstoneRanges(2);
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private BalancerOptions balanceConfig = new BalancerOptions();
    @JsonSetter(nulls = Nulls.SKIP)
    private Map<String, String> attributes = new HashMap<>();

    public RetainStoreConfig() {
        balanceConfig.getBalancers().put("org.apache.bifromq.retain.store.balance.ReplicaCntBalancerFactory",
            Struct.newBuilder()
                .putFields("votersPerRange", Value.newBuilder().setNumberValue(3).build())
                .putFields("learnersPerRange", Value.newBuilder().setNumberValue(-1).build())
                .build());
        balanceConfig.getBalancers().put("org.apache.bifromq.retain.store.balance.RangeSplitBalancerFactory",
            Struct.newBuilder()
                .putFields("maxRangesPerStore", Value.newBuilder().setNumberValue(
                    (EnvProvider.INSTANCE.availableProcessors() / 4.0)).build())
                .putFields("maxCPUUsage", Value.newBuilder().setNumberValue(0.8).build())
                .putFields("maxIODensity", Value.newBuilder().setNumberValue(100).build())
                .putFields("ioNanosLimit", Value.newBuilder().setNumberValue(30_000).build())
                .build());
    }
}
