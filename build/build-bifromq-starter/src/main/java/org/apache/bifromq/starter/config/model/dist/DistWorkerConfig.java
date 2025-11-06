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

import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.COMPACT_MIN_TOMBSTONE_KEYS;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.COMPACT_MIN_TOMBSTONE_RANGES;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.MANUAL_COMPACTION;

import com.fasterxml.jackson.annotation.JsonMerge;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.bifromq.starter.config.model.BalancerOptions;
import org.apache.bifromq.starter.config.model.EngineConfig;
import org.apache.bifromq.starter.config.model.SplitHinterOptions;

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
    @JsonMerge
    private EngineConfig dataEngineConfig = new EngineConfig()
        .setType("rocksdb")
        .setProps(new HashMap<>() {
            {
                put(MANUAL_COMPACTION, true);
                put(COMPACT_MIN_TOMBSTONE_KEYS, 2500);
                put(COMPACT_MIN_TOMBSTONE_RANGES, 2);
            }
        });
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private EngineConfig walEngineConfig = new EngineConfig()
        .setType("rocksdb")
        .setProps(new HashMap<>() {
            {
                put(MANUAL_COMPACTION, true);
                put(COMPACT_MIN_TOMBSTONE_KEYS, 2500);
                put(COMPACT_MIN_TOMBSTONE_RANGES, 2);
            }
        });
    @JsonSetter(nulls = Nulls.SKIP)
    private BalancerOptions balanceConfig = new BalancerOptions();
    @JsonSetter(nulls = Nulls.SKIP)
    private SplitHinterOptions splitHinterConfig = new SplitHinterOptions();
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

        splitHinterConfig.getHinters().put("org.apache.bifromq.dist.worker.hinter.FanoutSplitHinterFactory",
            Struct.newBuilder()
                .putFields("splitThreshold", Value.newBuilder().setNumberValue(100000).build())
                .build());
        splitHinterConfig.getHinters()
            .put("org.apache.bifromq.basekv.store.range.hinter.MutationKVLoadBasedSplitHinterFactory",
                Struct.newBuilder()
                    .putFields("windowSeconds", Value.newBuilder().setNumberValue(5).build())
                    .build());
    }
}
