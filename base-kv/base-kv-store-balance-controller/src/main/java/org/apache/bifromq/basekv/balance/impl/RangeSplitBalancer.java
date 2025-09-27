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

package org.apache.bifromq.basekv.balance.impl;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.compareEndKeys;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.compareStartKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.endKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.startKey;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.proto.SplitHint;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.utils.EffectiveRoute;
import org.apache.bifromq.basekv.utils.RangeLeader;

/**
 * The load-based split balancer.
 */
public class RangeSplitBalancer extends RuleBasedPlacementBalancer {
    public static final String LOAD_RULE_CPU_USAGE_LIMIT = "maxCpuUsagePerRange";
    public static final String LOAD_RULE_MAX_IO_DENSITY_PER_RANGE = "maxIODensityPerRange";
    public static final String LOAD_RULE_IO_NANOS_LIMIT_PER_RANGE = "ioNanosLimitPerRange";
    public static final String LOAD_RULE_MAX_RANGES_PER_STORE = "maxRangesPerStore";

    private static final String LOAD_TYPE_IO_DENSITY = "ioDensity";
    private static final String LOAD_TYPE_IO_LATENCY_NANOS = "ioLatencyNanos";
    private static final String LOAD_TYPE_CPU_USAGE = "cpu.usage";

    private final String hintType;

    private final Struct defaultLoadRules;

    /**
     * Constructor of StoreBalancer.
     *
     * @param clusterId         the id of the BaseKV cluster which the store belongs to
     * @param localStoreId      the id of the store which the balancer is responsible for
     * @param hintType          the type of load hint
     * @param maxRangesPerStore the max ranges per store
     * @param cpuUsageLimit     the cpu usage limit under which the balancer will be activated
     * @param maxIoDensity      the max io density for the range before it's considered for split
     * @param ioNanoLimit       the io nano limit for the range before it's considered for split
     */
    public RangeSplitBalancer(String clusterId,
                              String localStoreId,
                              String hintType,
                              int maxRangesPerStore,
                              double cpuUsageLimit,
                              int maxIoDensity,
                              long ioNanoLimit) {
        super(clusterId, localStoreId);
        this.hintType = hintType;
        this.defaultLoadRules = Struct.newBuilder()
            .putFields(LOAD_RULE_CPU_USAGE_LIMIT, Value.newBuilder().setNumberValue(cpuUsageLimit).build())
            .putFields(LOAD_RULE_MAX_IO_DENSITY_PER_RANGE,
                Value.newBuilder().setNumberValue(maxIoDensity).build())
            .putFields(LOAD_RULE_IO_NANOS_LIMIT_PER_RANGE,
                Value.newBuilder().setNumberValue(ioNanoLimit).build())
            .putFields(LOAD_RULE_MAX_RANGES_PER_STORE, Value.newBuilder().setNumberValue(maxRangesPerStore).build())
            .build();
    }

    @Override
    public Struct initialLoadRules() {
        return defaultLoadRules;
    }

    @Override
    public boolean validate(Struct loadRules) {
        Value cpuUsageLimit = loadRules.getFieldsMap().get(LOAD_RULE_CPU_USAGE_LIMIT);
        if (cpuUsageLimit == null
            || !cpuUsageLimit.hasNumberValue()
            || cpuUsageLimit.getNumberValue() < 0 || cpuUsageLimit.getNumberValue() > 1) {
            return false;
        }
        Value maxIODensityPerRange = loadRules.getFieldsMap().get(LOAD_RULE_MAX_IO_DENSITY_PER_RANGE);
        if (maxIODensityPerRange == null
            || !maxIODensityPerRange.hasNumberValue()
            || maxIODensityPerRange.getNumberValue() < 0) {
            return false;
        }
        Value maxIONanosPerRange = loadRules.getFieldsMap().get(LOAD_RULE_IO_NANOS_LIMIT_PER_RANGE);
        if (maxIONanosPerRange == null
            || !maxIONanosPerRange.hasNumberValue()
            || maxIONanosPerRange.getNumberValue() < 0) {
            return false;
        }
        Value maxRangesPerStore = loadRules.getFieldsMap().get(LOAD_RULE_MAX_RANGES_PER_STORE);
        return maxRangesPerStore != null
            && maxRangesPerStore.hasNumberValue()
            && maxRangesPerStore.getNumberValue() > 0;
    }

    @Override
    protected Map<Boundary, ClusterConfig> doGenerate(Struct loadRules,
                                                      Map<String, KVRangeStoreDescriptor> landscape,
                                                      EffectiveRoute effectiveRoute) {
        double cpuUsageLimit = loadRules.getFieldsMap().get(LOAD_RULE_CPU_USAGE_LIMIT).getNumberValue();
        double maxRangesPerStore = loadRules.getFieldsMap().get(LOAD_RULE_MAX_RANGES_PER_STORE).getNumberValue();
        double maxIODensityPerRange = loadRules.getFieldsMap().get(LOAD_RULE_MAX_IO_DENSITY_PER_RANGE).getNumberValue();
        double ioLatencyLimitPerRange =
            loadRules.getFieldsMap().get(LOAD_RULE_IO_NANOS_LIMIT_PER_RANGE).getNumberValue();
        Map<Boundary, ClusterConfig> expectedRangeLayout = new HashMap<>();
        for (Map.Entry<Boundary, RangeLeader> entry : effectiveRoute.leaderRanges().entrySet()) {
            Boundary boundary = entry.getKey();
            RangeLeader rangeLeader = entry.getValue();
            KVRangeDescriptor rangeDescriptor = rangeLeader.descriptor();
            KVRangeStoreDescriptor storeDescriptor = landscape.get(rangeLeader.storeId());
            ClusterConfig clusterConfig = rangeDescriptor.getConfig();
            if (containsDeadMember(clusterConfig, landscape.keySet())) {
                // shortcut when config contains dead members
                return Collections.emptyMap();
            }
            Optional<SplitHint> splitHintOpt = rangeDescriptor
                .getHintsList()
                .stream()
                .filter(h -> h.getType().equals(hintType))
                .findFirst();
            if (splitHintOpt.isPresent()) {
                SplitHint splitHint = splitHintOpt.get();
                double cpuUsage = storeDescriptor.getStatisticsMap().get(LOAD_TYPE_CPU_USAGE);
                double ioDensity = splitHint.getLoadOrDefault(LOAD_TYPE_IO_DENSITY, 0);
                double ioLatencyNanos = splitHint.getLoadOrDefault(LOAD_TYPE_IO_LATENCY_NANOS, 0);
                if (clusterConfig.getNextVotersList().isEmpty() && clusterConfig.getNextLearnersList().isEmpty()
                    && cpuUsage < cpuUsageLimit
                    && ioLatencyNanos < ioLatencyLimitPerRange
                    && ioDensity > maxIODensityPerRange
                    && storeDescriptor.getRangesList().size() < maxRangesPerStore
                    && splitHint.hasSplitKey()) {
                    if (compareStartKey(startKey(boundary), splitHint.getSplitKey()) < 0
                        && compareEndKeys(splitHint.getSplitKey(), endKey(boundary)) < 0) {
                        expectedRangeLayout.put(boundary
                            .toBuilder()
                            .setEndKey(splitHint.getSplitKey())
                            .build(), clusterConfig);
                        expectedRangeLayout.put(boundary
                            .toBuilder()
                            .setStartKey(splitHint.getSplitKey())
                            .build(), clusterConfig);
                    } else {
                        log.warn("Invalid split key in hint: {}, range: {}", splitHint.getSplitKey(), boundary);
                        expectedRangeLayout.put(boundary, rangeDescriptor.getConfig());
                    }
                } else {
                    expectedRangeLayout.put(boundary, rangeDescriptor.getConfig());
                }
            } else {
                expectedRangeLayout.put(boundary, rangeDescriptor.getConfig());
            }
        }
        return expectedRangeLayout;
    }

    private boolean containsDeadMember(ClusterConfig clusterConfig, Set<String> live) {
        Set<String> members = new HashSet<>();
        members.addAll(clusterConfig.getVotersList());
        members.addAll(clusterConfig.getLearnersList());
        members.addAll(clusterConfig.getNextVotersList());
        members.addAll(clusterConfig.getNextLearnersList());
        return members.stream().anyMatch(m -> !live.contains(m));
    }
}
