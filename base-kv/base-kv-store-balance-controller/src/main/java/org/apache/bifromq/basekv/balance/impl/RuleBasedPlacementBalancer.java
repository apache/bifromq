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

import static org.apache.bifromq.basekv.balance.util.CommandUtil.diffBy;
import static org.apache.bifromq.basekv.utils.DescriptorUtil.getEffectiveEpoch;
import static org.apache.bifromq.basekv.utils.DescriptorUtil.getEffectiveRoute;

import org.apache.bifromq.basekv.balance.BalanceNow;
import org.apache.bifromq.basekv.balance.BalanceResult;
import org.apache.bifromq.basekv.balance.NoNeedBalance;
import org.apache.bifromq.basekv.balance.StoreBalancer;
import org.apache.bifromq.basekv.balance.command.BalanceCommand;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.basekv.utils.EffectiveEpoch;
import org.apache.bifromq.basekv.utils.EffectiveRoute;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.protobuf.Struct;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * The base class for implementing Rule-based range placement balancer, subclasses can define the load rules and how the
 * load rules are used to generate range layout. Load rules are defined as a JSON object.
 */
public abstract class RuleBasedPlacementBalancer extends StoreBalancer {
    private final AtomicReference<BalanceCommand> balanceCommandHolder = new AtomicReference<>();
    private volatile Struct loadRules;

    /**
     * Constructor of StoreBalancer.
     *
     * @param clusterId    the id of the BaseKV cluster which the store belongs to
     * @param localStoreId the id of the store which the balancer is responsible for
     */
    public RuleBasedPlacementBalancer(String clusterId, String localStoreId) {
        super(clusterId, localStoreId);
    }

    @Override
    public void update(Struct loadRules) {
        this.loadRules = loadRules;
        log.debug("Update load rules: {}", loadRules);
    }

    @Override
    public final void update(Set<KVRangeStoreDescriptor> landscape) {
        log.trace("Update landscape: {}", landscape);
        if (loadRules != null) {
            update(loadRules, landscape);
        } else {
            Struct defaultLoadRules = defaultLoadRules();
            if (defaultLoadRules != null) {
                update(defaultLoadRules, landscape);
            }
        }
    }

    private void update(Struct loadRules, Set<KVRangeStoreDescriptor> landscape) {
        if (loadRules.getFieldsMap().isEmpty()) {
            //no load rules no balance
            balanceCommandHolder.set(null);
            return;
        }
        Optional<EffectiveEpoch> effectiveEpoch = getEffectiveEpoch(landscape);
        if (effectiveEpoch.isEmpty()) {
            // no effective epoch, no balance
            balanceCommandHolder.set(null);
            return;
        }
        EffectiveRoute effectiveRoute = getEffectiveRoute(effectiveEpoch.get());
        if (!BoundaryUtil.isValidSplitSet(effectiveRoute.leaderRanges().keySet())) {
            // no complete effective route in effective epoch, no balance
            balanceCommandHolder.set(null);
            return;
        }
        Optional<NavigableMap<Boundary, ClusterConfig>> expectedRoute =
            generate(loadRules, effectiveEpoch.get().storeDescriptors(), effectiveRoute);
        if (expectedRoute.isEmpty()) {
            // no expectedRange layout, no balance
            balanceCommandHolder.set(null);
            return;
        }
        if (!BoundaryUtil.isValidSplitSet(expectedRoute.get().keySet())) {
            log.warn("Invalid expected range layout: {}", expectedRoute.get());
            balanceCommandHolder.set(null);
            return;
        }
        balanceCommandHolder.set(diffBy(expectedRoute.get(), effectiveRoute));
    }

    @Override
    public final BalanceResult balance() {
        BalanceCommand command = balanceCommandHolder.get();
        if (command == null || !command.getToStore().equals(localStoreId)) {
            return NoNeedBalance.INSTANCE;
        } else {
            return BalanceNow.of(command);
        }
    }

    protected abstract Struct defaultLoadRules();

    public abstract boolean validate(Struct loadRules);

    protected abstract Map<Boundary, ClusterConfig> doGenerate(Struct loadRules,
                                                               Map<String, KVRangeStoreDescriptor> landscape,
                                                               EffectiveRoute effectiveRoute);

    private Optional<NavigableMap<Boundary, ClusterConfig>> generate(Struct loadRules,
                                                                     Set<KVRangeStoreDescriptor> landscape,
                                                                     EffectiveRoute effectiveRoute) {
        try {
            Map<String, KVRangeStoreDescriptor> landscapeMap =
                landscape.stream().collect(Collectors.toMap(KVRangeStoreDescriptor::getId, store -> store));
            Map<Boundary, ClusterConfig> rangeLayout = doGenerate(loadRules, landscapeMap, effectiveRoute);
            if (rangeLayout.isEmpty()) {
                // no range layout generated, no balance
                return Optional.empty();
            }
            if (!verify(rangeLayout, landscape)) {
                throw new IllegalStateException("Invalid range layout");
            }
            NavigableMap<Boundary, ClusterConfig> sortedRangeLayout = new TreeMap<>(BoundaryUtil::compare);
            sortedRangeLayout.putAll(rangeLayout);
            return Optional.of(sortedRangeLayout);
        } catch (Throwable e) {
            log.error("Balancer[{}] failed to generate range layout from load rules: {}",
                this.getClass().getSimpleName(), loadRules, e);
            return Optional.empty();
        }
    }

    @VisibleForTesting
    boolean verify(Map<Boundary, ClusterConfig> rangeLayout, Set<KVRangeStoreDescriptor> landscape) {
        // 1. check boundary non overlap and form a complete landscape
        if (rangeLayout.keySet().stream().anyMatch(b -> !BoundaryUtil.isNonEmptyRange(b))) {
            log.error("Balancer[{}] generated empty boundary in range layout: {}",
                this.getClass().getSimpleName(), rangeLayout);
            return false;
        }
        if (!BoundaryUtil.isValidSplitSet(rangeLayout.keySet())) {
            log.error("Balancer[{}] generated invalid boundary found in range layout: {}",
                this.getClass().getSimpleName(), rangeLayout);
            return false;
        }
        // 2. check Set<String> is non-empty and conform to landscape
        Set<String> storeIds = landscape.stream().map(KVRangeStoreDescriptor::getId).collect(Collectors.toSet());
        for (ClusterConfig clusterConfig : rangeLayout.values()) {
            if (!isValidClusterConfig(clusterConfig)
                || !storeIds.containsAll(clusterConfig.getVotersList())
                || !storeIds.containsAll(clusterConfig.getLearnersList())
                || !storeIds.containsAll(clusterConfig.getNextVotersList())
                || !storeIds.containsAll(clusterConfig.getNextLearnersList())) {
                log.error("Balancer[{}] generated invalid cluster config found in range layout: {}",
                    this.getClass().getSimpleName(), rangeLayout);
                return false;
            }
        }
        return true;
    }

    private boolean isValidClusterConfig(ClusterConfig clusterConfig) {
        if (clusterConfig.equals(ClusterConfig.getDefaultInstance())) {
            return false;
        }
        if (clusterConfig.getVotersList().isEmpty()) {
            return false;
        }
        return Sets.intersection(
            Sets.newHashSet(clusterConfig.getVotersList()),
            Sets.newHashSet(clusterConfig.getLearnersList())).isEmpty()
            &&
            Sets.intersection(
                    Sets.newHashSet(clusterConfig.getNextVotersList()),
                    Sets.newHashSet(clusterConfig.getNextLearnersList()))
                .isEmpty();
    }
}
