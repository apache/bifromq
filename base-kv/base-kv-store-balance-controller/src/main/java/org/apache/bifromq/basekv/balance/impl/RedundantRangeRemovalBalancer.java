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

import static org.apache.bifromq.basekv.balance.util.CommandUtil.quit;
import static org.apache.bifromq.basekv.utils.DescriptorUtil.getEffectiveRoute;
import static org.apache.bifromq.basekv.utils.DescriptorUtil.organizeByEpoch;

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.bifromq.basekv.balance.BalanceResult;
import org.apache.bifromq.basekv.balance.NoNeedBalance;
import org.apache.bifromq.basekv.balance.StoreBalancer;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.utils.EffectiveEpoch;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.basekv.utils.LeaderRange;

/**
 * The RedundantEpochRemovalBalancer is a specialized StoreBalancer designed to manage and remove redundant replicas
 * associated with higher epochs in a distributed key-value store. This balancer is primarily focused on ensuring that
 * only the necessary replicas from the OLDEST epoch remain active, while redundant replicas generated during bootstrap
 * or startup are removed.
 *
 * <p><b>WARNING:</b> This balancer treats the oldest epoch as the valid epoch. If a node with an older epoch is
 * introduced into a working cluster, it may lead to the removal of replicas from nodes that were previously functioning
 * correctly. This behavior can potentially disrupt the stability of the cluster and should be handled with
 * caution.</p>
 */
public class RedundantRangeRemovalBalancer extends StoreBalancer {
    private volatile NavigableMap<Long, Set<KVRangeStoreDescriptor>> latest = Collections.emptyNavigableMap();

    /**
     * Constructor of StoreBalancer.
     *
     * @param clusterId    the id of the BaseKV cluster which the store belongs to
     * @param localStoreId the id of the store which the balancer is responsible for
     */
    public RedundantRangeRemovalBalancer(String clusterId, String localStoreId) {
        super(clusterId, localStoreId);
    }

    @Override
    public void update(Set<KVRangeStoreDescriptor> landscape) {
        latest = organizeByEpoch(landscape);
    }

    @Override
    public BalanceResult balance() {
        if (latest.isEmpty()) {
            return NoNeedBalance.INSTANCE;
        }
        if (latest.size() > 1) {
            // deal with epoch-conflict ranges
            Set<KVRangeStoreDescriptor> storeDescriptors = latest.lastEntry().getValue();
            for (KVRangeStoreDescriptor storeDescriptor : storeDescriptors) {
                if (!storeDescriptor.getId().equals(localStoreId)) {
                    continue;
                }
                for (KVRangeDescriptor rangeDescriptor : storeDescriptor.getRangesList()) {
                    if (rangeDescriptor.getRole() != RaftNodeStatus.Leader) {
                        continue;
                    }
                    log.debug("Remove Epoch-Conflict range: {} in store {}",
                        KVRangeIdUtil.toString(rangeDescriptor.getId()),
                        storeDescriptor.getId());
                    return quit(localStoreId, rangeDescriptor);
                }
            }
            return NoNeedBalance.INSTANCE;
        }
        Map.Entry<Long, Set<KVRangeStoreDescriptor>> oldestEntry = latest.firstEntry();
        Map<KVRangeId, NavigableSet<LeaderRange>> conflictingRanges = findConflictingRanges(oldestEntry.getValue());
        if (!conflictingRanges.isEmpty()) {
            // deal with id-conflict ranges
            for (KVRangeId rangeId : conflictingRanges.keySet()) {
                NavigableSet<LeaderRange> leaderRanges = conflictingRanges.get(rangeId);
                for (LeaderRange leaderRange : leaderRanges) {
                    if (!leaderRange.ownerStoreDescriptor().getId().equals(localStoreId)) {
                        return NoNeedBalance.INSTANCE;
                    }
                    log.warn("Remove Id-Conflict range: {} in store {}",
                        KVRangeIdUtil.toString(leaderRange.descriptor().getId()),
                        leaderRange.ownerStoreDescriptor().getId());
                    return quit(localStoreId, leaderRange.descriptor());
                }
            }
            return NoNeedBalance.INSTANCE;
        }
        // deal with boundary-conflict ranges
        EffectiveEpoch effectiveEpoch = new EffectiveEpoch(oldestEntry.getKey(), oldestEntry.getValue());
        NavigableMap<Boundary, LeaderRange> effectiveLeaders = getEffectiveRoute(effectiveEpoch).leaderRanges();
        for (KVRangeStoreDescriptor storeDescriptor : effectiveEpoch.storeDescriptors()) {
            if (!storeDescriptor.getId().equals(localStoreId)) {
                continue;
            }
            for (KVRangeDescriptor rangeDescriptor : storeDescriptor.getRangesList()) {
                if (rangeDescriptor.getRole() != RaftNodeStatus.Leader) {
                    continue;
                }
                Boundary boundary = rangeDescriptor.getBoundary();
                LeaderRange leaderRange = effectiveLeaders.get(boundary);
                if (leaderRange == null || !leaderRange.descriptor().getId().equals(rangeDescriptor.getId())) {
                    log.warn("Remove Boundary-Conflict range: {} in store {}",
                        KVRangeIdUtil.toString(rangeDescriptor.getId()),
                        storeDescriptor.getId());
                    return quit(localStoreId, rangeDescriptor);
                }
            }
        }
        return NoNeedBalance.INSTANCE;
    }

    private Map<KVRangeId, NavigableSet<LeaderRange>> findConflictingRanges(
        Set<KVRangeStoreDescriptor> effectiveEpoch) {
        Map<KVRangeId, NavigableSet<LeaderRange>> leaderRangesByRangeId = new HashMap<>();
        Map<KVRangeId, NavigableSet<LeaderRange>> conflictingRanges = new HashMap<>();
        for (KVRangeStoreDescriptor storeDescriptor : effectiveEpoch) {
            for (KVRangeDescriptor rangeDescriptor : storeDescriptor.getRangesList()) {
                if (rangeDescriptor.getRole() != RaftNodeStatus.Leader) {
                    continue;
                }
                KVRangeId rangeId = rangeDescriptor.getId();
                SortedSet<LeaderRange> leaderRanges = leaderRangesByRangeId.computeIfAbsent(rangeId, k -> new TreeSet<>(
                    Comparator.comparing((LeaderRange lr) -> lr.ownerStoreDescriptor().getId(), String::compareTo)
                        .reversed()));
                leaderRanges.add(new LeaderRange(rangeDescriptor, storeDescriptor));
            }
        }
        for (KVRangeId rangeId : leaderRangesByRangeId.keySet()) {
            NavigableSet<LeaderRange> leaderRanges = leaderRangesByRangeId.get(rangeId);
            LeaderRange firstLeaderRange = leaderRanges.first();
            ClusterConfig firstLeaderClusterConfig = firstLeaderRange.descriptor().getConfig();
            if (leaderRanges.size() > 1) {
                NavigableSet<LeaderRange> restLeaderRanges = leaderRanges.tailSet(firstLeaderRange, false);
                // check if rest leader ranges are conflicting: disjoint voter set
                for (LeaderRange restLeaderRange : restLeaderRanges) {
                    ClusterConfig restLeaderClusterConfig = restLeaderRange.descriptor().getConfig();
                    if (isDisjoint(firstLeaderClusterConfig, restLeaderClusterConfig)) {
                        // if disjoint, add to conflicting ranges
                        conflictingRanges.put(rangeId, leaderRanges);
                    }
                }
            }
        }
        return conflictingRanges;
    }

    private boolean isDisjoint(ClusterConfig firstConfig, ClusterConfig secondConfig) {
        Set<String> firstVoters = Sets.newHashSet(firstConfig.getVotersList());
        Set<String> secondVoters = Sets.newHashSet(secondConfig.getVotersList());
        Set<String> firstNextVoters = Sets.newHashSet(firstConfig.getNextVotersList());
        Set<String> secondNextVoters = Sets.newHashSet(secondConfig.getNextVotersList());
        return Collections.disjoint(firstVoters, secondVoters)
            && Collections.disjoint(firstNextVoters, secondNextVoters);
    }
}
