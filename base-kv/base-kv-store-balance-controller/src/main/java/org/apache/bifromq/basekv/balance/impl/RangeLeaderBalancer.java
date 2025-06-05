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


import static org.apache.bifromq.basekv.utils.DescriptorUtil.getEffectiveEpoch;
import static org.apache.bifromq.basekv.utils.DescriptorUtil.getEffectiveRoute;

import org.apache.bifromq.basekv.balance.BalanceNow;
import org.apache.bifromq.basekv.balance.BalanceResult;
import org.apache.bifromq.basekv.balance.NoNeedBalance;
import org.apache.bifromq.basekv.balance.StoreBalancer;
import org.apache.bifromq.basekv.balance.command.TransferLeadershipCommand;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.utils.EffectiveEpoch;
import org.apache.bifromq.basekv.utils.EffectiveRoute;
import org.apache.bifromq.basekv.utils.LeaderRange;
import com.google.common.collect.Sets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;

/**
 * The goal of the balancer is to balance the leader count of each store by emitting TransferLeadership command.
 */
public class RangeLeaderBalancer extends StoreBalancer {
    private volatile EffectiveEpoch effectiveEpoch = null;

    public RangeLeaderBalancer(String clusterId, String localStoreId) {
        super(clusterId, localStoreId);
    }

    @Override
    public void update(Set<KVRangeStoreDescriptor> landscape) {
        Optional<EffectiveEpoch> effectiveEpoch = getEffectiveEpoch(landscape);
        if (effectiveEpoch.isEmpty()) {
            return;
        }
        this.effectiveEpoch = effectiveEpoch.get();
    }

    @Override
    public BalanceResult balance() {
        EffectiveEpoch currentEpoch = effectiveEpoch;
        if (currentEpoch == null) {
            return NoNeedBalance.INSTANCE;
        }
        // leader ranges including non-effective ranges
        EffectiveRoute effectiveRoute = getEffectiveRoute(currentEpoch);
        if (effectiveRoute.leaderRanges().isEmpty()) {
            // only operate on the leader range in leaderRanges
            return NoNeedBalance.INSTANCE;
        }
        return balanceLeaderCount(effectiveRoute.leaderRanges());
    }

    private BalanceResult balanceLeaderCount(NavigableMap<Boundary, LeaderRange> effectiveRoute) {
        Map<String, Integer> storeLeaderCount = new HashMap<>();
        for (Map.Entry<Boundary, LeaderRange> entry : effectiveRoute.entrySet()) {
            String localStoreId = entry.getValue().ownerStoreDescriptor().getId();
            KVRangeDescriptor rangeDescriptor = entry.getValue().descriptor();
            ClusterConfig clusterConfig = rangeDescriptor.getConfig();
            clusterConfig.getVotersList().forEach(voter -> storeLeaderCount.computeIfAbsent(voter, k -> 0));
            storeLeaderCount.compute(localStoreId, (k, v) -> v == null ? 1 : v + 1);
        }
        List<String> storeSortedByLeaderCount = storeLeaderCount.entrySet().stream()
            .sorted(Comparator.comparingInt(Map.Entry::getValue))
            .map(Map.Entry::getKey)
            .toList();
        String mostLeadersStore = storeSortedByLeaderCount.get(storeSortedByLeaderCount.size() - 1);
        String leastLeadersStore = storeSortedByLeaderCount.get(0);
        int mostLeadersCount = storeLeaderCount.get(mostLeadersStore);
        int leastLeadersCount = storeLeaderCount.get(leastLeadersStore);

        double totalLeaders = storeLeaderCount.values().stream().mapToInt(Integer::intValue).sum();
        double targetLeadersPerStore = totalLeaders / storeLeaderCount.size();
        int atMostLeadersPerStore = (int) Math.ceil(targetLeadersPerStore);
        int atLeastLeadersPerStore = (int) Math.floor(targetLeadersPerStore);

        // no need to balance if the leader count is within the range
        if (mostLeadersCount <= atMostLeadersPerStore && leastLeadersCount >= atLeastLeadersPerStore) {
            return NoNeedBalance.INSTANCE;
        }
        // scan the effective route to find the range to balance
        for (Map.Entry<Boundary, LeaderRange> entry : effectiveRoute.entrySet()) {
            LeaderRange leaderRange = entry.getValue();
            String leaderStoreId = leaderRange.ownerStoreDescriptor().getId();
            KVRangeDescriptor rangeDescriptor = leaderRange.descriptor();
            ClusterConfig clusterConfig = rangeDescriptor.getConfig();
            Set<String> voters = Sets.newHashSet(clusterConfig.getVotersList());

            // check if current range leader store is most overloaded
            if (leaderStoreId.equals(mostLeadersStore)) {
                // leader store has overloaded leaders replicas
                for (String underloadedStore : storeSortedByLeaderCount) {
                    // move to one underloaded store which is current follower
                    int leaderCount = storeLeaderCount.get(underloadedStore);
                    if (leaderCount + 1 <= atMostLeadersPerStore
                        && voters.contains(underloadedStore)
                        && !underloadedStore.equals(leaderStoreId)) {
                        if (leaderStoreId.equals(localStoreId)) {
                            return BalanceNow.of(TransferLeadershipCommand.builder()
                                .toStore(leaderStoreId)
                                .kvRangeId(rangeDescriptor.getId())
                                .expectedVer(rangeDescriptor.getVer())
                                .newLeaderStore(underloadedStore)
                                .build());
                        }
                        return NoNeedBalance.INSTANCE;
                    }
                }
            } else {
                // check if least overloaded store holds a voter replica of current range and safe to transfer leadership to it
                int leaderCount = storeLeaderCount.get(leaderStoreId);
                if (voters.contains(leastLeadersStore) && leaderCount - 1 >= atLeastLeadersPerStore) {
                    if (leaderStoreId.equals(localStoreId)) {
                        return BalanceNow.of(TransferLeadershipCommand.builder()
                            .toStore(leaderStoreId)
                            .kvRangeId(rangeDescriptor.getId())
                            .expectedVer(rangeDescriptor.getVer())
                            .newLeaderStore(leastLeadersStore)
                            .build());
                    }
                    return NoNeedBalance.INSTANCE;
                }
            }
        }
        return NoNeedBalance.INSTANCE;
    }
}
