/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.basekv.balance.util;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.inRange;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.startKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;

import org.apache.bifromq.basekv.balance.BalanceNow;
import org.apache.bifromq.basekv.balance.BalanceResult;
import org.apache.bifromq.basekv.balance.command.BalanceCommand;
import org.apache.bifromq.basekv.balance.command.BootstrapCommand;
import org.apache.bifromq.basekv.balance.command.ChangeConfigCommand;
import org.apache.bifromq.basekv.balance.command.MergeCommand;
import org.apache.bifromq.basekv.balance.command.SplitCommand;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.basekv.utils.EffectiveRoute;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.basekv.utils.LeaderRange;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

/**
 * Utility class for generating balance commands.
 */
public class CommandUtil {
    /**
     * Generate correct ChangeConfigCommand for quit.
     *
     * @param localStoreId    the store id of the store which the balancer is responsible for
     * @param rangeDescriptor the range descriptor of the range which the balancer is responsible for
     * @return the generated ChangeConfigCommand
     */
    public static BalanceResult quit(String localStoreId, KVRangeDescriptor rangeDescriptor) {
        ClusterConfig config = rangeDescriptor.getConfig();
        if (config.getVotersCount() > 1 || config.getLearnersCount() > 0) {
            return BalanceNow.of(ChangeConfigCommand.builder()
                .toStore(localStoreId)
                .kvRangeId(rangeDescriptor.getId())
                .expectedVer(rangeDescriptor.getVer())
                .voters(Set.of(localStoreId))
                .learners(Collections.emptySet())
                .build());
        } else {
            return BalanceNow.of(ChangeConfigCommand.builder()
                .toStore(localStoreId)
                .kvRangeId(rangeDescriptor.getId())
                .expectedVer(rangeDescriptor.getVer())
                .voters(Collections.emptySet())
                .learners(Collections.emptySet())
                .build());
        }
    }

    /**
     * Calculate the range layout difference between expected and current layout, and returns the first command to apply
     * so that the current layout can be transformed to the expected layout.
     *
     * @param expectedRouteLayout the expected range layout.
     * @param effectiveRoute      the current range layout.
     * @return the first command to apply so that the current layout can be transformed to the expected layout.
     */
    public static BalanceCommand diffBy(NavigableMap<Boundary, ClusterConfig> expectedRouteLayout,
                                        EffectiveRoute effectiveRoute) {
        assert BoundaryUtil.isValidSplitSet(expectedRouteLayout.keySet());

        Iterator<Boundary> currentItr = effectiveRoute.leaderRanges().keySet().iterator();
        for (Boundary expectedRange : expectedRouteLayout.keySet()) {
            ClusterConfig expectedRangeLayout = expectedRouteLayout.get(expectedRange);
            if (currentItr.hasNext()) {
                Boundary currentRange = currentItr.next();
                LeaderRange currentLeaderRange = effectiveRoute.leaderRanges().get(currentRange);
                ClusterConfig currentRangeLayout = currentLeaderRange.descriptor().getConfig();
                if (!currentRangeLayout.getNextVotersList().isEmpty()
                    || !currentRangeLayout.getNextLearnersList().isEmpty()) {
                    // abort generation if current layout is in config change process
                    return null;
                }
                if (expectedRange.equals(currentRange)) {
                    if (!isSame(expectedRangeLayout, currentRangeLayout)) {
                        // generate config change
                        return ChangeConfigCommand.builder()
                            .toStore(currentLeaderRange.ownerStoreDescriptor().getId())
                            .kvRangeId(currentLeaderRange.descriptor().getId())
                            .expectedVer(currentLeaderRange.descriptor().getVer())
                            .voters(setOf(expectedRangeLayout.getVotersList()))
                            .learners(setOf(expectedRangeLayout.getLearnersList()))
                            .build();
                    }
                    // move on to next
                } else if (inRange(expectedRange, currentRange)) {
                    return SplitCommand.builder()
                        .toStore(currentLeaderRange.ownerStoreDescriptor().getId())
                        .kvRangeId(currentLeaderRange.descriptor().getId())
                        .expectedVer(currentLeaderRange.descriptor().getVer())
                        .splitKey(expectedRange.getEndKey())
                        .build();
                } else if (BoundaryUtil.compare(expectedRange, currentRange) < 0) {
                    return BootstrapCommand.builder()
                        .toStore(bootstrapStore(expectedRangeLayout))
                        .boundary(toBoundary(startKey(expectedRange), startKey(currentRange)))
                        .kvRangeId(KVRangeIdUtil.next(effectiveRoute.epoch()))
                        .build();
                } else {
                    // handling merging
                    if (!currentItr.hasNext()) {
                        // no mergee range, bootstrap it using merger config
                        return BootstrapCommand.builder()
                            .toStore(bootstrapStore(expectedRangeLayout))
                            .boundary(toBoundary(currentRange.getEndKey(), expectedRange.getEndKey()))
                            .kvRangeId(KVRangeIdUtil.next(effectiveRoute.epoch()))
                            .build();
                    } else {
                        Boundary nextCurrentBoundary = currentItr.next();
                        LeaderRange nextLeaderRange = effectiveRoute.leaderRanges().get(nextCurrentBoundary);
                        ClusterConfig nextCurrentRangeLayout = nextLeaderRange.descriptor().getConfig();
                        if (!nextCurrentRangeLayout.getNextVotersList().isEmpty()
                            || !nextCurrentRangeLayout.getNextLearnersList().isEmpty()) {
                            // abort generation if there is running config change process in mergee range
                            return null;
                        } else if (isSame(expectedRangeLayout, nextCurrentRangeLayout)) {
                            return MergeCommand.builder()
                                .toStore(currentLeaderRange.ownerStoreDescriptor().getId())
                                .kvRangeId(currentLeaderRange.descriptor().getId())
                                .expectedVer(currentLeaderRange.descriptor().getVer())
                                .mergeeId(nextLeaderRange.descriptor().getId())
                                .build();
                        } else {
                            // align mergee layout with merger layout
                            return ChangeConfigCommand.builder()
                                .toStore(nextLeaderRange.ownerStoreDescriptor().getId())
                                .kvRangeId(nextLeaderRange.descriptor().getId())
                                .expectedVer(nextLeaderRange.descriptor().getVer())
                                .voters(setOf(currentRangeLayout.getVotersList()))
                                .learners(setOf(currentRangeLayout.getLearnersList()))
                                .build();
                        }
                    }
                }
            } else {
                return BootstrapCommand.builder()
                    .toStore(bootstrapStore(expectedRangeLayout))
                    .boundary(expectedRange)
                    .kvRangeId(KVRangeIdUtil.next(effectiveRoute.epoch()))
                    .build();
            }
        }
        return null;
    }

    private static String bootstrapStore(ClusterConfig layout) {
        List<String> voters = new ArrayList<>(layout.getVotersList());
        Collections.sort(voters);
        return voters.get(0);
    }

    /**
     * Check if two ClusterConfigs are the same.
     *
     * @param config1 the first ClusterConfig
     * @param config2 the second ClusterConfig
     * @return true if the two ClusterConfigs are the same, otherwise false
     */
    public static boolean isSame(ClusterConfig config1, ClusterConfig config2) {
        return setOf(config1.getVotersList()).equals(setOf(config2.getVotersList()))
            && setOf(config1.getLearnersList()).equals(setOf(config2.getLearnersList()))
            && setOf(config1.getNextVotersList()).equals(setOf(config2.getNextVotersList()))
            && setOf(config1.getNextLearnersList()).equals(setOf(config2.getNextLearnersList()));
    }

    private static Set<String> setOf(Collection<String> iterable) {
        return new HashSet<>(iterable);
    }
}
