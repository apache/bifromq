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

package org.apache.bifromq.basekv.balance.impl;

import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.union;

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.utils.EffectiveRoute;
import org.apache.bifromq.basekv.utils.LeaderRange;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * ReplicaCntBalancer is used to achieve following goals:
 * <ul>
 *    <li>1. meet the expected number of Voter replicas and learner replicas for each Range dynamically.</li>
 *    <li>2. evenly distributed range replicas across all stores.</li>
 * </ul>
 * <br>
 * The Balancer supports controlling the number of Voter and Learner replicas via load rules at runtime.
 */
public class ReplicaCntBalancer extends RuleBasedPlacementBalancer {
    public static final String LOAD_RULE_VOTERS = "votersPerRange";
    public static final String LOAD_RULE_LEARNERS = "learnersPerRange";
    private final Struct defaultLoadRules;

    /**
     * Constructor of StoreBalancer.
     *
     * @param clusterId        the id of the BaseKV cluster which the store belongs to
     * @param localStoreId     the id of the store which the balancer is responsible for
     * @param votersPerRange   default number of voters per range if not specified via load rules
     * @param learnersPerRange default number of learners per range if not specified via load rules
     */
    public ReplicaCntBalancer(String clusterId,
                              String localStoreId,
                              int votersPerRange,
                              int learnersPerRange) {
        super(clusterId, localStoreId);
        defaultLoadRules = Struct.newBuilder()
            .putFields(LOAD_RULE_VOTERS, Value.newBuilder().setNumberValue(votersPerRange).build())
            .putFields(LOAD_RULE_LEARNERS, Value.newBuilder().setNumberValue(learnersPerRange).build())
            .build();
        Preconditions.checkArgument(validate(defaultLoadRules), "Invalid default load rules");
    }

    @Override
    protected Struct defaultLoadRules() {
        return defaultLoadRules;
    }

    @Override
    public boolean validate(Struct loadRules) {
        Value voters = loadRules.getFieldsMap().get(LOAD_RULE_VOTERS);
        // voters must be odd number
        if (voters == null
            || !voters.hasNumberValue()
            || voters.getNumberValue() == 0
            || voters.getNumberValue() % 2 == 0) {
            return false;
        }
        Value learners = loadRules.getFieldsMap().get(LOAD_RULE_LEARNERS);
        return learners != null && learners.hasNumberValue() && !(learners.getNumberValue() < -1);
    }

    @Override
    protected Map<Boundary, ClusterConfig> doGenerate(Struct loadRules,
                                                      Map<String, KVRangeStoreDescriptor> landscape,
                                                      EffectiveRoute effectiveRoute) {
        Map<Boundary, ClusterConfig> expectedRangeLayout = new HashMap<>();
        boolean meetingGoalOne = meetExpectedConfig(loadRules, landscape, effectiveRoute, expectedRangeLayout);
        if (meetingGoalOne) {
            return expectedRangeLayout;
        }
        boolean meetingGoalTwo = balanceVoterCount(landscape, effectiveRoute, expectedRangeLayout);
        if (meetingGoalTwo) {
            return expectedRangeLayout;
        }
        balanceLearnerCount(landscape, effectiveRoute, expectedRangeLayout);
        return expectedRangeLayout;
    }

    private boolean meetExpectedConfig(Struct loadRules,
                                       Map<String, KVRangeStoreDescriptor> landscape,
                                       EffectiveRoute effectiveRoute,
                                       Map<Boundary, ClusterConfig> expectedRangeLayout) {
        int expectedVoters = (int) loadRules.getFieldsMap().get(LOAD_RULE_VOTERS).getNumberValue();
        int expectedLearners = (int) loadRules.getFieldsMap().get(LOAD_RULE_LEARNERS).getNumberValue();
        // meeting goal one - meet the expected number of Voter replicas and learner replicas for each Range dynamically
        boolean meetingGoal = false;
        for (Map.Entry<Boundary, LeaderRange> entry : effectiveRoute.leaderRanges().entrySet()) {
            Boundary boundary = entry.getKey();
            LeaderRange leaderRange = entry.getValue();
            KVRangeDescriptor rangeDescriptor = leaderRange.descriptor();
            ClusterConfig clusterConfig = rangeDescriptor.getConfig();
            if (meetingGoal) {
                expectedRangeLayout.put(boundary, clusterConfig);
                continue;
            }
            if (clusterConfig.getNextVotersCount() > 0 || clusterConfig.getNextLearnersCount() > 0) {
                // if there is running config change process, abort generation
                expectedRangeLayout.put(boundary, clusterConfig);
                meetingGoal = true;
                continue;
            }
            // voter count not meet expectation or exceeds actual store node amount
            Set<String> voters = new HashSet<>(clusterConfig.getVotersList());
            Set<String> learners = new HashSet<>(clusterConfig.getLearnersList());
            if (clusterConfig.getVotersCount() != expectedVoters || clusterConfig.getVotersCount() > landscape.size()) {
                if (clusterConfig.getVotersCount() < expectedVoters) {
                    // add some voters from the least range count store
                    List<String> aliveStoresSortedByRangeCountAsc = landscape.entrySet().stream()
                        .filter(e ->
                            !learners.contains(e.getKey()) && !voters.contains(e.getKey()))
                        .sorted(Comparator.comparingInt(e -> e.getValue().getRangesCount()))
                        .map(Map.Entry::getKey)
                        .toList();
                    for (String aliveStoreId : aliveStoresSortedByRangeCountAsc) {
                        voters.add(aliveStoreId);
                        if (voters.size() == expectedVoters) {
                            break;
                        }
                    }
                } else {
                    // remove some voters from the most range count store
                    List<String> aliveStoresSortedByRangeCountDesc = landscape.entrySet().stream()
                        .sorted((a, b) -> b.getValue().getRangesCount() - a.getValue().getRangesCount())
                        .map(Map.Entry::getKey)
                        .toList();
                    for (String aliveStoreId : aliveStoresSortedByRangeCountDesc) {
                        if (!aliveStoreId.equals(leaderRange.ownerStoreDescriptor().getId())) {
                            voters.remove(aliveStoreId);
                        }
                        if (voters.size() == expectedVoters) {
                            break;
                        }
                    }
                }
                // remove unreachable voters
                voters.removeIf(voter -> !landscape.containsKey(voter));
                ClusterConfig newConfig = ClusterConfig.newBuilder()
                    .mergeFrom(clusterConfig)
                    .clearVoters()
                    .addAllVoters(voters)
                    .build();
                if (!newConfig.equals(clusterConfig)) {
                    meetingGoal = true;
                }
                expectedRangeLayout.put(boundary, newConfig);
            } else {
                expectedRangeLayout.put(boundary, clusterConfig);
            }
        }
        if (meetingGoal) {
            return true;
        }
        // voter count met the expectation, check learner count
        for (Map.Entry<Boundary, LeaderRange> entry : effectiveRoute.leaderRanges().entrySet()) {
            Boundary boundary = entry.getKey();
            LeaderRange leaderRange = entry.getValue();
            KVRangeDescriptor rangeDescriptor = leaderRange.descriptor();
            ClusterConfig clusterConfig = rangeDescriptor.getConfig();
            if (meetingGoal) {
                expectedRangeLayout.put(boundary, clusterConfig);
                continue;
            }
            Set<String> voters = new HashSet<>(clusterConfig.getVotersList());
            Set<String> learners = new HashSet<>(clusterConfig.getLearnersList());
            if (expectedLearners == -1
                || clusterConfig.getLearnersCount() != expectedLearners
                || clusterConfig.getLearnersCount() > landscape.size()) {
                if (expectedLearners == -1) {
                    Set<String> newLearners = new HashSet<>(landscape.keySet());
                    newLearners.removeAll(voters);
                    learners.addAll(newLearners);
                } else {
                    if (clusterConfig.getLearnersCount() < expectedLearners) {
                        // add some learners from the least range count store
                        List<String> aliveStoresSortedByRangeCountAsc = landscape.entrySet().stream()
                            .sorted(Comparator.comparingInt(e -> e.getValue().getRangesCount()))
                            .map(Map.Entry::getKey)
                            .toList();
                        for (String aliveStoreId : aliveStoresSortedByRangeCountAsc) {
                            if (!voters.contains(aliveStoreId)) {
                                learners.add(aliveStoreId);
                            }
                            if (learners.size() == expectedVoters) {
                                break;
                            }
                        }
                    } else {
                        // remove some learners from the most range count store
                        List<String> aliveStoresSortedByRangeCountDesc = landscape.entrySet().stream()
                            .sorted((a, b) -> b.getValue().getRangesCount() - a.getValue().getRangesCount())
                            .map(Map.Entry::getKey)
                            .toList();
                        for (String aliveStoreId : aliveStoresSortedByRangeCountDesc) {
                            learners.remove(aliveStoreId);
                            if (learners.size() == expectedLearners) {
                                break;
                            }
                        }
                    }
                }
                // remove unreachable learners
                learners.removeIf(learner -> !landscape.containsKey(learner));
                ClusterConfig newConfig = ClusterConfig.newBuilder()
                    .mergeFrom(clusterConfig)
                    .clearLearners()
                    .addAllLearners(learners)
                    .build();
                if (!newConfig.equals(clusterConfig)) {
                    meetingGoal = true;
                }
                expectedRangeLayout.put(boundary, newConfig);
            } else {
                expectedRangeLayout.put(boundary, clusterConfig);
            }
        }
        return meetingGoal;
    }

    private boolean balanceVoterCount(Map<String, KVRangeStoreDescriptor> landscape,
                                      EffectiveRoute effectiveRoute,
                                      Map<Boundary, ClusterConfig> expectedRangeLayout) {
        // goal one has met, meeting goal two - evenly distributed voter replicas across all stores
        boolean meetingGoal = false;
        Map<String, Integer> storeVoterCount = new HashMap<>();
        for (Map.Entry<Boundary, LeaderRange> entry : effectiveRoute.leaderRanges().entrySet()) {
            ClusterConfig config = entry.getValue().descriptor().getConfig();
            config.getVotersList()
                .forEach(storeId -> storeVoterCount.put(storeId, storeVoterCount.getOrDefault(storeId, 0) + 1));
        }
        landscape.keySet().forEach(storeId -> {
            if (!storeVoterCount.containsKey(storeId)) {
                storeVoterCount.put(storeId, 0);
            }
        });
        record StoreVoterCount(String storeId, int voterCount) {
        }

        SortedSet<StoreVoterCount> storeVoterCountSorted = new TreeSet<>(Comparator
            .comparingInt(StoreVoterCount::voterCount).thenComparing(StoreVoterCount::storeId));
        storeVoterCount.forEach(
            (storeId, voterCount) -> storeVoterCountSorted.add(new StoreVoterCount(storeId, voterCount)));
        double totalVoters = storeVoterCount.values().stream().mapToInt(Integer::intValue).sum();
        double targetVotersPerStore = totalVoters / landscape.size();
        int maxVotersPerStore = (int) Math.ceil(targetVotersPerStore);
        for (Map.Entry<Boundary, LeaderRange> entry : effectiveRoute.leaderRanges().entrySet()) {
            Boundary boundary = entry.getKey();
            LeaderRange leaderRange = entry.getValue();
            KVRangeDescriptor rangeDescriptor = leaderRange.descriptor();
            ClusterConfig clusterConfig = rangeDescriptor.getConfig();
            if (meetingGoal) {
                expectedRangeLayout.put(boundary, clusterConfig);
                continue;
            }
            // examine in sorted order to ensure the result is deterministic
            Set<String> learners = Sets.newHashSet(clusterConfig.getLearnersList());
            SortedSet<String> voterSorted = Sets.newTreeSet(clusterConfig.getVotersList());
            for (String voter : voterSorted) {
                if (storeVoterCount.get(voter) > maxVotersPerStore) {
                    // voter store has overloaded voters
                    for (StoreVoterCount underloadedStore : storeVoterCountSorted) {
                        // move to one underloaded store which is current not in the voter list
                        if (storeVoterCount.get(underloadedStore.storeId) < maxVotersPerStore
                            && !voterSorted.contains(underloadedStore.storeId)
                            && !learners.contains(underloadedStore.storeId)) {
                            meetingGoal = true;
                            ClusterConfig newConfig = ClusterConfig.newBuilder()
                                .addAllVoters(
                                    difference(union(voterSorted, Set.of(underloadedStore.storeId)), Set.of(voter)))
                                .addAllLearners(learners)
                                .build();
                            expectedRangeLayout.put(boundary, newConfig);
                            break;
                        }
                    }
                }
            }
        }
        return meetingGoal;
    }

    private boolean balanceLearnerCount(Map<String, KVRangeStoreDescriptor> landscape,
                                        EffectiveRoute effectiveRoute,
                                        Map<Boundary, ClusterConfig> expectedRangeLayout) {
        boolean meetingGoal = false;
        Map<String, Integer> storeLearnerCount = new HashMap<>();
        for (Map.Entry<Boundary, LeaderRange> entry : effectiveRoute.leaderRanges().entrySet()) {
            ClusterConfig config = entry.getValue().descriptor().getConfig();
            config.getLearnersList()
                .forEach(storeId -> storeLearnerCount.put(storeId, storeLearnerCount.getOrDefault(storeId, 0) + 1));
        }
        landscape.keySet().forEach(storeId -> {
            if (!storeLearnerCount.containsKey(storeId)) {
                storeLearnerCount.put(storeId, 0);
            }
        });
        record StoreLearnerCount(String storeId, int voterCount) {
        }

        SortedSet<StoreLearnerCount> storeVoterCountSorted = new TreeSet<>(Comparator
            .comparingInt(StoreLearnerCount::voterCount).thenComparing(StoreLearnerCount::storeId));
        storeLearnerCount.forEach(
            (storeId, voterCount) -> storeVoterCountSorted.add(new StoreLearnerCount(storeId, voterCount)));

        double totalLearners = storeLearnerCount.values().stream().mapToInt(Integer::intValue).sum();
        double targetLearnersPerStore = totalLearners / landscape.size();
        int maxLearnersPerStore = (int) Math.ceil(targetLearnersPerStore);

        for (Map.Entry<Boundary, LeaderRange> entry : effectiveRoute.leaderRanges().entrySet()) {
            Boundary boundary = entry.getKey();
            LeaderRange leaderRange = entry.getValue();
            KVRangeDescriptor rangeDescriptor = leaderRange.descriptor();
            ClusterConfig clusterConfig = rangeDescriptor.getConfig();
            if (meetingGoal) {
                expectedRangeLayout.put(boundary, clusterConfig);
                continue;
            }
            // examine in sorted order to ensure the result is deterministic
            Set<String> voters = Sets.newHashSet(clusterConfig.getVotersList());
            SortedSet<String> learnerSorted = Sets.newTreeSet(clusterConfig.getLearnersList());
            for (String learner : learnerSorted) {
                if (storeLearnerCount.get(learner) > maxLearnersPerStore) {
                    // learner store has overloaded learners
                    for (StoreLearnerCount underloadedStore : storeVoterCountSorted) {
                        // move to one underloaded store which is current not in the voter or learner list
                        if (storeLearnerCount.get(underloadedStore.storeId) < maxLearnersPerStore
                            && !voters.contains(underloadedStore.storeId)
                            && !learnerSorted.contains(underloadedStore.storeId)) {
                            meetingGoal = true;
                            ClusterConfig newConfig = ClusterConfig.newBuilder()
                                .addAllVoters(voters)
                                .addAllLearners(difference(
                                    union(learnerSorted, Set.of(underloadedStore.storeId)), Set.of(learner)))
                                .build();
                            expectedRangeLayout.put(boundary, newConfig);
                            break;
                        }
                    }
                }
            }
        }
        return meetingGoal;
    }
}
