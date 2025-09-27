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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.utils.EffectiveRoute;
import org.apache.bifromq.basekv.utils.RangeLeader;

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

    private ClusterConfig buildConfig(Set<String> voters, Set<String> learners) {
        return ClusterConfig.newBuilder()
            .addAllVoters(voters)
            .addAllLearners(learners)
            .build();
    }

    private void sanitize(Set<String> s, Set<String> live) {
        s.retainAll(live);
    }

    @Override
    public Struct initialLoadRules() {
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
        final Set<String> liveStores = landscape.keySet();
        final int expectedVoters = (int) loadRules.getFieldsMap().get(LOAD_RULE_VOTERS).getNumberValue();
        final int expectedLearners = (int) loadRules.getFieldsMap().get(LOAD_RULE_LEARNERS).getNumberValue();

        if (liveStores.size() < expectedVoters) {
            for (Map.Entry<Boundary, RangeLeader> e : effectiveRoute.leaderRanges().entrySet()) {
                ClusterConfig cc = e.getValue().descriptor().getConfig();
                for (String v : cc.getVotersList()) {
                    if (!liveStores.contains(v)) {
                        // shortcut for rolling restart
                        return true;
                    }
                }
            }
        }

        boolean meetingGoal = false;

        for (Map.Entry<Boundary, RangeLeader> entry : effectiveRoute.leaderRanges().entrySet()) {
            RangeLeader rangeLeader = entry.getValue();
            KVRangeDescriptor rangeDescriptor = rangeLeader.descriptor();
            ClusterConfig clusterConfig = rangeDescriptor.getConfig();

            // if there is running config change process, abort generation and wait for the next round
            // keep range config change as linear as possible
            if (clusterConfig.getNextVotersCount() > 0 || clusterConfig.getNextLearnersCount() > 0) {
                expectedRangeLayout.clear();
                // shortcut
                return true;
            }

            final Set<String> voters = new HashSet<>(clusterConfig.getVotersList());
            final Set<String> learners = new HashSet<>(clusterConfig.getLearnersList());

            // remove unreachable stores from voters and learners
            sanitize(voters, liveStores);
            sanitize(learners, liveStores);

            Boundary boundary = entry.getKey();
            int targetVoters = Math.min(expectedVoters, liveStores.size());
            boolean needFix = voters.size() != targetVoters;
            if (!meetingGoal && needFix) {
                String leaderStore = rangeLeader.storeId();
                if (voters.size() < targetVoters) {
                    if (!learners.isEmpty()) {
                        List<String> learnerCandidates = landscape.entrySet().stream()
                            .filter(e -> learners.contains(e.getKey()))
                            .sorted(Comparator.comparingInt(e -> e.getValue().getRangesCount()))
                            .map(Map.Entry::getKey)
                            .toList();
                        for (String s : learnerCandidates) {
                            learners.remove(s);   // promote learner -> voter
                            voters.add(s);
                            if (voters.size() == targetVoters) {
                                break;
                            }
                        }
                    }

                    if (voters.size() < targetVoters) {
                        List<String> freeCandidates = landscape.entrySet().stream()
                            .filter(e -> !learners.contains(e.getKey()) && !voters.contains(e.getKey()))
                            .sorted(Comparator.comparingInt(e -> e.getValue().getRangesCount()))
                            .map(Map.Entry::getKey)
                            .toList();
                        for (String s : freeCandidates) {
                            voters.add(s);
                            if (voters.size() == targetVoters) {
                                break;
                            }
                        }
                    }

                    if (expectedLearners == -1) {
                        Set<String> newLearners = new HashSet<>(liveStores);
                        newLearners.removeAll(voters);
                        learners.clear();
                        learners.addAll(newLearners);
                    }
                    List<String> candidates = landscape.entrySet().stream()
                        .filter(e -> !learners.contains(e.getKey()) && !voters.contains(e.getKey()))
                        .sorted(Comparator.comparingInt(e -> e.getValue().getRangesCount()))
                        .map(Map.Entry::getKey)
                        .toList();
                    for (String s : candidates) {
                        voters.add(s);
                        if (voters.size() == targetVoters) {
                            break;
                        }
                    }
                } else { // voters.size() > targetVoters
                    List<String> overloaded = landscape.entrySet().stream()
                        .sorted((a, b) -> b.getValue().getRangesCount() - a.getValue().getRangesCount())
                        .map(Map.Entry::getKey)
                        .toList();
                    for (String s : overloaded) {
                        if (!s.equals(leaderStore) && voters.contains(s)) {
                            voters.remove(s);
                            if (voters.size() == targetVoters) {
                                break;
                            }
                        }
                    }
                    if (voters.size() > targetVoters) {
                        for (String s : new ArrayList<>(voters)) {
                            if (!s.equals(leaderStore)) {
                                voters.remove(s);
                                if (voters.size() == targetVoters) {
                                    break;
                                }
                            }
                        }
                    }
                }
                expectedRangeLayout.put(boundary, buildConfig(voters, learners));
                meetingGoal = true;
            } else {
                expectedRangeLayout.put(boundary, buildConfig(voters, learners));
            }
        }

        if (meetingGoal) {
            return true;
        }

        for (Map.Entry<Boundary, RangeLeader> entry : effectiveRoute.leaderRanges().entrySet()) {
            RangeLeader rangeLeader = entry.getValue();
            KVRangeDescriptor rangeDescriptor = rangeLeader.descriptor();
            ClusterConfig clusterConfig = rangeDescriptor.getConfig();

            Set<String> voters = new HashSet<>(clusterConfig.getVotersList());
            Set<String> learners = new HashSet<>(clusterConfig.getLearnersList());
            sanitize(voters, liveStores);
            sanitize(learners, liveStores);

            boolean changed = false;

            if (expectedLearners == -1) {
                // learners = live - voters
                Set<String> newLearners = new HashSet<>(liveStores);
                newLearners.removeAll(voters);
                if (!newLearners.equals(learners)) {
                    learners = newLearners;
                    changed = true;
                }
            } else {
                int maxPossible = Math.max(0, liveStores.size() - voters.size());
                int targetLearners = Math.min(expectedLearners, maxPossible);

                if (learners.size() < targetLearners) {
                    List<String> candidates = landscape.entrySet().stream()
                        .sorted(Comparator.comparingInt(e -> e.getValue().getRangesCount()))
                        .map(Map.Entry::getKey)
                        .toList();
                    for (String s : candidates) {
                        if (!voters.contains(s) && !learners.contains(s)) {
                            learners.add(s);
                            if (learners.size() == targetLearners) {
                                break;
                            }
                        }
                    }
                    changed = true;
                } else if (learners.size() > targetLearners) {
                    List<String> overloaded = landscape.entrySet().stream()
                        .sorted((a, b) -> b.getValue().getRangesCount() - a.getValue().getRangesCount())
                        .map(Map.Entry::getKey)
                        .toList();
                    for (String s : overloaded) {
                        if (learners.contains(s)) {
                            learners.remove(s);
                            if (learners.size() == targetLearners) {
                                break;
                            }
                        }
                    }
                    changed = true;
                }
            }

            Boundary boundary = entry.getKey();
            expectedRangeLayout.put(boundary, buildConfig(voters, learners));
            if (!meetingGoal && changed) {
                meetingGoal = true;
            }
        }
        return meetingGoal;
    }

    private boolean balanceVoterCount(Map<String, KVRangeStoreDescriptor> landscape,
                                      EffectiveRoute effectiveRoute,
                                      Map<Boundary, ClusterConfig> expectedRangeLayout) {
        final Set<String> liveStores = landscape.keySet();
        Map<String, Integer> storeVoterCount = new HashMap<>();
        for (Map.Entry<Boundary, RangeLeader> entry : effectiveRoute.leaderRanges().entrySet()) {
            ClusterConfig config = entry.getValue().descriptor().getConfig();
            config.getVotersList().stream()
                .filter(liveStores::contains)
                .forEach(storeId -> storeVoterCount.put(storeId, storeVoterCount.getOrDefault(storeId, 0) + 1));
        }
        liveStores.forEach(s -> storeVoterCount.putIfAbsent(s, 0));

        record StoreVoterCount(String storeId, int voterCount) {}

        SortedSet<StoreVoterCount> storeVoterCountSorted = new TreeSet<>(
            Comparator.comparingInt(StoreVoterCount::voterCount).thenComparing(StoreVoterCount::storeId));
        storeVoterCount.forEach(
            (storeId, voterCount) -> storeVoterCountSorted.add(new StoreVoterCount(storeId, voterCount)));

        double totalVoters = storeVoterCount.values().stream().mapToInt(Integer::intValue).sum();
        double targetVotersPerStore = liveStores.isEmpty() ? 0 : totalVoters / liveStores.size();
        int minVotersPerStore = (int) Math.floor(targetVotersPerStore);

        int globalMax = storeVoterCount.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        int globalMin = storeVoterCount.values().stream().mapToInt(Integer::intValue).min().orElse(0);
        if (globalMax - globalMin <= 1) {
            return false;
        }

        boolean meetingGoal = false;
        for (Map.Entry<Boundary, RangeLeader> entry : effectiveRoute.leaderRanges().entrySet()) {
            Boundary boundary = entry.getKey();
            RangeLeader lr = entry.getValue();
            ClusterConfig cc = lr.descriptor().getConfig();

            Set<String> learners = Sets.newHashSet(cc.getLearnersList());
            SortedSet<String> voterSorted = Sets.newTreeSet(cc.getVotersList());
            sanitize(learners, liveStores);
            voterSorted.retainAll(liveStores);

            if (!meetingGoal) {
                meet:
                for (String voter : new ArrayList<>(voterSorted)) {
                    int voters = storeVoterCount.getOrDefault(voter, 0);
                    if (voters == globalMax) {
                        for (StoreVoterCount under : storeVoterCountSorted) {
                            if (storeVoterCount.getOrDefault(under.storeId, 0) <= minVotersPerStore
                                && !voterSorted.contains(under.storeId)
                                && !learners.contains(under.storeId)) {
                                // move voter -> underloaded
                                Set<String> newVoters = new HashSet<>(voterSorted);
                                newVoters.remove(voter);
                                newVoters.add(under.storeId);

                                expectedRangeLayout.put(boundary, buildConfig(newVoters, learners));
                                meetingGoal = true;
                                break meet;
                            }
                        }
                    }
                }
                if (!meetingGoal) {
                    expectedRangeLayout.put(boundary, buildConfig(voterSorted, learners));
                }
            } else {
                expectedRangeLayout.put(boundary, buildConfig(voterSorted, learners));
            }
        }
        if (!meetingGoal) {
            expectedRangeLayout.clear();
        }
        return meetingGoal;
    }

    private void balanceLearnerCount(Map<String, KVRangeStoreDescriptor> landscape,
                                     EffectiveRoute effectiveRoute,
                                     Map<Boundary, ClusterConfig> expectedRangeLayout) {
        final Set<String> liveStores = landscape.keySet();

        Map<String, Integer> storeLearnerCount = new HashMap<>();
        for (Map.Entry<Boundary, RangeLeader> entry : effectiveRoute.leaderRanges().entrySet()) {
            ClusterConfig config = entry.getValue().descriptor().getConfig();
            config.getLearnersList().stream()
                .filter(liveStores::contains)
                .forEach(storeId -> storeLearnerCount.put(storeId, storeLearnerCount.getOrDefault(storeId, 0) + 1));
        }
        liveStores.forEach(s -> storeLearnerCount.putIfAbsent(s, 0));

        record StoreLearnerCount(String storeId, int learnerCount) {}

        SortedSet<StoreLearnerCount> storeLearnerCountSorted = new TreeSet<>(
            Comparator.comparingInt(StoreLearnerCount::learnerCount).thenComparing(StoreLearnerCount::storeId));
        storeLearnerCount.forEach((id, c) -> storeLearnerCountSorted.add(new StoreLearnerCount(id, c)));

        double totalLearners = storeLearnerCount.values().stream().mapToInt(Integer::intValue).sum();
        double targetLearnersPerStore = liveStores.isEmpty() ? 0 : totalLearners / liveStores.size();
        int minLearnersPerStore = (int) Math.floor(targetLearnersPerStore);

        int globalMax = storeLearnerCount.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        int globalMin = storeLearnerCount.values().stream().mapToInt(Integer::intValue).min().orElse(0);
        if (globalMax - globalMin <= 1) {
            return;
        }

        boolean meetingGoal = false;
        for (Map.Entry<Boundary, RangeLeader> entry : effectiveRoute.leaderRanges().entrySet()) {
            Boundary boundary = entry.getKey();
            RangeLeader lr = entry.getValue();
            ClusterConfig cc = lr.descriptor().getConfig();

            Set<String> voters = Sets.newHashSet(cc.getVotersList());
            SortedSet<String> learnerSorted = Sets.newTreeSet(cc.getLearnersList());
            sanitize(voters, liveStores);
            learnerSorted.retainAll(liveStores);

            if (!meetingGoal) {
                meet:
                for (String learner : new ArrayList<>(learnerSorted)) {
                    int learners = storeLearnerCount.getOrDefault(learner, 0);
                    if (learners == globalMax) {
                        for (StoreLearnerCount under : storeLearnerCountSorted) {
                            if (storeLearnerCount.getOrDefault(under.storeId, 0) < minLearnersPerStore
                                && !voters.contains(under.storeId)
                                && !learnerSorted.contains(under.storeId)) {
                                Set<String> newLearners = new HashSet<>(learnerSorted);
                                newLearners.remove(learner);
                                newLearners.add(under.storeId);

                                expectedRangeLayout.put(boundary, buildConfig(voters, newLearners));
                                meetingGoal = true;
                                break meet;
                            }
                        }
                    }
                }
                if (!meetingGoal) {
                    expectedRangeLayout.put(boundary, buildConfig(voters, learnerSorted));
                }
            } else {
                expectedRangeLayout.put(boundary, buildConfig(voters, learnerSorted));
            }
        }
        if (!meetingGoal) {
            expectedRangeLayout.clear();
        }
    }
}
