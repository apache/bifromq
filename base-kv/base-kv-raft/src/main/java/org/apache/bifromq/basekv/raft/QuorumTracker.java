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

package org.apache.bifromq.basekv.raft;

import static org.apache.bifromq.basekv.raft.QuorumTracker.VoteResult.Won;

import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;

class QuorumTracker {
    private final Set<String> voterGroupOne = new HashSet<>();
    private final Set<String> voterGroupTwo = new HashSet<>(); // non empty in joint config
    private final Map<String, Boolean> votes = new HashMap<>();
    private final Logger logger;

    QuorumTracker(ClusterConfig clusterConfig, Logger logger) {
        voterGroupOne.addAll(clusterConfig.getVotersList());
        voterGroupTwo.addAll(clusterConfig.getNextVotersList());
        this.logger = logger;
    }

    void refresh(ClusterConfig clusterConfig) {
        logger.debug("Quorum tracker reset to config[v:{},nv:{}]",
            clusterConfig.getVotersList(), clusterConfig.getNextVotersList());
        voterGroupOne.clear();
        voterGroupOne.addAll(clusterConfig.getVotersList());
        voterGroupTwo.clear();
        voterGroupTwo.addAll(clusterConfig.getNextVotersList());
        // only refresh the voter group, don't reset the polls already received and let caller decide.
        // In our case caller wants to leave existing polls intact.
    }

    void reset() {
        votes.clear();
    }

    void poll(String voter, boolean vote) {
        votes.put(voter, vote);
    }

    TallyResult tally(String voter) {
        if (votes.containsKey(voter)) {
            if (votes.get(voter)) {
                return TallyResult.Yes;
            } else {
                return TallyResult.No;
            }
        } else {
            return TallyResult.Miss;
        }
    }

    JointVoteResult tally() {
        VoteGroupResult groupOneResult = tally(voterGroupOne);
        VoteGroupResult groupTwoResult = tally(voterGroupTwo);
        return new JointVoteResult(groupOneResult, groupTwoResult);
    }

    VoteGroupResult tally(Set<String> voters) {
        int miss = 0;
        int yes = 0;
        int no = 0;
        for (String voter : voters) {
            switch (tally(voter)) {
                case Yes:
                    yes++;
                    break;
                case No:
                    no++;
                    break;
                case Miss:
                    miss++;
                    break;
                default: {
                    // never happen
                }
            }
        }
        return new VoteGroupResult(voters.size(), yes, no, miss);
    }

    enum VoteResult {
        Won, Lost, Pending
    }

    enum TallyResult {
        Yes, No, Miss
    }

    static class VoteGroupResult {
        final VoteResult result;
        final int yes;
        final int no;
        final int miss;

        private VoteGroupResult(int voters, int yes, int no, int miss) {
            this.yes = yes;
            this.no = no;
            this.miss = miss;
            if (voters == 0) {
                this.result = Won;
            } else {
                int quorum = voters / 2 + 1;
                if (yes >= quorum) {
                    this.result = Won;
                } else if (yes + miss >= quorum) {
                    this.result = VoteResult.Pending;
                } else {
                    this.result = VoteResult.Lost;
                }
            }
        }

        @Override
        public String toString() {
            return "VoteGroupResult{result=" + result + ", yes=" + yes + ", no=" + no + ", miss=" + miss + '}';
        }
    }

    static class JointVoteResult {
        final VoteResult result;
        final VoteGroupResult groupOneResult;
        final VoteGroupResult groupTwoResult;

        JointVoteResult(VoteGroupResult groupOneResult, VoteGroupResult groupTwoResult) {
            this.groupOneResult = groupOneResult;
            this.groupTwoResult = groupTwoResult;
            if (groupOneResult.result == groupTwoResult.result) {
                this.result = groupOneResult.result;
            } else if (groupOneResult.result == VoteResult.Lost || groupTwoResult.result == VoteResult.Lost) {
                this.result = VoteResult.Lost;
            } else {
                this.result = VoteResult.Pending;
            }
        }

        @Override
        public String toString() {
            return "JointVoteResult{result=" + result + ", groupOneResult=" + groupOneResult + ", groupTwoResult="
                + groupTwoResult + '}';
        }
    }
}
