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

package org.apache.bifromq.basekv.raft.exception;

public class ClusterConfigChangeException extends RuntimeException {
    protected ClusterConfigChangeException(String message) {
        super(message);
    }

    public static ConcurrentChangeException concurrentChange() {
        return new ConcurrentChangeException();
    }

    public static ClusterConfigChangeException emptyVoters() {
        return new EmptyVotersException();
    }

    public static ClusterConfigChangeException learnersOverlap() {
        return new LearnersOverlapException();
    }

    public static ClusterConfigChangeException slowLearner() {
        return new SlowLearnerException();
    }

    public static ClusterConfigChangeException leaderStepDown() {
        return new LeaderStepDownException();
    }

    public static ClusterConfigChangeException notLeader() {
        return new NotLeaderException();
    }

    public static ClusterConfigChangeException noLeader() {
        return new NoLeaderException();
    }

    public static ClusterConfigChangeException cancelled() {
        return new CancelledException();
    }

    public static class ConcurrentChangeException extends ClusterConfigChangeException {
        private ConcurrentChangeException() {
            super("Only one on-going change is allowed");
        }
    }

    public static class LeaderStepDownException extends ClusterConfigChangeException {
        private LeaderStepDownException() {
            super("Leader has stepped down");
        }
    }

    public static class EmptyVotersException extends ClusterConfigChangeException {
        private EmptyVotersException() {
            super("Voters can not be empty");
        }
    }

    public static class LearnersOverlapException extends ClusterConfigChangeException {
        private LearnersOverlapException() {
            super("Learners must not overlap voters");
        }
    }

    public static class SlowLearnerException extends ClusterConfigChangeException {
        private SlowLearnerException() {
            super("Some new added servers are too slow to catch up leader's progress");
        }
    }

    public static class NotLeaderException extends ClusterConfigChangeException {

        private NotLeaderException() {
            super("Cluster change can only do via leader");
        }
    }

    public static class NoLeaderException extends ClusterConfigChangeException {
        private NoLeaderException() {
            super("No leader elected");
        }
    }

    public static class CancelledException extends ClusterConfigChangeException {
        private CancelledException() {
            super("Cancelled");
        }
    }
}
