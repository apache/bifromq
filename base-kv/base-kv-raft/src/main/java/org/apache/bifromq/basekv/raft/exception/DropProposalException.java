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

public abstract class DropProposalException extends RuntimeException {
    public final Code code;

    protected DropProposalException(Code code, String message) {
        super(message);
        this.code = code;
    }

    public static DropProposalException transferringLeader() {
        return new TransferringLeaderException();
    }

    public static DropProposalException throttledByThreshold() {
        return new ThrottleByThresholdException();
    }

    public static DropProposalException noLeader() {
        return new NoLeaderException();
    }

    public static DropProposalException leaderForwardDisabled() {
        return new LeaderForwardDisabledException();
    }

    public static DropProposalException forwardTimeout() {
        return new ForwardTimeoutException();
    }

    public static DropProposalException overridden() {
        return new OverriddenException();
    }

    public static DropProposalException superseded() {
        return new SupersededBySnapshotException();
    }

    public static CancelledException cancelled() {
        return new CancelledException();
    }

    public enum Code {
        TransferringLeader,
        ThrottleByThreshold,
        NoLeader,
        LeaderForwardDisabled,
        ForwardTimeout,
        Overridden,
        SupersededBySnapshot,
        Cancelled
    }

    public static class TransferringLeaderException extends DropProposalException {
        private TransferringLeaderException() {
            super(Code.TransferringLeader, "Proposal dropped due to on-going transferring leader");
        }
    }

    public static class ThrottleByThresholdException extends DropProposalException {
        private ThrottleByThresholdException() {
            super(Code.ThrottleByThreshold, "Proposal dropped due to too many uncommitted logs");
        }
    }

    public static class NoLeaderException extends DropProposalException {
        private NoLeaderException() {
            super(Code.NoLeader, "No leader elected");
        }
    }

    public static class LeaderForwardDisabledException extends DropProposalException {
        private LeaderForwardDisabledException() {
            super(Code.LeaderForwardDisabled, "Proposal forward feature is disabled");
        }
    }

    public static class ForwardTimeoutException extends DropProposalException {
        private ForwardTimeoutException() {
            super(Code.ForwardTimeout, "Doesn't receive propose reply from leader within timeout");
        }
    }

    public static class OverriddenException extends DropProposalException {
        private OverriddenException() {
            super(Code.Overridden, "Proposal dropped due to overridden by proposal from newer leader");
        }
    }

    public static class SupersededBySnapshotException extends DropProposalException {
        private SupersededBySnapshotException() {
            super(Code.SupersededBySnapshot, "Proposal dropped due to superseded by snapshot");
        }
    }

    public static class CancelledException extends DropProposalException {
        private CancelledException() {
            super(Code.Cancelled, "Proposal dropped due to cancelled");
        }
    }
}
