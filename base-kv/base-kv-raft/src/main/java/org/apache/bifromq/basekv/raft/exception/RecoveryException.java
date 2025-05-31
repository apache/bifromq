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

package org.apache.bifromq.basekv.raft.exception;

public class RecoveryException extends RuntimeException {
    private RecoveryException(String message) {
        super(message);
    }

    public static RecoveryException notLostQuorum() {
        return new NotLostQuorumException();
    }

    public static RecoveryException abort() {
        return new AbortException();
    }

    public static RecoveryException notVoter() {
        return new NotVoterException();
    }

    public static RecoveryException notQualify() {
        return new NotQualifyException();
    }

    public static RecoveryException recoveryInProgress() {
        return new RecoveryInProgressException();
    }

    public static RecoveryException cancelled() {
        return new CancelledException();
    }

    public static class NotLostQuorumException extends RecoveryException {
        private NotLostQuorumException() {
            super("Not lost quorum");
        }
    }

    public static class AbortException extends RecoveryException {
        private AbortException() {
            super("Aborted");
        }
    }

    public static class NotVoterException extends RecoveryException {
        private NotVoterException() {
            super("Not voter");
        }
    }

    public static class NotQualifyException extends RecoveryException {
        private NotQualifyException() {
            super("Not qualify");
        }
    }

    public static class RecoveryInProgressException extends RecoveryException {
        private RecoveryInProgressException() {
            super("There is recovery in progress");
        }
    }

    public static class CancelledException extends RecoveryException {
        private CancelledException() {
            super("Cancelled");
        }
    }
}
