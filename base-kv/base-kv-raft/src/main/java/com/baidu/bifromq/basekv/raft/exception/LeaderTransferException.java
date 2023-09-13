/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.basekv.raft.exception;

public class LeaderTransferException extends RuntimeException {
    public static LeaderNotReadyException leaderNotReady() {
        return new LeaderNotReadyException();
    }

    public static TransferringInProgressException transferringInProgress() {
        return new TransferringInProgressException();
    }

    public static SelfTransferException selfTransfer() {
        return new SelfTransferException();
    }

    public static StepDownByOtherException stepDownByOther() {
        return new StepDownByOtherException();
    }

    public static NotFoundOrQualifiedException notFoundOrQualified() {
        return new NotFoundOrQualifiedException();
    }

    public static TransferTimeoutException transferTimeout() {
        return new TransferTimeoutException();
    }

    public static NotLeaderException notLeader() {
        return new NotLeaderException();
    }

    public static NoLeaderException noLeader() {
        return new NoLeaderException();
    }

    protected LeaderTransferException(String message) {
        super(message);
    }

    public static class LeaderNotReadyException extends LeaderTransferException {
        private LeaderNotReadyException() {
            super("Leader has not been ready due to commit index of its term has not been confirmed");
        }
    }

    public static class TransferringInProgressException extends LeaderTransferException {
        private TransferringInProgressException() {
            super("There is transferring in progress");
        }
    }

    public static class SelfTransferException extends LeaderTransferException {
        private SelfTransferException() {
            super("Cannot transfer to self");
        }
    }

    public static class StepDownByOtherException extends LeaderTransferException {
        private StepDownByOtherException() {
            super("Step down by another candidate");
        }
    }

    public static class NotFoundOrQualifiedException extends LeaderTransferException {
        private NotFoundOrQualifiedException() {
            super("Transferee not found or not qualified");
        }
    }

    public static class TransferTimeoutException extends LeaderTransferException {
        private TransferTimeoutException() {
            super("Cannot finish transfer within one election timeout");
        }
    }

    public static class NotLeaderException extends LeaderTransferException {
        private NotLeaderException() {
            super("Only leader can do transfer");
        }
    }

    public static class NoLeaderException extends LeaderTransferException {
        private NoLeaderException() {
            super("No leader elected");
        }
    }
}
