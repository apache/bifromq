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

import org.apache.bifromq.basekv.raft.exception.ReadIndexException;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;

class ReadProgressTracker {
    private final TreeMap<Long, ReadProgress> readProgressMap;
    private final IRaftStateStore stateStorage;
    private final Logger logger;
    private int total;

    ReadProgressTracker(IRaftStateStore stateStorage, Logger logger) {
        this.readProgressMap = new TreeMap<>();
        this.stateStorage = stateStorage;
        this.logger = logger;
    }

    public void add(Long readIndex, CompletableFuture<Long> onDone) {
        readProgressMap.compute(readIndex, (key, value) -> {
            if (value == null) {
                value = new ReadProgress(stateStorage.latestClusterConfig(), logger);
                // vote for local
                value.confirmTracker.poll(stateStorage.local(), true);
            }
            value.add(onDone);
            total++;
            return value;
        });
    }

    public void confirm(long readIndex, String fromPeer) {
        ReadProgress readProgress = readProgressMap.get(readIndex);
        if (readProgress != null) {
            readProgress.confirmTracker.poll(fromPeer, true);
            QuorumTracker.JointVoteResult commitTallyResult = readProgress.confirmTracker.tally();
            if (commitTallyResult.result == QuorumTracker.VoteResult.Won) {
                // complete and remove pending read futures up to the confirmed read index
                while (!readProgressMap.isEmpty()) {
                    Map.Entry<Long, ReadProgress> entry = readProgressMap.pollFirstEntry();
                    entry.getValue().complete(readIndex);
                    total -= entry.getValue().count();
                    if (readIndex == entry.getKey()) {
                        break;
                    }
                }
            }
        }
    }

    public void abort(ReadIndexException e) {
        logger.debug("Abort on-going read progresses");
        readProgressMap.values().forEach(p -> p.abort(e));
        readProgressMap.clear();
        total = 0;
    }

    public long highestReadIndex() {
        if (!readProgressMap.isEmpty()) {
            return readProgressMap.lastKey();
        }
        return 0;
    }

    public int underConfirming() {
        return total;
    }

    private static class ReadProgress {
        private final List<CompletableFuture<Long>> pendingFutures;
        private final QuorumTracker confirmTracker;

        ReadProgress(ClusterConfig clusterConfig, Logger logger) {
            pendingFutures = new ArrayList<>();
            confirmTracker = new QuorumTracker(clusterConfig, logger);
        }

        void add(CompletableFuture<Long> future) {
            pendingFutures.add(future);
        }

        void complete(Long readIndex) {
            pendingFutures.forEach(future -> future.complete(readIndex));
        }

        void abort(ReadIndexException e) {
            pendingFutures.forEach(future -> future.completeExceptionally(e));
        }

        int count() {
            return pendingFutures.size();
        }
    }
}
