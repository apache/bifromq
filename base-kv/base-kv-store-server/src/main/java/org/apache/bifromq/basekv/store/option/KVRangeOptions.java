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

package org.apache.bifromq.basekv.store.option;

import org.apache.bifromq.basekv.raft.RaftConfig;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain = true)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(toBuilder = true)
public class KVRangeOptions {
    @Builder.Default
    private boolean enableLoadEstimation = false;
    @Builder.Default
    private int snapshotSyncBytesPerSec = 128 * 1024 * 1024; // 128MB
    @Builder.Default
    private int compactWALThreshold = 10000; // the max number of logs before compaction
    @Builder.Default
    private int shrinkWALCheckIntervalSec = 60;
    @Builder.Default
    private long tickUnitInMS = 100;
    @Builder.Default
    private int maxWALFatchBatchSize = 5 * 1024 * 1024; // 5MB
    @Builder.Default
    private int snapshotSyncIdleTimeoutSec = 600; // 10min
    @Builder.Default
    private int statsCollectIntervalSec = 5;
    @Builder.Default
    private int zombieTimeoutSec = 60; // 1min
    @Builder.Default
    private RaftConfig walRaftConfig = new RaftConfig()
        .setPreVote(true)
        .setHeartbeatTimeoutTick(5) // 500ms
        .setInstallSnapshotTimeoutTick(6000) // 10min
        .setElectionTimeoutTick(30) // 3s
        .setMaxSizePerAppend(100 * 1024 * 1024); // 100MB;
}
