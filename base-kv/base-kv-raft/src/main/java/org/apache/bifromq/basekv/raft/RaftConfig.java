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

package org.apache.bifromq.basekv.raft;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Raft configuration.
 */
@Builder(toBuilder = true)
@Accessors(chain = true)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class RaftConfig {
    @Builder.Default
    private int electionTimeoutTick = 10;
    @Builder.Default
    private int heartbeatTimeoutTick = 1;
    @Builder.Default
    private int installSnapshotTimeoutTick = 2000;
    @Builder.Default
    private long maxSizePerAppend = 1024;
    // max inflight appends during replicating
    @Builder.Default
    private int maxInflightAppends = 1024;
    // the max number of uncommitted proposals before rejection
    @Builder.Default
    private int maxUncommittedProposals = 1024;
    @Builder.Default
    private boolean preVote = true;
    @Builder.Default
    private boolean readOnlyLeaderLeaseMode = true;
    @Builder.Default
    private int readOnlyBatch = 10;
    @Builder.Default
    private boolean disableForwardProposal = false;
    // if append log entries asynchronously which is an optimization described in $10.2.1 section of raft thesis
    @Builder.Default
    private boolean asyncAppend = true;
}
