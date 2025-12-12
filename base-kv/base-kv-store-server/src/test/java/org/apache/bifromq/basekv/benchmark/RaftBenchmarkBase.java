/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.basekv.benchmark;

import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.raft.RaftConfig;
import org.openjdk.jmh.runner.RunnerException;

/**
 * Base class for Raft consensus algorithm benchmarks.
 * Provides common infrastructure for testing Raft protocol performance including
 * leader election, log replication, and consensus mechanisms.
 */
@Slf4j
public abstract class RaftBenchmarkBase {
    
    /**
     * Default Raft configuration for benchmarks.
     * Configured for optimal performance testing:
     * - Higher election timeout to avoid unnecessary elections
     * - Asynchronous append operations for realistic scenarios
     * - Large uncommitted proposal queue to test throughput
     */
    protected static final RaftConfig DEFAULT_RAFT_CONFIG = RaftConfig.builder()
        .electionTimeoutTick(100)
        .heartbeatTimeoutTick(1)
        .installSnapshotTimeoutTick(5000)
        .maxInflightAppends(1024)
        .maxUncommittedProposals(Integer.MAX_VALUE)
        .preVote(true)
        .readOnlyLeaderLeaseMode(true)
        .asyncAppend(true)
        .build();

    /**
     * Setup method called before each benchmark iteration.
     * Subclasses should override this to initialize their state.
     */
    protected abstract void doSetup();

    /**
     * Teardown method called after each benchmark iteration.
     * Subclasses should override this to cleanup resources.
     */
    protected abstract void doTearDown();

    /**
     * Get human-readable name for this benchmark.
     */
    protected abstract String getBenchmarkName();

    /**
     * Get description of what this benchmark measures.
     */
    protected abstract String getBenchmarkDescription();

    /**
     * Log benchmark information.
     */
    protected final void logBenchmarkInfo() {
        log.info("\n=== Benchmark: {} ===", getBenchmarkName());
        log.info("Description: {}", getBenchmarkDescription());
        log.info("Raft Config: electionTimeout={}, heartbeatTimeout={}, asyncAppend={}",
            DEFAULT_RAFT_CONFIG.getElectionTimeoutTick(),
            DEFAULT_RAFT_CONFIG.getHeartbeatTimeoutTick(),
            DEFAULT_RAFT_CONFIG.isAsyncAppend());
    }

    /**
     * Utility method to handle RunnerException.
     */
    protected final void handleRunnerException(RunnerException e) {
        log.error("Benchmark failed with exception", e);
        throw new RuntimeException(e);
    }
}
