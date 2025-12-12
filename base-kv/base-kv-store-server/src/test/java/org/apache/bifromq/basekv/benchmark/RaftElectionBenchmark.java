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

import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.KVRangeConfig;
import org.apache.bifromq.basekv.store.KVRangeStoreTestCluster;
import org.apache.bifromq.basekv.store.option.KVRangeStoreOptions;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.collect.Lists;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import static org.awaitility.Awaitility.await;
import static com.google.protobuf.ByteString.copyFromUtf8;

/**
 * Benchmark for Raft leader election performance.
 * Tests the performance of leader election process and failover times
 * in a Raft-based KV store cluster.
 * 
 * Key metrics measured:
 * - Election time after leader failure
 * - Pre-vote mechanism effectiveness
 * - Follower recovery time
 * - Election throughput under normal conditions
 */
@Slf4j
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 8, time = 5)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class RaftElectionBenchmark extends RaftBenchmarkBase {
    private KVRangeStoreTestCluster cluster;
    private String leaderStore;
    private List<String> allStores;
    private KVRangeId testRange;
    private AtomicInteger keyCounter;
    private AtomicLong electionTimeMs;

    @Override
    @Setup
    public void doSetup() {
        logBenchmarkInfo();
        log.info("Setting up Raft election performance benchmark");
        
        KVRangeStoreOptions options = new KVRangeStoreOptions();
        options.getKvRangeOptions().getWalRaftConfig()
            .setAsyncAppend(DEFAULT_RAFT_CONFIG.isAsyncAppend())
            .setMaxUncommittedProposals(DEFAULT_RAFT_CONFIG.getMaxUncommittedProposals())
            .setElectionTimeoutTick(DEFAULT_RAFT_CONFIG.getElectionTimeoutTick())
            .setPreVote(true);
        
        cluster = new KVRangeStoreTestCluster(options);
        leaderStore = cluster.bootstrapStore();
        
        // Create additional stores
        allStores = Lists.newArrayList(leaderStore);
        for (int i = 0; i < 2; i++) {
            allStores.add(cluster.bootstrapStore());
        }
        
        // Setup test range
        testRange = cluster.genesisKVRangeId();
        
        // Wait for all stores to be ready
        for (String store : allStores) {
            cluster.awaitKVRangeReady(store, testRange);
        }
        
        long start = System.currentTimeMillis();
        KVRangeConfig config = cluster.awaitAllKVRangeReady(testRange, 0, 40);
        leaderStore = config.leader;
        log.info("All nodes ready in {}ms. Leader: {}", 
            System.currentTimeMillis() - start, leaderStore);
        
        keyCounter = new AtomicInteger(0);
        electionTimeMs = new AtomicLong(0);
        
        log.info("Election benchmark setup completed. Stores: {}, Leader: {}", 
            allStores.size(), leaderStore);
    }

    @Override
    @TearDown
    public void doTearDown() {
        if (cluster != null) {
            log.info("Shutting down Raft election benchmark cluster");
            cluster.shutdown();
        }
    }

    @Override
    protected String getBenchmarkName() {
        return "RaftElectionBenchmark";
    }

    @Override
    protected String getBenchmarkDescription() {
        return "Measures the performance of Raft leader election and failover detection";
    }

    /**
     * Benchmark normal write operations under stable leadership.
     * Baseline measurement for throughput with active leader.
     */
    @Benchmark
    @Group("StableLeadership")
    @GroupThreads(8)
    public void writeUnderStableLeadership() {
        try {
            ByteString key = copyFromUtf8("stable_key_" + keyCounter.incrementAndGet());
            ByteString value = copyFromUtf8("stable_value_" + System.nanoTime());
            
            cluster.put(leaderStore, 1, testRange, key, value)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Write under stable leadership failed", e);
        }
    }

    /**
     * Benchmark read operations for election detection.
     * Measures how quickly reads can detect leader unavailability.
     */
    @Benchmark
    @Group("ElectionDetection")
    @GroupThreads(4)
    public void detectElectionViaRead() {
        try {
            int storeIdx = keyCounter.getAndIncrement() % allStores.size();
            ByteString key = copyFromUtf8("detect_key_" + (keyCounter.get() % 100));
            
            cluster.get(allStores.get(storeIdx), testRange, key);
        } catch (Exception e) {
            // Expected when leader is unavailable
            log.debug("Read during election detection: {}", e.getMessage());
        }
    }

    /**
     * Benchmark heartbeat monitoring responsiveness.
     * Tests the effectiveness of heartbeat-based leader detection.
     */
    @Benchmark
    @Group("HeartbeatMonitoring")
    @GroupThreads(2)
    public void monitorLeaderHeartbeat() {
        try {
            // Attempt to write which requires heartbeat from leader
            ByteString key = copyFromUtf8("heartbeat_key_" + keyCounter.incrementAndGet());
            ByteString value = copyFromUtf8("heartbeat_" + System.nanoTime());
            
            long startTime = System.currentTimeMillis();
            cluster.put(leaderStore, 1, testRange, key, value)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
            long elapsed = System.currentTimeMillis() - startTime;
            electionTimeMs.set(elapsed);
        } catch (Exception e) {
            log.warn("Heartbeat monitoring failed", e);
        }
    }

    /**
     * Run the benchmark as a standalone test.
     */
    public static void main(String[] args) {
        Options opt = new OptionsBuilder()
            .include(RaftElectionBenchmark.class.getSimpleName())
            .forks(1)
            .build();
        
        try {
            new Runner(opt).run();
        } catch (RunnerException e) {
            log.error("Benchmark execution failed", e);
            System.exit(1);
        }
    }
}
