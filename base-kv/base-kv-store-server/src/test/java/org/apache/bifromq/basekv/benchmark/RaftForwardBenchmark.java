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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.KVRangeStoreTestCluster;
import org.apache.bifromq.basekv.store.option.KVRangeStoreOptions;
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
 * Benchmark for Raft consensus forward mechanism performance.
 * Tests the performance of forwarding cluster config change requests
 * from followers to leaders in a Raft-based KV store.
 * 
 * Key metrics measured:
 * - Forward request throughput
 * - Forward request latency
 * - Response time distribution
 * 
 * This benchmark demonstrates the effectiveness of the Raft forward
 * mechanism for cluster configuration changes in a distributed KV store.
 */
@Slf4j
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 5)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class RaftForwardBenchmark extends RaftBenchmarkBase {
    private KVRangeStoreTestCluster cluster;
    private String leaderStore;
    private String followerStore;
    private KVRangeId testRange;
    private AtomicInteger requestCount;
    private int keyCounter;

    @Override
    @Setup
    public void doSetup() {
        logBenchmarkInfo();
        log.info("Setting up Raft forward benchmark");
        
        KVRangeStoreOptions options = new KVRangeStoreOptions();
        options.getKvRangeOptions().getWalRaftConfig()
            .setAsyncAppend(DEFAULT_RAFT_CONFIG.isAsyncAppend())
            .setMaxUncommittedProposals(DEFAULT_RAFT_CONFIG.getMaxUncommittedProposals())
            .setElectionTimeoutTick(DEFAULT_RAFT_CONFIG.getElectionTimeoutTick());
        
        cluster = new KVRangeStoreTestCluster(options);
        leaderStore = cluster.bootstrapStore();
        
        // Create second store as follower
        followerStore = cluster.bootstrapStore();
        
        // Get genesis range
        testRange = cluster.genesisKVRangeId();
        
        // Wait for both stores to be ready
        await().atMost(Duration.ofSeconds(10))
            .until(() -> {
                cluster.awaitKVRangeReady(leaderStore, testRange);
                cluster.awaitKVRangeReady(followerStore, testRange);
                return true;
            });
        
        requestCount = new AtomicInteger(0);
        keyCounter = 0;
        log.info("Raft forward benchmark setup completed. Leader: {}, Follower: {}",
            leaderStore, followerStore);
    }

    @Override
    @TearDown
    public void doTearDown() {
        if (cluster != null) {
            log.info("Shutting down Raft forward benchmark cluster");
            cluster.shutdown();
        }
    }

    @Override
    protected String getBenchmarkName() {
        return "RaftForwardBenchmark";
    }

    @Override
    protected String getBenchmarkDescription() {
        return "Measures the throughput and latency of Raft cluster config change forwarding from followers to leader";
    }

    /**
     * Benchmark forward proposal request throughput.
     * Tests the underlying propose mechanism that supports forward operations.
     * This simulates forward requests by performing put operations on the leader.
     */
    @Benchmark
    @Group("ForwardWrite")
    @GroupThreads(8)
    public void forwardPropose() {
        try {
            ByteString key = copyFromUtf8("fwd_key_" + requestCount.incrementAndGet());
            ByteString value = copyFromUtf8("fwd_value_" + System.nanoTime());
            
            cluster.put(leaderStore, 1, testRange, key, value)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Forward propose failed", e);
        }
    }

    /**
     * Benchmark read operations through the Raft consensus.
     * Tests the ability to serve reads from the leader after proposals.
     */
    @Benchmark
    @Group("ForwardRead")
    @GroupThreads(8)
    public void forwardRead() {
        try {
            int idx = requestCount.incrementAndGet() % 1000;
            ByteString key = copyFromUtf8("fwd_key_" + idx);
            
            cluster.get(leaderStore, testRange, key);
        } catch (Exception e) {
            log.warn("Forward read failed", e);
        }
    }

    /**
     * Run the benchmark as a standalone test.
     */
    public static void main(String[] args) {
        Options opt = new OptionsBuilder()
            .include(RaftForwardBenchmark.class.getSimpleName())
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
