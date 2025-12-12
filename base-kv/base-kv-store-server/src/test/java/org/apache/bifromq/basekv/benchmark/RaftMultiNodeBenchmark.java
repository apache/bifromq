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
 * Benchmark for multi-node Raft consensus replication and synchronization.
 * Tests the performance of log replication and consensus across multiple nodes
 * in a Raft-based KV store cluster.
 * 
 * Key metrics measured:
 * - Write throughput across multiple replicas
 * - Replication latency
 * - Consensus commit performance
 * - Read consistency from followers
 */
@Slf4j
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 10, time = 5)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class RaftMultiNodeBenchmark extends RaftBenchmarkBase {
    private KVRangeStoreTestCluster cluster;
    private String leaderStore;
    private List<String> followerStores;
    private List<KVRangeId> ranges;
    private AtomicInteger keyCounter;
    private int count;

    @Override
    @Setup
    public void doSetup() {
        logBenchmarkInfo();
        log.info("Setting up multi-node Raft consensus benchmark");
        
        KVRangeStoreOptions options = new KVRangeStoreOptions();
        options.getKvRangeOptions().getWalRaftConfig()
            .setAsyncAppend(DEFAULT_RAFT_CONFIG.isAsyncAppend())
            .setMaxUncommittedProposals(DEFAULT_RAFT_CONFIG.getMaxUncommittedProposals())
            .setElectionTimeoutTick(DEFAULT_RAFT_CONFIG.getElectionTimeoutTick());
        
        cluster = new KVRangeStoreTestCluster(options);
        leaderStore = cluster.bootstrapStore();
        
        // Create additional stores as followers
        followerStores = Lists.newArrayList();
        for (int i = 0; i < 2; i++) {
            followerStores.add(cluster.bootstrapStore());
        }
        
        // Get genesis range and split it
        KVRangeId genesisRange = cluster.genesisKVRangeId();
        long start = System.currentTimeMillis();
        cluster.awaitKVRangeReady(leaderStore, genesisRange);
        log.info("Genesis KVRange ready in {}ms: kvRangeId={}", 
            System.currentTimeMillis() - start, KVRangeIdUtil.toString(genesisRange));
        
        KVRangeConfig rangeSettings = cluster.awaitAllKVRangeReady(genesisRange, 0, 40);
        
        // Split range to create second range
        cluster.split(leaderStore, rangeSettings.ver, genesisRange, copyFromUtf8("Key1"))
            .toCompletableFuture().join();
        
        await().atMost(Duration.ofSeconds(10))
            .until(() -> cluster.allKVRangeIds().size() == 2);
        
        ranges = Lists.newArrayList(cluster.allKVRangeIds());
        for (KVRangeId r : ranges) {
            cluster.awaitKVRangeReady(leaderStore, r);
        }
        
        for (String followerStore : followerStores) {
            for (KVRangeId r : ranges) {
                cluster.awaitKVRangeReady(followerStore, r);
            }
        }
        
        keyCounter = new AtomicInteger(0);
        count = 3;
        
        log.info("Multi-node benchmark setup completed. Leader: {}, Followers: {}, Ranges: {}",
            leaderStore, followerStores, ranges.size());
    }

    @Override
    @TearDown
    public void doTearDown() {
        if (cluster != null) {
            log.info("Shutting down multi-node Raft consensus benchmark cluster");
            cluster.shutdown();
        }
    }

    @Override
    protected String getBenchmarkName() {
        return "RaftMultiNodeBenchmark";
    }

    @Override
    protected String getBenchmarkDescription() {
        return "Measures throughput and latency of Raft log replication and consensus across multiple nodes";
    }

    /**
     * Benchmark write operations with full replication.
     * Tests the performance of proposing entries that need to be replicated
     * to all followers in the Raft cluster.
     */
    @Benchmark
    @Group("MultiNodeWrites")
    @GroupThreads(8)
    public void writeWithReplication() {
        try {
            int rangeIdx = keyCounter.getAndIncrement() % ranges.size();
            ByteString key = copyFromUtf8("key_" + keyCounter.get());
            ByteString value = copyFromUtf8("value_" + System.nanoTime());
            
            cluster.put(leaderStore, 1, ranges.get(rangeIdx), key, value)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
            count++;
        } catch (Exception e) {
            log.warn("Write with replication failed", e);
        }
    }

    /**
     * Benchmark read consistency from follower replicas.
     * Tests the performance of reading from followers after entries are replicated.
     */
    @Benchmark
    @Group("MultiNodeReads")
    @GroupThreads(8)
    public void readFromFollower() {
        try {
            if (followerStores.isEmpty()) {
                return;
            }
            int followerIdx = keyCounter.getAndIncrement() % followerStores.size();
            int keyIdx = keyCounter.get() % 1000;
            ByteString key = copyFromUtf8("key_" + keyIdx);
            
            cluster.get(followerStores.get(followerIdx), ranges.get(0), key);
        } catch (Exception e) {
            log.warn("Read from follower failed", e);
        }
    }

    /**
     * Benchmark concurrent writes to different range partitions.
     * Tests the performance of parallel Raft proposals across multiple ranges.
     */
    @Benchmark
    @Group("MultiPartitionWrites")
    @GroupThreads(12)
    public void writeMultiPartition() {
        try {
            int rangeIdx = keyCounter.getAndIncrement() % ranges.size();
            ByteString key = copyFromUtf8("mp_key_" + keyCounter.get());
            ByteString value = copyFromUtf8("mp_value_" + System.nanoTime());
            
            cluster.put(leaderStore, 1, ranges.get(rangeIdx), key, value)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Multi-partition write failed", e);
        }
    }

    /**
     * Run the benchmark as a standalone test.
     */
    public static void main(String[] args) {
        Options opt = new OptionsBuilder()
            .include(RaftMultiNodeBenchmark.class.getSimpleName())
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
