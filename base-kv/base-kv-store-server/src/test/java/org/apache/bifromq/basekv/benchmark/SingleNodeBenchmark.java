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

package org.apache.bifromq.basekv.benchmark;

import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.awaitility.Awaitility.await;

import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.KVRangeConfig;
import org.apache.bifromq.basekv.store.KVRangeStoreTestCluster;
import org.apache.bifromq.basekv.store.option.KVRangeStoreOptions;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3)
@Measurement(iterations = 100, time = 5)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class SingleNodeBenchmark {
    protected KVRangeStoreTestCluster cluster;
    private int count = 3;
    private KVRangeStoreOptions options = new KVRangeStoreOptions();
    private List<KVRangeId> ranges;

    @Setup
    public void setup() {
        log.info("Starting test cluster");
        options.getKvRangeOptions().getWalRaftConfig()
            .setAsyncAppend(false)
            .setMaxUncommittedProposals(Integer.MAX_VALUE);
        cluster = new KVRangeStoreTestCluster(options);
        String store0 = cluster.bootstrapStore();
        KVRangeId rangeId = cluster.genesisKVRangeId();
        long start = System.currentTimeMillis();
        cluster.awaitKVRangeReady(store0, rangeId);
        log.info("KVRange ready in {}ms: kvRangeId={}", System.currentTimeMillis() - start,
            KVRangeIdUtil.toString(rangeId));
        KVRangeConfig rangeSettings = cluster.awaitAllKVRangeReady(rangeId, 0, 40);
        cluster.split(store0, rangeSettings.ver, rangeId, ByteString.copyFromUtf8("Key1")).toCompletableFuture()
            .join();
        await().atMost(Duration.ofSeconds(10)).until(() -> cluster.allKVRangeIds().size() == 2);
        ranges = Lists.newArrayList(cluster.allKVRangeIds());
        for (KVRangeId r : ranges) {
            cluster.awaitKVRangeReady(store0, r);
        }
    }

    @TearDown
    public void teardown() {
        if (cluster != null) {
            log.info("Shutting down test cluster");
            cluster.shutdown();
        }
    }

    @Benchmark
    @Group("WriteOnly")
    @GroupThreads(20)
    public void putRange0() {
        cluster.put(cluster.bootstrapStore(),
            1,
            ranges.get(0),
            copyFromUtf8("key0" + count),
            copyFromUtf8("value" + count)).toCompletableFuture().join();
        count++;
    }

    @Benchmark
    @Group("WriteOnly")
    @GroupThreads(20)
    public void putRange1() {
        cluster.put(cluster.bootstrapStore(),
            1,
            ranges.get(1),
            copyFromUtf8("key1" + count),
            copyFromUtf8("value" + count)).toCompletableFuture().join();
        count++;
    }

    public static void main(String[] args) {
        Options opt = new OptionsBuilder()
            .include(SingleNodeBenchmark.class.getSimpleName())
            .forks(1)
            .build();
        try {
            new Runner(opt).run();
        } catch (RunnerException e) {
            System.out.println(e);
        }
    }
}
