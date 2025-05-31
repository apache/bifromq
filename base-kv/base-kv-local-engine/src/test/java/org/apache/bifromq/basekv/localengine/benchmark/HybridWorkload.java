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

package org.apache.bifromq.basekv.localengine.benchmark;

import lombok.SneakyThrows;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class HybridWorkload {
    @SneakyThrows
    public static void main(String[] args) {
        Options opt = new OptionsBuilder()
            .include(HybridWorkload.class.getSimpleName())
            .warmupIterations(3)
            .measurementIterations(8)
            .forks(1)
            .build();
        new Runner(opt).run();
    }

    @Benchmark
    @Group("Hybrid")
    @BenchmarkMode(Mode.Throughput)
    @GroupThreads(4)
    public void randomPut(HybridWorkloadState state) {
        state.randomPut();
    }

    @Benchmark
    @Group("Hybrid")
    @BenchmarkMode(Mode.Throughput)
    @GroupThreads(4)
    public void randomDelete(HybridWorkloadState state) {
        state.randomDelete();
    }

    @Benchmark
    @Group("Hybrid")
    @BenchmarkMode(Mode.Throughput)
    @GroupThreads(4)
    public void randomPutAndDelete(HybridWorkloadState state) {
        state.randomPutAndDelete();
    }

    @Benchmark
    @Group("Hybrid")
    @BenchmarkMode(Mode.Throughput)
    @GroupThreads(2)
    public void randomGet(HybridWorkloadState state, Blackhole blackhole) {
        blackhole.consume(state.randomGet());
    }

    @Benchmark
    @Group("Hybrid")
    @GroupThreads(3)
    @BenchmarkMode(Mode.Throughput)
    public void seekToFirst(HybridWorkloadState state) {
        state.seekToFirst();
    }

    @Benchmark
    @Group("Hybrid")
    @GroupThreads(3)
    @BenchmarkMode(Mode.Throughput)
    public void randomSeek(HybridWorkloadState state) {
        state.randomSeek();
    }
}
