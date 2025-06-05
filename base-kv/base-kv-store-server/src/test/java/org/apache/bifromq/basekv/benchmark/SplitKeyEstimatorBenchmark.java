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

package org.apache.bifromq.basekv.benchmark;

import org.apache.bifromq.basekv.store.range.IKVLoadRecorder;
import org.apache.bifromq.basekv.store.range.KVLoadRecorder;
import org.apache.bifromq.basekv.store.range.hinter.QueryKVLoadBasedSplitHinter;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@Slf4j
@State(Scope.Benchmark)
public class SplitKeyEstimatorBenchmark {
    private static final int totalKeyCount = 1000000;
    private final QueryKVLoadBasedSplitHinter loadEstimator = new QueryKVLoadBasedSplitHinter(Duration.ofSeconds(5));
    private ByteString[] keys;

    @Setup(Level.Trial)
    public void setup() {
        keys = new ByteString[totalKeyCount];
        for (int i = 0; i < totalKeyCount; i++) {
            keys[i] = ByteString.copyFromUtf8("Key" + i);
        }
    }

    @Benchmark
    @Group("load")
    @BenchmarkMode(Mode.Throughput)
    public void track() {
        IKVLoadRecorder recorder = new KVLoadRecorder();
        ByteString userKey = keys[ThreadLocalRandom.current().nextInt(0, totalKeyCount)];
        recorder.record(userKey, 1);
        loadEstimator.recordQuery(null, recorder.stop());
    }

    @Benchmark
    @Group("load")
    @BenchmarkMode(Mode.Throughput)
    public void estimate(Blackhole blackhole) {
        blackhole.consume(loadEstimator.estimate());
    }

    @SneakyThrows
    public static void main(String[] args) {
        Options opt = new OptionsBuilder()
            .include(SplitKeyEstimatorBenchmark.class.getSimpleName())
            .threads(4)
            .warmupIterations(2)
            .measurementIterations(3)
            .build();
        new Runner(opt).run();
    }
}

