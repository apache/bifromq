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

package org.apache.bifromq.util.index.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.util.index.TopicLevelTrie;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class TopicIndexRemoveBenchmark {

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(TopicIndexRemoveBenchmark.class.getSimpleName())
            .addProfiler(GCProfiler.class)
            .build();
        new Runner(opt).run();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    @Threads(1)
    @Fork(1)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void remove(BenchState shared, ThreadState thread, Blackhole bh) {
        thread.pickTargets(shared.keys);
        shared.trie.removePath(thread.currentPath, shared.value);
        bh.consume(1);
    }

    @State(Scope.Benchmark)
    public static class BenchState {

        final String value = "v";
        final List<String> keys = new ArrayList<>();
        @Param({"30000", "300000", "3000000"})
        public int keyCount;

        TestTrie trie;

        @Setup(Level.Trial)
        public void setup() {
            trie = new TestTrie();

            for (int g = 0; g < keyCount; g++) {
                String deviceKey = UUID.randomUUID().toString();

                keys.add(deviceKey);
                trie.addPath(toTopic(deviceKey), value);
            }
        }

        private List<String> toTopic(String deviceKey) {
            // Remove: "a/<key>/b/c/<hash>/#"
            return List.of("a", deviceKey, "b", "c", Integer.toString(Math.abs(deviceKey.hashCode()) % 65535), "#");
        }

    }

    @State(Scope.Thread)
    public static class ThreadState {
        List<String> currentPath;

        void pickTargets(List<String> keys) {
            ThreadLocalRandom r = ThreadLocalRandom.current();
            String deviceKey = keys.get(r.nextInt(keys.size()));
            currentPath = List.of("a", deviceKey, "b", "c",
                Integer.toString(Math.abs(deviceKey.hashCode()) % 65535), "#");
        }

        @TearDown(Level.Invocation)
        public void restore(BenchState shared) {
            shared.trie.addPath(currentPath, shared.value);
        }
    }

    private static final class TestTrie extends TopicLevelTrie<String> {
        void addPath(List<String> topicLevels, String value) {
            add(topicLevels, value);
        }

        void removePath(List<String> topicLevels, String value) {
            remove(topicLevels, value);
        }
    }
}
