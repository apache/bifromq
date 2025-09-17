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

package org.apache.bifromq.dist.worker.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.dist.worker.TopicIndex;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class TopicIndexBench {

    @Param({"1000", "10000"})
    int deviceCount;

    @Param({"16"})
    int regionCount;

    @Param({"16"})
    int groupCount;

    TopicIndex<String> index;
    List<String> devices;
    List<String> regions;
    List<String> groups;

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(TopicIndexBench.class.getSimpleName())
            .warmupIterations(5)
            .measurementIterations(10)
            .threads(Math.max(2, Runtime.getRuntime().availableProcessors()))
            .forks(1)
            .shouldDoGC(true)
            .build();
        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        index = new TopicIndex<>();
        devices = new ArrayList<>(deviceCount);
        regions = new ArrayList<>(regionCount);
        groups = new ArrayList<>(groupCount);

        for (int i = 0; i < deviceCount; i++) {
            devices.add("dev" + i);
        }
        for (int i = 0; i < regionCount; i++) {
            regions.add("r" + i);
        }
        for (int i = 0; i < groupCount; i++) {
            groups.add("g" + i);
        }

        int preload = deviceCount / 2;
        for (int i = 0; i < preload; i++) {
            String t = topic(devices.get(i), regions.get(i % regionCount), groups.get(i % groupCount));
            index.add(t, "v" + i);
        }
    }

    private String topic(String dev, String region, String group) {
        return "$iot/" + dev + "/user/up/" + region + "/" + group;
    }

    private String randDevice() {
        return devices.get(ThreadLocalRandom.current().nextInt(devices.size()));
    }

    private String randRegion() {
        return regions.get(ThreadLocalRandom.current().nextInt(regions.size()));
    }

    private String randGroup() {
        return groups.get(ThreadLocalRandom.current().nextInt(groups.size()));
    }

    @Benchmark
    public void subAndUnsubOnce(Blackhole bh) {
        String dev = randDevice();
        String t = topic(dev, randRegion(), randGroup());
        String v = "x";
        index.add(t, v);
        index.remove(t, v);
        bh.consume(dev);
    }

    @Benchmark
    public void getExact(Blackhole bh) {
        String t = topic(randDevice(), randRegion(), randGroup());
        bh.consume(index.get(t));
    }

    @Benchmark
    public void matchWildcardRegion(Blackhole bh) {
        String filter = "$iot/+/" + "user/up/" + randRegion() + "/" + randGroup();
        bh.consume(index.match(filter));
    }

    @Benchmark
    public void matchMultiWildcardTail(Blackhole bh) {
        String filter = "$iot/" + randDevice() + "/#";
        bh.consume(index.match(filter));
    }
}