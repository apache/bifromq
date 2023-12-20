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

package com.baidu.bifromq.basecrdt.core.benchmark;

import static com.baidu.bifromq.basecrdt.core.api.CRDTURI.toURI;
import static com.baidu.bifromq.basecrdt.core.api.CausalCRDTType.ormap;

import com.baidu.bifromq.basecrdt.core.api.CCounterOperation;
import com.baidu.bifromq.basecrdt.core.api.ICCounter;
import com.baidu.bifromq.basecrdt.core.api.IORMap;
import com.baidu.bifromq.basecrdt.proto.Replica;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@Slf4j
@State(Scope.Group)
public class CCounterMapBenchmark extends CRDTBenchmarkTemplate {
    Replica replica;
    private ArrayList<ICCounter> counters = new ArrayList<>();

    @Override
    protected void doSetup() {
        replica = engine.host(toURI(ormap, "cctr"));
        Optional<IORMap> counterMap = engine.get(replica.getUri());
        for (int i = 0; i < 1000; i++) {
            counters.add(counterMap.get().getCCounter(ByteString.copyFromUtf8("c-" + i)));
        }
    }

    @Benchmark
    @Group("ReadWrite")
    @GroupThreads(1)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void add() {
        counters.get(ThreadLocalRandom.current().nextInt(1000)).execute(CCounterOperation.add(1));
    }

    @Benchmark
    @Group("ReadWrite")
    @GroupThreads(1)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void zerout() {
        counters.get(ThreadLocalRandom.current().nextInt(1000)).execute(CCounterOperation.zeroOut());
    }

    @Benchmark
    @Group("ReadWrite")
    @GroupThreads(1)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void read() {
        counters.get(ThreadLocalRandom.current().nextInt(1000)).read();
    }
}
