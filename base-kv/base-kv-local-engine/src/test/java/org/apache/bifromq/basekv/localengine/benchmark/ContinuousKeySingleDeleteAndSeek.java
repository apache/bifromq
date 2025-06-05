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

package org.apache.bifromq.basekv.localengine.benchmark;


import static org.apache.bifromq.basekv.localengine.TestUtil.toByteString;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@Slf4j
public class ContinuousKeySingleDeleteAndSeek {
    @SneakyThrows
    public static void main(String[] args) {
        Options opt = new OptionsBuilder()
            .include(ContinuousKeySingleDeleteAndSeek.class.getSimpleName())
            .build();
        new Runner(opt).run();
    }

    @Benchmark
    @Warmup(iterations = 3)
    @Measurement(iterations = 8)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void iterator(ContinuousKeySingleDeleteAndSeekState state, Blackhole bl) {
//        IKVEngineIterator itr = kvEngine.newIterator(DEFAULT_NS, key.concat(toByteString(state.i)),
//                key.concat(toByteString(state.i + 1)));
//        try (IKVEngineIterator itr = state.kvEngine.newIterator(DEFAULT_NS)) {
        state.itr.refresh();
//        state.itr.seekToLast();
        state.itr.seek(state.key.concat(toByteString(ThreadLocalRandom.current().nextInt(state.keyCount))));
        bl.consume(state.itr.isValid());
//    }

    }
}
