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

package org.apache.bifromq.basecrdt.core.benchmark;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import org.apache.bifromq.basecrdt.core.internal.CausalCRDTInflaterFactory;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.testng.annotations.Test;

@Slf4j
public abstract class CRDTBenchmarkTemplate {
    protected CausalCRDTInflaterFactory inflaterFactory;

    @Setup
    public void setup() throws IOException {
        inflaterFactory = new CausalCRDTInflaterFactory(
            Duration.ofMillis(200), Duration.ofSeconds(20), Duration.ofMillis(200),
            Executors.newSingleThreadScheduledExecutor());
        doSetup();
    }

    @TearDown
    public void tearDown() {
        log.info("Stop engine");
    }

    protected abstract void doSetup();

    @Test(enabled = false)
    public void test() {
        Options opt = new OptionsBuilder()
            .include(this.getClass().getSimpleName())
            .warmupIterations(0)
            .measurementIterations(100)
            .forks(1)
            .build();
        try {
            new Runner(opt).run();
        } catch (RunnerException e) {
            throw new RuntimeException(e);
        }
    }

    protected ByteString toByteString(long l) {
        return unsafeWrap(ByteBuffer.allocate(Long.BYTES).putLong(l).array());
    }
}
