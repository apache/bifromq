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


import static org.apache.bifromq.basekv.localengine.TestUtil.toByteString;

import org.apache.bifromq.basekv.localengine.IKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.IKVSpaceWriter;
import org.apache.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@Slf4j
@State(Scope.Group)
public class ContinuousKeyDeleteAndGet_Benchmark extends BenchmarkTemplate {
    private static int keyCount = 1000000;
    private ByteString key = ByteString.copyFromUtf8("key");
    private IKVSpace kvSpace;
    private IKVSpaceIterator itr;
    private String rangeId = "testRange";

    @State(Scope.Thread)
    public static class BenchmarkThreadState {
        volatile int i = ThreadLocalRandom.current().nextInt(keyCount);

        @Setup(Level.Invocation)
        public void inc() {
            i = ThreadLocalRandom.current().nextInt(keyCount);
        }
    }

    @Override
    protected void doSetup() {
        kvSpace = kvEngine.createIfMissing(rangeId);
        itr = kvSpace.newIterator();
        IKVSpaceWriter writer = kvSpace.toWriter();
        for (int i = 0; i < keyCount; i++) {
            writer.put(key.concat(toByteString(i)), ByteString.EMPTY);
//            kvEngine.delete(batchId, DEFAULT_NS, key.concat(toByteString(i)));
        }
        for (int i = keyCount - 1; i >= 0; i--) {
//            kvEngine.delete(batchId, DEFAULT_NS, key.concat(toByteString(i)));
            writer.clear(Boundary.newBuilder()
                .setStartKey(key.concat(toByteString(i)))
                .setEndKey(key.concat(toByteString(i + 1)))
                .build());
        }
        writer.put(key.concat(toByteString(keyCount)), ByteString.EMPTY);
//        kvEngine.deleteRange(batchId, DEFAULT_NS, key.concat(toByteString(0)), key.concat(toByteString(keyCount)));
        writer.done();
    }

    @Benchmark
    @Group("Get")
    @GroupThreads(8)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public Optional<ByteString> get(BenchmarkThreadState state) {
        return kvSpace.get(key.concat(toByteString(state.i)));
    }
}
