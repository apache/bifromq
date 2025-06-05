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

import org.apache.bifromq.basekv.localengine.IKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.IKVSpaceWriter;
import org.apache.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;
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
public class ContinuousKeyDeleteAndSeek_Benchmark extends BenchmarkTemplate {
    private static int keyCount = 1000000;
    private static ByteString key = ByteString.copyFromUtf8("key");
    private IKVSpace kvSpace;
    private IKVSpaceIterator itr;
    private String rangeId = "testRange";

//    IKVEngineIterator itr;

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
            writer.insert(key.concat(toByteString(i)), ByteString.copyFromUtf8("val" + i));
            writer.delete(key.concat(toByteString(i)));
//            kvEngine.deleteRange(batchId, DEFAULT_NS, key.concat(toByteString(i)), key.concat(toByteString(i + 1)));
        }
        writer.put(key.concat(toByteString(keyCount)), ByteString.EMPTY);
//        kvEngine.deleteRange(batchId, DEFAULT_NS, key.concat(toByteString(0)), key.concat(toByteString(keyCount)));
        writer.done();
    }

//    @Benchmark
//    @Group("Seek")
//    @GroupThreads(1)
//    @BenchmarkMode(Mode.Throughput)
//    @OutputTimeUnit(TimeUnit.SECONDS)
//    public void delete(BenchmarkThreadState state) {
//        kvEngine.deleteRange(DEFAULT_NS, key.concat(toByteString(state.i)), key.concat(toByteString(state.i + 1)));
//    }

    @Benchmark
    @Group("Seek")
    @GroupThreads(1)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public boolean iterator(BenchmarkThreadState state) {
        IKVSpaceIterator itr = kvSpace.newIterator(Boundary.newBuilder()
            .setStartKey(key.concat(toByteString(state.i)))
            .setEndKey(key.concat(toByteString(state.i + 1)))
            .build());
//        IKVEngineIterator itr = kvEngine.newIterator(DEFAULT_NS);
//        itr.seekToFirst();
        itr.seek(key.concat(toByteString(state.i)));
        return itr.isValid();
    }
}
