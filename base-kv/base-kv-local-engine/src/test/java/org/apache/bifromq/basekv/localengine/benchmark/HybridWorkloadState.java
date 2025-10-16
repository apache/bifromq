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

import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.localengine.IKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.IKVSpaceRefreshableReader;
import org.apache.bifromq.basekv.localengine.IKVSpaceWriter;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@Slf4j
@State(Scope.Benchmark)
public class HybridWorkloadState extends BenchmarkState {
    ConcurrentHashMap<Long, IKVSpaceIterator> itrMap = new ConcurrentHashMap<>();
    private String rangeId = "testRange";
    private IKVSpace kvSpace;
    private IKVSpaceRefreshableReader reader;
    private IKVSpaceWriter writer;

    @Override
    protected void afterSetup() {
        kvSpace = kvEngine.createIfMissing(rangeId);
        reader = kvSpace.reader();
//        itr = kvEngine.newIterator(rangeId);
    }


    @Override
    protected void beforeTeardown() {
//        itr.close();
        itrMap.values().forEach(IKVSpaceIterator::close);
    }

    public void randomPut() {
        writer.put(randomBS(), randomBS());
    }

    public void randomDelete() {
        writer.delete(randomBS());
    }

    public void randomPutAndDelete() {
        ByteString key = randomBS();
        writer.put(key, randomBS());
        writer.delete(key);
        writer.done();
    }

    public Optional<ByteString> randomGet() {
        return reader.get(randomBS());
    }

    public boolean randomExist() {
        return reader.exist(randomBS());
    }

    public void seekToFirst() {
        IKVSpaceIterator itr = itrMap.computeIfAbsent(Thread.currentThread().getId(),
            k -> reader.newIterator());
        reader.refresh();
        itr = itrMap.get(Thread.currentThread().getId());
        itr.seekToFirst();
    }

    public void randomSeek() {
        IKVSpaceIterator itr = itrMap.computeIfAbsent(Thread.currentThread().getId(),
            k -> reader.newIterator());
        reader.refresh();
        itr = itrMap.get(Thread.currentThread().getId());
        itr.seek(randomBS());
    }

    private ByteString randomBS() {
        return ByteString.copyFromUtf8(ThreadLocalRandom.current().nextInt(0, 1000000000) + "");
    }
}
