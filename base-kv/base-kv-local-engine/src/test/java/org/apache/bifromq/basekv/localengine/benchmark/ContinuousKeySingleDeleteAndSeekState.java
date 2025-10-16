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

import com.google.protobuf.ByteString;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.IKVSpaceRefreshableReader;
import org.apache.bifromq.basekv.localengine.IKVSpaceWriter;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class ContinuousKeySingleDeleteAndSeekState extends BenchmarkState {
    int keyCount = 1000000;
    ByteString key = ByteString.copyFromUtf8("key");
    IKVSpaceRefreshableReader reader;
    IKVSpaceIterator itr;
    private ICPableKVSpace kvSpace;
    private String rangeId = "testRange";

    @Override
    protected void afterSetup() {
        kvSpace = kvEngine.createIfMissing(rangeId);
        reader = kvSpace.reader();
        itr = reader.newIterator();
        IKVSpaceWriter writer = kvSpace.toWriter();

        for (int i = 0; i < keyCount; i++) {
            writer.put(key.concat(toByteString(i)), ByteString.EMPTY);
            writer.delete(key.concat(toByteString(i)));
        }
        writer.put(key.concat(toByteString(keyCount)), ByteString.EMPTY);
        writer.done();
        itr = reader.newIterator();
    }

    @Override
    protected void beforeTeardown() {

    }
}
