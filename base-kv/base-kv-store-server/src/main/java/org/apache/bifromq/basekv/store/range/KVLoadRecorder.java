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

package org.apache.bifromq.basekv.store.range;

import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.bifromq.basekv.store.range.hinter.IKVLoadRecord;

public class KVLoadRecorder implements IKVLoadRecorder {
    private final long startNanos;
    private final Map<ByteString, Long> loadDistribution = new HashMap<>(256);
    private long kvIONanos = 0L;
    private int kvIOs = 0;

    public KVLoadRecorder() {
        this(System::nanoTime);
    }

    public KVLoadRecorder(Supplier<Long> nanoSource) {
        startNanos = nanoSource.get();
    }

    @Override
    public void record(ByteString key, long latencyNanos) {
        loadDistribution.compute(key, (k, v) -> v == null ? latencyNanos : v + latencyNanos);
        kvIOs++;
        kvIONanos += latencyNanos;
    }

    @Override
    public void record(long latencyNanos) {
        kvIONanos += latencyNanos;
        kvIOs++;
    }

    @Override
    public IKVLoadRecord stop() {
        return new IKVLoadRecord() {
            @Override
            public long startNanos() {
                return startNanos;
            }

            @Override
            public int getKVIOs() {
                return kvIOs;
            }

            @Override
            public long getKVIONanos() {
                return kvIONanos;
            }

            @Override
            public Map<ByteString, Long> keyDistribution() {
                return loadDistribution;
            }
        };
    }
}
