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

package org.apache.bifromq.basekv.store.range.hinter;

import static com.google.protobuf.ByteString.unsignedLexicographicalComparator;

import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

final class LoadRecordWindow {
    private final AtomicInteger records = new AtomicInteger();
    private final AtomicInteger totalKVIOs = new AtomicInteger();
    private final AtomicLong totalKVIONanos = new AtomicLong();
    private final AtomicLong totalLatency = new AtomicLong();
    private final Map<ByteString, AtomicLong> loadDistribution = new ConcurrentHashMap<>();

    LoadRecordWindow() {

    }

    void record(Map<ByteString, Long> keyLoads, int kvIOs, long kvIOTimeNanos, long latencyNanos) {
        keyLoads.forEach((key, keyAccessNanos) -> loadDistribution.computeIfAbsent(key, k -> new AtomicLong())
            .addAndGet(keyAccessNanos));
        totalKVIOs.addAndGet(kvIOs);
        totalKVIONanos.addAndGet(kvIOTimeNanos);
        totalLatency.addAndGet(latencyNanos);
        records.incrementAndGet();
    }

    public int records() {
        return records.get();
    }

    public int ioDensity() {
        return (int) Math.ceil(totalKVIOs.get() / Math.max(records.get(), 1.0));
    }

    public long ioLatencyNanos() {
        return totalKVIONanos.get() / Math.max(totalKVIOs.get(), 1);
    }

    public long avgLatencyNanos() {
        return totalLatency.get() / Math.max(records.get(), 1);
    }

    public Optional<ByteString> estimateSplitKey() {
        long loadSum = 0;
        long totalKeyIONanos = 0;
        NavigableMap<ByteString, AtomicLong> slotDistro = new TreeMap<>(unsignedLexicographicalComparator());
        for (Map.Entry<ByteString, AtomicLong> entry : loadDistribution.entrySet()) {
            slotDistro.put(entry.getKey(), entry.getValue());
            totalKeyIONanos += entry.getValue().get();
        }
        long halfTotal = totalKeyIONanos / 2;
        for (Map.Entry<ByteString, AtomicLong> e : slotDistro.entrySet()) {
            loadSum += e.getValue().get();
            if (loadSum >= halfTotal) {
                return Optional.of(e.getKey());
            }
        }
        return Optional.empty();
    }
}
