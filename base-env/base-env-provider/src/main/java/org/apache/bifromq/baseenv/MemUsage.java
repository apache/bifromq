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

package org.apache.bifromq.baseenv;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufAllocatorMetric;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PoolChunkListMetric;
import io.netty.buffer.PoolChunkMetric;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import io.netty.util.internal.PlatformDependent;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class MemUsage {
    private static final long UPDATE_INTERVAL = Duration.ofMillis(10).toNanos();
    private static final long JVM_MAX_DIRECT_MEMORY = PlatformDependent.estimateMaxDirectMemory();
    private static final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private static final ThreadLocal<MemUsage> THREAD_LOCAL = ThreadLocal.withInitial(MemUsage::new);
    private static final NonblockingNettyDirectMemoryUsage nonblockingNettyDirectMemoryUsage =
        new NonblockingNettyDirectMemoryUsage();
    private double nettyDirectMemoryUsage = 0;
    private double heapMemoryUsage = 0;
    private long refreshNettyDirectMemoryUsageAt = 0;
    private long refreshHeapMemoryUsageAt = 0;

    public static MemUsage local() {
        return THREAD_LOCAL.get();
    }

    public double nettyDirectMemoryUsage() {
        scheduleNettyDirectMemoryUsage();
        return nettyDirectMemoryUsage;
    }

    public double heapMemoryUsage() {
        scheduleHeapMemoryUsage();
        return heapMemoryUsage;
    }

    private void scheduleNettyDirectMemoryUsage() {
        long now = System.nanoTime();
        if (now - refreshNettyDirectMemoryUsageAt > UPDATE_INTERVAL) {
            nettyDirectMemoryUsage = nonblockingNettyDirectMemoryUsage.usage();
            refreshNettyDirectMemoryUsageAt = System.nanoTime();
        }
    }

    private void scheduleHeapMemoryUsage() {
        long now = System.nanoTime();
        if (now - refreshHeapMemoryUsageAt > UPDATE_INTERVAL) {
            heapMemoryUsage = calculateHeapMemoryUsage();
            refreshHeapMemoryUsageAt = System.nanoTime();
        }
    }

    private double calculateHeapMemoryUsage() {
        try {
            MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
            long usedHeapMemory = memoryUsage.getUsed();
            long maxHeapMemory = memoryUsage.getMax();
            return (double) usedHeapMemory / maxHeapMemory;
        } catch (IllegalArgumentException e) {
            // there is an unresolved issue in open jdk17: https://bugs.openjdk.org/browse/JDK-8207200
            return 0;
        }
    }

    private static class NonblockingNettyDirectMemoryUsage {
        private final Executor executor =
            newSingleThreadExecutor(EnvProvider.INSTANCE.newThreadFactory("netty-pool-usage-reader", true));
        private final AtomicBoolean isRefreshing = new AtomicBoolean(false);
        private final int[] buckets = new int[101];
        private volatile double nettyDirectMemoryUsage = 0;

        double usage() {
            scheduleRefresh();
            return nettyDirectMemoryUsage;
        }

        void scheduleRefresh() {
            if (isRefreshing.compareAndSet(false, true)) {
                executor.execute(() -> {
                    nettyDirectMemoryUsage = calculateNettyDirectMemoryUsage();
                    isRefreshing.set(false);
                });
            }
        }

        private double calculateNettyDirectMemoryUsage() {
            if (PlatformDependent.useDirectBufferNoCleaner()) {
                if (ByteBufAllocator.DEFAULT.isDirectBufferPooled()) {
                    long pooledDirectMemory = PlatformDependent.usedDirectMemory();
                    double pooledDirectMemoryUsage = pooledDirectMemoryUsage();
                    double jvmDirectMemoryUsage = pooledDirectMemory / (double) JVM_MAX_DIRECT_MEMORY;
                    return Math.min(pooledDirectMemoryUsage, jvmDirectMemoryUsage);
                } else {
                    return (PlatformDependent.usedDirectMemory()) / (double) PlatformDependent.maxDirectMemory();
                }
            } else {
                ByteBufAllocatorMetric allocatorMetric =
                    ((ByteBufAllocatorMetricProvider) ByteBufAllocator.DEFAULT).metric();
                long usedDirectMemory = allocatorMetric.usedDirectMemory();
                if (ByteBufAllocator.DEFAULT.isDirectBufferPooled()) {
                    double pooledDirectMemoryUsage = pooledDirectMemoryUsage();
                    double jvmDirectMemoryUsage = usedDirectMemory / (double) JVM_MAX_DIRECT_MEMORY;
                    return Math.min(pooledDirectMemoryUsage, jvmDirectMemoryUsage);
                } else {
                    return usedDirectMemory
                        / (double) Math.min(PlatformDependent.maxDirectMemory(), JVM_MAX_DIRECT_MEMORY);
                }
            }
        }

        private double pooledDirectMemoryUsage() {
            PooledByteBufAllocatorMetric allocatorMetric =
                (PooledByteBufAllocatorMetric) ((ByteBufAllocatorMetricProvider) ByteBufAllocator.DEFAULT).metric();

            Arrays.fill(buckets, 0);
            int totalChunks = 0;

            for (PoolArenaMetric arenaMetric : allocatorMetric.directArenas()) {
                totalChunks += collectChunkUsages(arenaMetric, buckets);
            }

            return calculatePercentile(buckets, totalChunks, 90) / 100.0;
        }

        private int collectChunkUsages(PoolArenaMetric arenaMetric, int[] buckets) {
            int totalChunks = 0;
            for (PoolChunkListMetric chunkListMetric : arenaMetric.chunkLists()) {
                for (PoolChunkMetric chunkMetric : chunkListMetric) {
                    int usage = chunkMetric.usage();
                    buckets[usage]++;
                    totalChunks++;
                }
            }
            return totalChunks;
        }

        private double calculatePercentile(int[] buckets, int totalChunks, double percentile) {
            if (totalChunks == 0) {
                return 0;
            }
            int threshold = (int) Math.ceil(percentile / 100.0 * totalChunks);
            int cumulativeCount = 0;
            for (int i = 0; i < buckets.length; i++) {
                cumulativeCount += buckets[i];
                if (cumulativeCount >= threshold) {
                    return i;
                }
            }
            return 100;
        }
    }
}
