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

package org.apache.bifromq.basekv.localengine.rocksdb.metrics;

import io.micrometer.core.instrument.Meter;
import org.apache.bifromq.basekv.localengine.metrics.IKVSpaceMetric;

/**
 * RocksDB specific metrics for KVSpace.
 */
public enum RocksDBKVSpaceMetric implements IKVSpaceMetric {
    BlockCache("basekv.le.rocksdb.mem.blockcache", Meter.Type.GAUGE),
    TableReader("basekv.le.rocksdb.mem.tablereader", Meter.Type.GAUGE),
    MemTable("basekv.le.rocksdb.mem.memtable", Meter.Type.GAUGE),
    PinnedMem("basekv.le.rocksdb.mem.pinned", Meter.Type.GAUGE),
    // DB state and capacity gauges
    StateTotalSSTSize("basekv.le.rocksdb.state.totalsstsize", Meter.Type.GAUGE),
    StateLiveSSTSize("basekv.le.rocksdb.state.livesstsize", Meter.Type.GAUGE),
    StateLiveDataSize("basekv.le.rocksdb.state.livedatasize", Meter.Type.GAUGE),
    StateEstimateNumKeys("basekv.le.rocksdb.state.keys.est", Meter.Type.GAUGE),
    StatePendingCompactionBytes("basekv.le.rocksdb.state.pending.compaction.bytes", Meter.Type.GAUGE),
    StateRunningCompactions("basekv.le.rocksdb.state.running.compactions", Meter.Type.GAUGE),
    StateRunningFlushes("basekv.le.rocksdb.state.running.flushes", Meter.Type.GAUGE),
    StateCompactionPending("basekv.le.rocksdb.state.pending.compactions", Meter.Type.GAUGE),
    StateMemTableFlushPending("basekv.le.rocksdb.state.pending.flushes", Meter.Type.GAUGE),
    StateBackgroundErrors("basekv.le.rocksdb.state.bg.errors", Meter.Type.GAUGE),
    // IO and cache efficiency
    IOBytesReadCounter("basekv.le.rocksdb.io.read.bytes", Meter.Type.COUNTER, true),
    IOBytesWrittenCounter("basekv.le.rocksdb.io.write.bytes", Meter.Type.COUNTER, true),
    BlockCacheHitCounter("basekv.le.rocksdb.block.cache.hit", Meter.Type.COUNTER, true),
    BlockCacheMissCounter("basekv.le.rocksdb.block.cache.miss", Meter.Type.COUNTER, true),
    BlobDBCacheHitCounter("basekv.le.rocksdb.blob.cache.hit", Meter.Type.COUNTER, true),
    BlobDBCacheMissCounter("basekv.le.rocksdb.blob.cache.miss", Meter.Type.COUNTER, true),
    BloomUsefulCounter("basekv.le.rocksdb.bloom.useful.count", Meter.Type.COUNTER, true),
    CheckpointTimer("basekv.le.rocksdb.checkpoint.time", Meter.Type.TIMER),
    TotalKeysGauge("basekv.le.rocksdb.compaction.keys", Meter.Type.GAUGE),
    TotalTombstoneKeysGauge("basekv.le.rocksdb.compaction.delkeys", Meter.Type.GAUGE),
    TotalTombstoneRangesGauge("basekv.le.rocksdb.compaction.delranges", Meter.Type.GAUGE),
    ManualCompactionCounter("basekv.le.rocksdb.manual.compaction.count", Meter.Type.COUNTER),
    ManualCompactionTimer("basekv.le.rocksdb.manual.compaction.time", Meter.Type.TIMER),
    ManualFlushTimer("basekv.le.rocksdb.manual.flush.time", Meter.Type.TIMER),
    // Histogram exposure: counters and gauges
    GetLatency("basekv.le.rocksdb.latency.get.time", Meter.Type.TIMER, true),
    WriteLatency("basekv.le.rocksdb.latency.write.time", Meter.Type.TIMER, true),
    SeekLatency("basekv.le.rocksdb.latency.seek.time", Meter.Type.TIMER, true),
    BlobGetLatency("basekv.le.rocksdb.latency.blob.get.time", Meter.Type.TIMER, true),
    BlobWriteLatency("basekv.le.rocksdb.latency.blob.write.time", Meter.Type.TIMER, true),
    SSTReadLatency("basekv.le.rocksdb.latency.sstread.time", Meter.Type.TIMER, true),
    SSTWriteLatency("basekv.le.rocksdb.latency.sstwrite.time", Meter.Type.TIMER, true),
    FlushLatency("basekv.le.rocksdb.latency.flush.time", Meter.Type.TIMER, true),
    CompactionLatency("basekv.le.rocksdb.latency.compaction.time", Meter.Type.TIMER, true),
    WriteStall("basekv.le.rocksdb.write.stall", Meter.Type.TIMER, true);

    private final String metricName;
    private final Meter.Type meterType;
    private final boolean isFunction;

    RocksDBKVSpaceMetric(String metricName, Meter.Type meterType) {
        this(metricName, meterType, false);
    }

    RocksDBKVSpaceMetric(String metricName, Meter.Type meterType, boolean isFunction) {
        this.metricName = metricName;
        this.meterType = meterType;
        this.isFunction = isFunction;
    }

    @Override
    public String metricName() {
        return metricName;
    }

    @Override
    public Meter.Type meterType() {
        return meterType;
    }

    @Override
    public boolean isFunction() {
        return isFunction;
    }
}
