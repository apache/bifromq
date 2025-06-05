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

import org.apache.bifromq.basekv.localengine.metrics.IKVSpaceMetric;
import io.micrometer.core.instrument.Meter;

/**
 * RocksDB specific metrics for KVSpace.
 */
public enum RocksDBKVSpaceMetric implements IKVSpaceMetric {
    BlockCache("basekv.le.rocksdb.mem.blockcache", Meter.Type.GAUGE),
    TableReader("basekv.le.rocksdb.mem.tablereader", Meter.Type.GAUGE),
    MemTable("basekv.le.rocksdb.mem.memtable", Meter.Type.GAUGE),
    PinnedMem("basekv.le.rocksdb.mem.pinned", Meter.Type.GAUGE),
    CheckpointTimer("basekv.le.rocksdb.checkpoint.time", Meter.Type.TIMER),
    CompactionCounter("basekv.le.rocksdb.compaction.count", Meter.Type.COUNTER),
    CompactionTimer("basekv.le.rocksdb.compaction.time", Meter.Type.TIMER),
    TotalKeysGauge("basekv.le.rocksdb.compaction.keys", Meter.Type.GAUGE),
    TotalTombstoneKeysGauge("basekv.le.rocksdb.compaction.delkeys", Meter.Type.GAUGE),
    TotalTombstoneRangesGauge("basekv.le.rocksdb.compaction.delranges", Meter.Type.GAUGE),
    FlushTimer("basekv.le.rocksdb.flush.time", Meter.Type.TIMER);

    private final String metricName;
    private final Meter.Type meterType;

    RocksDBKVSpaceMetric(String metricName, Meter.Type meterType) {
        this.metricName = metricName;
        this.meterType = meterType;
    }

    @Override
    public String metricName() {
        return metricName;
    }

    @Override
    public Meter.Type meterType() {
        return meterType;
    }
}
