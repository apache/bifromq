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

package com.baidu.bifromq.basekv.localengine.rocksdb;

import com.baidu.bifromq.basekv.localengine.metrics.KVSpaceMeters;
import com.baidu.bifromq.basekv.localengine.metrics.KVSpaceMetric;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import java.util.concurrent.atomic.AtomicInteger;

public class RocksDBKVSpaceCompactionTrigger implements IWriteStatsRecorder {
    public interface CompactionScheduler {
        boolean schedule();
    }

    private final AtomicInteger totalKeyCount = new AtomicInteger();
    private final AtomicInteger totalTombstoneKeyCount = new AtomicInteger();
    private final AtomicInteger totalTombstoneRangeCount = new AtomicInteger();

    private final int minTombstoneKeysTrigger;
    private final int minTombstoneRangesTrigger;
    private final double minTombstoneKeysRatioTrigger;
    private final Runnable compactionScheduler;
    private final Gauge totalKeysGauge;
    private final Gauge totalTombstoneKeysGauge;
    private final Gauge totalTombstoneRangesGauge;

    public RocksDBKVSpaceCompactionTrigger(String id,
                                           int minTombstoneKeysTrigger,
                                           int minTombstoneRangesTrigger,
                                           double minTombstoneKeysRatioTrigger,
                                           Runnable compactionScheduler,
                                           String... tags) {
        this.minTombstoneKeysTrigger = minTombstoneKeysTrigger;
        this.minTombstoneRangesTrigger = minTombstoneRangesTrigger;
        this.minTombstoneKeysRatioTrigger = minTombstoneKeysRatioTrigger;
        this.compactionScheduler = compactionScheduler;
        Tags metricTags = Tags.of(tags);
        totalKeysGauge = KVSpaceMeters
            .getGauge(id, KVSpaceMetric.TotalKeysGauge, totalKeyCount::get, metricTags);
        totalTombstoneKeysGauge =
            KVSpaceMeters.getGauge(id, KVSpaceMetric.TotalTombstoneKeysGauge, totalTombstoneKeyCount::get, metricTags);
        totalTombstoneRangesGauge =
            KVSpaceMeters.getGauge(id, KVSpaceMetric.TotalTombstoneRangesGauge, totalTombstoneRangeCount::get,
                metricTags);
    }

    public IRecorder newRecorder() {
        return new WriteStatsRecorder();
    }

    public void reset() {
        totalKeyCount.set(0);
        totalTombstoneKeyCount.set(0);
        totalTombstoneRangeCount.set(0);
    }

    class WriteStatsRecorder implements IRecorder {
        private final AtomicInteger keyCount = new AtomicInteger();
        private final AtomicInteger tombstoneKeyCount = new AtomicInteger();
        private final AtomicInteger tombstoneRangeCount = new AtomicInteger();


        public void recordPut() {
            keyCount.incrementAndGet();
            tombstoneKeyCount.incrementAndGet();
        }

        public void recordInsert() {
            keyCount.incrementAndGet();
        }

        public void recordDelete() {
            tombstoneKeyCount.incrementAndGet();
        }

        public void recordDeleteRange() {
            tombstoneRangeCount.incrementAndGet();
        }

        public void stop() {
            int totalKeys = totalKeyCount.addAndGet(keyCount.get());
            int totalTombstones = totalTombstoneKeyCount.addAndGet(tombstoneKeyCount.get());
            int totalRangeTombstones = totalTombstoneRangeCount.addAndGet(tombstoneRangeCount.get());
            if (totalRangeTombstones > minTombstoneRangesTrigger || (totalTombstones > minTombstoneKeysTrigger &&
                (double) totalTombstones / (totalKeys + totalTombstones) >= minTombstoneKeysRatioTrigger)) {
                compactionScheduler.run();
            }
        }
    }
}
