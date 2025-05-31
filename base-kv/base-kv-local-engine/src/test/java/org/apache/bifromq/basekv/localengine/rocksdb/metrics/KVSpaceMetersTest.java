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

package org.apache.bifromq.basekv.localengine.rocksdb.metrics;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertFalse;

import org.apache.bifromq.basekv.localengine.MockableTest;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceMeters;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class KVSpaceMetersTest extends MockableTest {
    @Test
    public void removeGaugeWhenNoRef() {
        AtomicInteger gaugeCounter = new AtomicInteger();
        Gauge gauge = KVSpaceMeters.getGauge("testSpace", RocksDBKVSpaceMetric.TotalTombstoneRangesGauge,
            gaugeCounter::get,
            Tags.of(new String[0]));
        String gaugeName = gauge.getId().getName();
        WeakReference<Gauge> weakRef = new WeakReference<>(gauge);
        assertFalse(Metrics.globalRegistry.find(gaugeName).gauges().isEmpty());
        gauge = null;
        await().forever().until(() -> {
            System.gc();
            return weakRef.get() == null && Metrics.globalRegistry.find(gaugeName).gauges().isEmpty();
        });
    }

    @Test
    public void removeTimerWhenNoRef() {
        Timer timer = KVSpaceMeters.getTimer("testSpace", RocksDBKVSpaceMetric.FlushTimer, Tags.of(new String[0]));
        String timerName = timer.getId().getName();
        WeakReference<Timer> weakRef = new WeakReference<>(timer);
        assertFalse(Metrics.globalRegistry.find(timerName).timers().isEmpty());
        timer = null;
        await().forever().until(() -> {
            System.gc();
            return weakRef.get() == null && Metrics.globalRegistry.find(timerName).timers().isEmpty();
        });
    }

    @Test
    public void removeCounterWhenNoRef() {
        Counter counter =
            KVSpaceMeters.getCounter("testSpace", RocksDBKVSpaceMetric.CompactionCounter, Tags.of(new String[0]));
        String timerName = counter.getId().getName();
        WeakReference<Counter> weakRef = new WeakReference<>(counter);
        assertFalse(Metrics.globalRegistry.find(timerName).counters().isEmpty());
        counter = null;
        await().forever().until(() -> {
            System.gc();
            return weakRef.get() == null && Metrics.globalRegistry.find(timerName).counters().isEmpty();
        });
    }
}
