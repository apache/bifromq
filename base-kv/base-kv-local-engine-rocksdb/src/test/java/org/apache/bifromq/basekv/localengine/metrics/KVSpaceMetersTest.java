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

package org.apache.bifromq.basekv.localengine.metrics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.basekv.localengine.rocksdb.metrics.RocksDBKVSpaceMetric;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KVSpaceMetersTest {
    private SimpleMeterRegistry registry;
    private List<MeterRegistry> savedRegistries;
    private String id;
    private Tags baseTags;

    @BeforeMethod
    public void setUp() {
        registry = new SimpleMeterRegistry();
        savedRegistries = new ArrayList<>();
        CompositeMeterRegistry composite = Metrics.globalRegistry;
        savedRegistries.addAll(composite.getRegistries());
        for (MeterRegistry r : savedRegistries) {
            Metrics.removeRegistry(r);
        }
        Metrics.addRegistry(registry);
        id = "kv1";
        baseTags = Tags.of("env", "test");
    }

    @AfterMethod
    public void tearDown() {
        Metrics.removeRegistry(registry);
        registry.close();
        for (MeterRegistry r : savedRegistries) {
            Metrics.addRegistry(r);
        }
    }

    @Test
    public void counterIncrementAndUnregister() {
        Counter c = KVSpaceMeters.getCounter(id, RocksDBKVSpaceMetric.ManualCompactionCounter, baseTags);
        c.increment(2.0);
        c.increment(3.0);
        assertEquals(c.count(), 5.0, 0.0001);

        assertFalse(registry
            .find(RocksDBKVSpaceMetric.ManualCompactionCounter.metricName())
            .tags("env", "test", "kvspace", id)
            .meters().isEmpty());

        c.close();
        assertTrue(registry
            .find(RocksDBKVSpaceMetric.ManualCompactionCounter.metricName())
            .tags("env", "test", "kvspace", id)
            .meters().isEmpty());
    }

    @Test
    public void functionCounterBehaviorAndUnregister() {
        class Holder {
            double v;
        }
        Holder h = new Holder();
        h.v = 7.0;

        FunctionCounter fc = KVSpaceMeters.getFunctionCounter(
            id, RocksDBKVSpaceMetric.IOBytesReadCounter, h, x -> x.v, baseTags);

        assertEquals(fc.count(), 7.0, 0.0001);
        h.v = 9.0;
        assertEquals(fc.count(), 9.0, 0.0001);

        assertFalse(registry
            .find(RocksDBKVSpaceMetric.IOBytesReadCounter.metricName())
            .tags("env", "test", "kvspace", id)
            .meters().isEmpty());

        fc.close();
        assertTrue(registry
            .find(RocksDBKVSpaceMetric.IOBytesReadCounter.metricName())
            .tags("env", "test", "kvspace", id)
            .meters().isEmpty());
    }

    @Test
    public void functionTimerBehaviorAndUnregister() {
        class T {
            long c;
            double t;
        }
        T h = new T();
        h.c = 2L;
        h.t = 1.5;

        FunctionTimer ft = KVSpaceMeters.getFunctionTimer(
            id, RocksDBKVSpaceMetric.GetLatency, h, x -> x.c, x -> x.t, TimeUnit.SECONDS, baseTags);

        assertEquals(ft.count(), 2.0, 0.0001);
        assertEquals(ft.totalTime(TimeUnit.SECONDS), 1.5, 0.0001);

        h.c = 3L;
        h.t = 2.5;
        assertEquals(ft.count(), 3.0, 0.0001);
        assertEquals(ft.totalTime(TimeUnit.SECONDS), 2.5, 0.0001);

        assertFalse(registry
            .find(RocksDBKVSpaceMetric.GetLatency.metricName())
            .tags("env", "test", "kvspace", id)
            .meters().isEmpty());

        ft.close();
        assertTrue(registry
            .find(RocksDBKVSpaceMetric.GetLatency.metricName())
            .tags("env", "test", "kvspace", id)
            .meters().isEmpty());
    }
}
