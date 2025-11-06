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
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
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
    public void timerCacheAndUnregister() {
        Timer t1 = KVSpaceMeters.getTimer(id, GeneralKVSpaceMetric.CallTimer, baseTags);
        Timer t2 = KVSpaceMeters.getTimer(id, GeneralKVSpaceMetric.CallTimer, baseTags);
        assertSame(t1, t2);

        t1.record(10, TimeUnit.MILLISECONDS);
        assertTrue(t1.count() > 0);
        assertTrue(t1.totalTime(TimeUnit.MILLISECONDS) > 0);

        t1.close();
        assertTrue(registry
            .find(GeneralKVSpaceMetric.CallTimer.metricName())
            .tags("env", "test", "kvspace", id)
            .meters().isEmpty());
    }

    @Test
    public void summaryRecordAndMax() {
        DistributionSummary s =
            KVSpaceMeters.getSummary(id, GeneralKVSpaceMetric.ReadBytesDistribution, baseTags);
        s.record(1.0);
        s.record(3.0);
        s.record(2.0);

        assertEquals(s.count(), 3L);
        assertEquals(s.totalAmount(), 6.0, 0.0001);
        assertEquals(s.max(), 3.0, 0.0001);

        assertFalse(registry
            .find(GeneralKVSpaceMetric.ReadBytesDistribution.metricName())
            .tags("env", "test", "kvspace", id)
            .meters().isEmpty());

        s.close();
        assertTrue(registry
            .find(GeneralKVSpaceMetric.ReadBytesDistribution.metricName())
            .tags("env", "test", "kvspace", id)
            .meters().isEmpty());
    }

    @Test
    public void gaugeValueUpdatesAndUnregister() {
        AtomicLong value = new AtomicLong(1);
        Gauge g = KVSpaceMeters.getGauge(id, GeneralKVSpaceMetric.CheckpointNumGauge, value::get, baseTags);
        assertEquals(g.value(), 1.0, 0.0001);

        value.set(5);
        assertEquals(g.value(), 5.0, 0.0001);

        assertFalse(registry
            .find(GeneralKVSpaceMetric.CheckpointNumGauge.metricName())
            .tags("env", "test", "kvspace", id)
            .meters().isEmpty());

        g.close();
        assertTrue(registry
            .find(GeneralKVSpaceMetric.CheckpointNumGauge.metricName())
            .tags("env", "test", "kvspace", id)
            .meters().isEmpty());
    }

    @Test
    public void tagsInjectionOnMeters() {
        Timer t = KVSpaceMeters.getTimer(id, GeneralKVSpaceMetric.CallTimer, baseTags);
        Map<String, String> tagMap = t.getId().getTags().stream()
            .collect(Collectors.toMap(Tag::getKey, Tag::getValue));

        assertEquals(tagMap.get("env"), "test");
        assertEquals(tagMap.get("kvspace"), id);

        assertFalse(registry
            .find(GeneralKVSpaceMetric.CallTimer.metricName())
            .tags("env", "test", "kvspace", id)
            .meters().isEmpty());
        t.close();
    }
}
