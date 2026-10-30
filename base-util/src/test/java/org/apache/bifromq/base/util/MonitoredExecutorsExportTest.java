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

package org.apache.bifromq.base.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Checks executor meter removal from Prometheus exports.
 */
public class MonitoredExecutorsExportTest {
    @DataProvider
    public Object[][] poolKinds() {
        return new Object[][] {{0, 9}, {1, 11}, {2, 6}};
    }

    @Test(dataProvider = "poolKinds")
    public void removesExportedSamples(int kind, int count) throws Exception {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        try {
            ExecutorService service = MonitoredExecutors.monitor(registry, pool(kind), "first", "owned", Tags.empty());
            ExecutorService other = MonitoredExecutors.monitor(registry, pool(kind), "other", "owned", Tags.empty());
            try {
                service.submit(() -> 1).get(5, TimeUnit.SECONDS);
                other.submit(() -> 2).get(5, TimeUnit.SECONDS);
                assertEquals(registry.getMeters().size(), count * 2);
                assertTrue(registry.scrape().contains("name=\"first\""));
                assertTrue(registry.scrape().contains("name=\"other\""));
                service.shutdown();
                assertTrue(service.awaitTermination(5, TimeUnit.SECONDS));
                String scrape = registry.scrape();
                assertFalse(scrape.contains("name=\"first\""));
                assertTrue(scrape.contains("name=\"other\""));
                assertEquals(other.submit(() -> 3).get(5, TimeUnit.SECONDS), Integer.valueOf(3));
            } finally {
                service.shutdownNow();
                other.shutdownNow();
                assertTrue(service.awaitTermination(5, TimeUnit.SECONDS));
                assertTrue(other.awaitTermination(5, TimeUnit.SECONDS));
            }
            assertFalse(registry.scrape().contains("owned_executor"));
        } finally {
            registry.close();
        }
    }

    @Test
    public void followsCompositeMembershipAndKeepsHistograms() throws Exception {
        CompositeMeterRegistry registry = new CompositeMeterRegistry();
        PrometheusMeterRegistry first = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        PrometheusMeterRegistry second = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        try {
            second.config().meterFilter(new MeterFilter() {
                @Override
                public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
                    return DistributionStatisticConfig.builder().percentilesHistogram(true).build().merge(config);
                }
            });
            ExecutorService service = MonitoredExecutors.monitor(registry, pool(0), "composite");
            try {
                registry.add(first);
                service.submit(() -> 1).get(5, TimeUnit.SECONDS);
                assertTrue(first.scrape().contains("executor_seconds_count"));
                registry.add(second);
                service.submit(() -> 2).get(5, TimeUnit.SECONDS);
                assertTrue(second.scrape().contains("executor_seconds_bucket"));
                registry.remove(first);
                long firstCount = first.get("executor").timer().count();
                service.submit(() -> 3).get(5, TimeUnit.SECONDS);
                assertEquals(first.get("executor").timer().count(), firstCount);
                assertEquals(second.get("executor").timer().count(), 2L);
                service.shutdown();
                assertTrue(service.awaitTermination(5, TimeUnit.SECONDS));
                assertTrue(registry.getMeters().isEmpty());
                assertFalse(second.scrape().contains("name=\"composite\""));
            } finally {
                service.shutdownNow();
                assertTrue(service.awaitTermination(5, TimeUnit.SECONDS));
            }
        } finally {
            registry.close();
            first.close();
            second.close();
        }
    }

    private static ExecutorService pool(int kind) {
        return switch (kind) {
            case 0 -> new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
            case 1 -> new ScheduledThreadPoolExecutor(1);
            default -> new ForkJoinPool(1);
        };
    }
}
