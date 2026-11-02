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

package org.apache.bifromq.dist.worker;

import static org.apache.bifromq.dist.worker.schema.cache.Matchings.normalMatching;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.deliverer.DeliveryCallResult;
import org.apache.bifromq.deliverer.IMessageDeliverer;
import org.apache.bifromq.deliverer.TopicMessagePackHolder;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.type.TopicMessagePack;
import org.testng.annotations.Test;

/**
 * Checks executor meter ownership across ranges.
 */
public class DeliverExecutorLifecycleTest {
    @Test
    public void sameIndexInDifferentRangesHasSeparateMeters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Metrics.addRegistry(registry);
        IMessageDeliverer deliverer = mock(IMessageDeliverer.class);
        when(deliverer.schedule(any())).thenReturn(CompletableFuture.completedFuture(DeliveryCallResult.OK));
        IEventCollector collector = mock(IEventCollector.class);
        DeliverExecutor first = new DeliverExecutor(0, deliverer, collector, "rangeId", "first");
        DeliverExecutor second = new DeliverExecutor(0, deliverer, collector, "rangeId", "second");
        try {
            assertEquals(count(registry, "first"), 9L);
            assertEquals(count(registry, "second"), 9L);
            submit(first);
            submit(second);
            await().atMost(Duration.ofSeconds(5)).until(() -> registry.get("deliver.executor")
                .tags("rangeId", "first").timer().count() > 0);
            await().atMost(Duration.ofSeconds(5)).until(() -> registry.get("deliver.executor")
                .tags("rangeId", "second").timer().count() > 0);
            first.shutdown();
            assertEquals(count(registry, "first"), 0L);
            long before = registry.get("deliver.executor").tags("rangeId", "second").timer().count();
            submit(second);
            await().atMost(Duration.ofSeconds(5)).until(() -> registry.get("deliver.executor")
                .tags("rangeId", "second").timer().count() > before);
            assertEquals(count(registry, "second"), 9L);
        } finally {
            first.shutdown();
            second.shutdown();
            assertEquals(count(registry, "first") + count(registry, "second"), 0L);
            Metrics.removeRegistry(registry);
            registry.close();
        }
    }

    private static void submit(DeliverExecutor executor) {
        executor.submit(normalMatching("tenant", "topic", 0, "receiver", "deliverer", 1),
            TopicMessagePackHolder.hold(TopicMessagePack.newBuilder().setTopic("topic").build()), false);
    }

    private static long count(SimpleMeterRegistry registry, String range) {
        return registry.getMeters().stream().filter(meter -> meter.getId().getName().startsWith("deliver.executor")
            && range.equals(meter.getId().getTag("rangeId"))).count();
    }
}
