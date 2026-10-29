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

package org.apache.bifromq.basecluster;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class AgentHostExecutorLifecycleTest {
    @Test
    public void closeStopsScheduler() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Metrics.addRegistry(registry);
        Set<Thread> before = schedulerThreads();
        try (IAgentHost host = IAgentHost.newInstance(options())) {
            await().atMost(Duration.ofSeconds(10)).until(() -> schedulerThreads().size() > before.size());
            Set<Thread> started = schedulerThreads();
            started.removeAll(before);
            assertEquals(started.size(), 1);
            CompletableFuture.runAsync(host::close).get(15, TimeUnit.SECONDS);
            for (Thread thread : started) {
                thread.join(5000);
                assertFalse(thread.isAlive(), thread.getName());
            }
            assertTrue(registry.getMeters().stream().map(Meter::getId)
                .noneMatch(id -> id.getName().startsWith("executor")
                    && id.getTag("name").startsWith("agent-host-scheduler")));
        } finally {
            Metrics.removeRegistry(registry);
            registry.close();
        }
    }

    @Test
    public void hostMetricsAreIndependentAndCanBeRecreated() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Metrics.addRegistry(registry);
        try (IAgentHost first = IAgentHost.newInstance(options());
             IAgentHost second = IAgentHost.newInstance(options())) {
            String firstName = executorName(first);
            String secondName = executorName(second);
            assertNotEquals(firstName, secondName);
            assertEquals(executorMeters(registry, firstName).size(), 11);
            assertEquals(executorMeters(registry, secondName).size(), 11);
            for (Meter meter : executorMeters(registry, firstName)) {
                assertEquals(meter.getId().getTags().size(), 1);
                assertEquals(meter.getId().getTags().get(0).getKey(), "name");
            }
            Timer firstTimer = registry.get("executor").tag("name", firstName).timer();
            Timer secondTimer = registry.get("executor").tag("name", secondName).timer();
            await().atMost(Duration.ofSeconds(10)).until(() -> firstTimer.count() > 0 && secondTimer.count() > 0);
            CompletableFuture.runAsync(first::close).get(15, TimeUnit.SECONDS);
            assertTrue(executorMeters(registry, firstName).isEmpty());
            assertEquals(executorMeters(registry, secondName).size(), 11);
            long secondCount = secondTimer.count();
            second.host("still-running");
            await().atMost(Duration.ofSeconds(10)).until(() -> secondTimer.count() > secondCount);
            try (IAgentHost replacement = IAgentHost.newInstance(options().port(first.local().getPort()))) {
                assertEquals(executorName(replacement), firstName);
                Timer newTimer = registry.get("executor").tag("name", firstName).timer();
                assertNotSame(newTimer, firstTimer);
                assertEquals(executorMeters(registry, firstName).size(), 11);
                first.close();
                assertEquals(executorMeters(registry, firstName).size(), 11);
                await().atMost(Duration.ofSeconds(10)).until(() -> newTimer.count() > 0);
            }
            assertTrue(executorMeters(registry, firstName).isEmpty());
        } finally {
            Metrics.removeRegistry(registry);
            registry.close();
        }
    }

    private static AgentHostOptions options() {
        return AgentHostOptions.builder().addr("127.0.0.1").env("executor-lifecycle")
            .gossipPeriod(Duration.ofMillis(20)).build();
    }

    private static String executorName(IAgentHost host) {
        return "agent-host-scheduler[" + host.env() + "@" + host.local().getAddress() + ":"
            + host.local().getPort() + "]";
    }

    private static List<Meter> executorMeters(SimpleMeterRegistry registry, String name) {
        return registry.getMeters().stream().filter(meter -> meter.getId().getName().startsWith("executor")
            && name.equals(meter.getId().getTag("name"))).toList();
    }

    private static Set<Thread> schedulerThreads() {
        return Thread.getAllStackTraces().keySet().stream().filter(thread -> thread.isAlive()
            && thread.getName().startsWith("agent-host-scheduler")).collect(Collectors.toSet());
    }
}
