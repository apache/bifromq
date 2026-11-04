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

package org.apache.bifromq.inbox.store.delay;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.Test;

/**
 * Checks executor meter removal before range replacement.
 */
public class DelayTaskRunnerLifecycleTest {
    @Test
    public void closesOnlyTheOwningRange() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Metrics.addRegistry(registry);
        List<DelayTaskRunner<String>> runners = new ArrayList<>();
        CompletableFuture<Void> release = new CompletableFuture<>();
        try {
            DelayTaskRunner<String> first = runner("first");
            runners.add(first);
            DelayTaskRunner<String> second = runner("second");
            runners.add(second);
            assertEquals(count(registry, "first"), 11L);
            assertEquals(count(registry, "second"), 11L);
            execute(first).get(5, TimeUnit.SECONDS);
            execute(second).get(5, TimeUnit.SECONDS);
            final Thread worker = execute(first, release).get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> closeStarted = new CompletableFuture<>();
            CompletableFuture<Void> closing = CompletableFuture.runAsync(() -> {
                closeStarted.complete(null);
                first.shutdown();
            });
            runners.remove(first);
            closeStarted.get(5, TimeUnit.SECONDS);
            assertThrows(TimeoutException.class, () -> closing.get(100, TimeUnit.MILLISECONDS));
            assertEquals(count(registry, "first"), 11L);
            release.complete(null);
            closing.get(5, TimeUnit.SECONDS);
            worker.join(5000);
            assertFalse(worker.isAlive());
            assertEquals(count(registry, "first"), 0L);
            assertEquals(count(registry, "second"), 11L);
            execute(second).get(5, TimeUnit.SECONDS);
            DelayTaskRunner<String> replacement = runner("first");
            runners.add(replacement);
            execute(replacement).get(5, TimeUnit.SECONDS);
            assertEquals(count(registry, "first"), 11L);
        } finally {
            release.complete(null);
            runners.forEach(DelayTaskRunner::shutdown);
            await().atMost(Duration.ofSeconds(5))
                .until(() -> count(registry, "first") + count(registry, "second") == 0);
            Metrics.removeRegistry(registry);
            registry.close();
        }
    }

    @Test
    public void shutdownCancelsFutureTriggerAndStopsWorker() throws Exception {
        DelayTaskRunner<String> runner = runner("future-trigger");
        CompletableFuture<Thread> scheduled = new CompletableFuture<>();
        AtomicBoolean executed = new AtomicBoolean();
        Thread worker;
        try {
            runner.schedule("future", new IDelayedTask<>() {
                @Override
                public Duration getDelay() {
                    scheduled.complete(Thread.currentThread());
                    return Duration.ofSeconds(1);
                }

                @Override
                public CompletableFuture<Void> run(String key, IDelayTaskRunner<String> owner) {
                    executed.set(true);
                    return CompletableFuture.completedFuture(null);
                }
            });
            worker = scheduled.get(5, TimeUnit.SECONDS);
        } finally {
            runner.shutdown();
        }
        worker.join(5000);
        assertFalse(worker.isAlive());
        assertFalse(executed.get());
    }

    private static DelayTaskRunner<String> runner(String range) {
        return new DelayTaskRunner<>(String::compareTo, System::currentTimeMillis, 1000,
            "clusterId", "delay-test", "storeId", "store", "rangeId", range);
    }

    private static CompletableFuture<Thread> execute(DelayTaskRunner<String> runner) {
        return execute(runner, CompletableFuture.completedFuture(null));
    }

    private static CompletableFuture<Thread> execute(DelayTaskRunner<String> runner, CompletableFuture<Void> release) {
        CompletableFuture<Thread> done = new CompletableFuture<>();
        runner.schedule("task", new IDelayedTask<>() {
            @Override
            public Duration getDelay() {
                return Duration.ZERO;
            }

            @Override
            public CompletableFuture<Void> run(String key, IDelayTaskRunner<String> owner) {
                done.complete(Thread.currentThread());
                release.join();
                return CompletableFuture.completedFuture(null);
            }
        });
        return done;
    }

    private static long count(SimpleMeterRegistry registry, String range) {
        return registry.getMeters().stream().filter(meter -> meter.getId().getName().startsWith("inbox.delay.executor")
            && range.equals(meter.getId().getTag("rangeId"))).count();
    }
}
