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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DelayTaskRunnerTest {
    private DelayTaskRunner<String> runner;
    private Supplier<Long> currentMillisSupplier;
    private BiFunction<String, IDelayTaskRunner<String>, CompletableFuture<Void>> taskFunction1;
    private BiFunction<String, IDelayTaskRunner<String>, CompletableFuture<Void>> taskFunction2;

    @BeforeMethod
    public void setUp() {
        currentMillisSupplier = System::currentTimeMillis;
        runner = new DelayTaskRunner<>(String::compareTo, currentMillisSupplier, 1000);
        taskFunction1 = mock(BiFunction.class);
        taskFunction2 = mock(BiFunction.class);
    }

    @Test
    public void testScheduleIfAbsent() {
        TestDelayedTask first = new TestDelayedTask(Duration.ZERO, taskFunction1);
        TestDelayedTask second = new TestDelayedTask(Duration.ZERO, taskFunction2);

        runner.scheduleIfAbsent("absKey", first);
        runner.scheduleIfAbsent("absKey", second);

        verify(taskFunction1, timeout(100).times(1)).apply("absKey", runner);
        verify(taskFunction2, timeout(100).times(0)).apply(anyString(), any());
    }

    @Test
    public void testScheduleIfAbsentNoOverride() {
        TestDelayedTask scheduled = new TestDelayedTask(Duration.ZERO, taskFunction1);
        TestDelayedTask absent = new TestDelayedTask(Duration.ZERO, taskFunction2);

        runner.schedule("absKey2", scheduled);
        runner.scheduleIfAbsent("absKey2", absent);

        verify(taskFunction1, timeout(100).times(1)).apply("absKey2", runner);
        verify(taskFunction2, timeout(100).times(0)).apply(anyString(), any());
    }

    @Test
    public void testImmediateExecution() {
        TestDelayedTask task = new TestDelayedTask(Duration.ZERO, taskFunction1);

        runner.schedule("immediateKey", task);
        verify(taskFunction1, timeout(100).times(1)).apply("immediateKey", runner);
        assertFalse(runner.hasTask("immidateKey"));
    }

    @Test
    public void testDelayedExecution() {
        TestDelayedTask task = new TestDelayedTask(Duration.ofMillis(50), taskFunction1);

        runner.schedule("delayedKey", task);

        verify(taskFunction1, timeout(40).times(0)).apply(anyString(), any());
        verify(taskFunction1, timeout(100).times(1)).apply("delayedKey", runner);
        assertFalse(runner.hasTask("delayedKey"));
    }

    @Test
    public void testScheduleAnyway() {
        TestDelayedTask task1 = new TestDelayedTask(Duration.ofMillis(50), taskFunction1);
        TestDelayedTask task2 = new TestDelayedTask(Duration.ofMillis(50), taskFunction2);

        runner.schedule("ifAbsentKey", task1);
        runner.schedule("ifAbsentKey", task2);

        verify(taskFunction1, timeout(200).times(0)).apply("ifAbsentKey", runner);
        verify(taskFunction2, timeout(200).times(1)).apply("ifAbsentKey", runner);
    }

    @Test
    public void testHasTask() {
        TestDelayedTask task = new TestDelayedTask(Duration.ofMillis(1000), taskFunction1);

        runner.schedule("hasTaskKey", task);
        await().until(() -> runner.hasTask("hasTaskKey"));
        await().until(() -> runner.hasTask("hasTaskKey"));
    }

    @Test
    public void testRateLimiterBehavior() {
        int rateLimit = 2;
        DelayTaskRunner<String> rateLimitedRunner = new DelayTaskRunner<>(String::compareTo, System::currentTimeMillis,
            rateLimit);

        List<Long> executionTimes = Collections.synchronizedList(new ArrayList<>());

        for (int i = 1; i <= 6; i++) {
            String key = "task" + i;
            rateLimitedRunner.schedule(key, new RecordingTask(Duration.ofMillis(10), executionTimes));
        }

        await().atMost(Duration.ofSeconds(10)).until(() -> executionTimes.size() == 6);

        List<Long> times = new ArrayList<>(executionTimes);
        Collections.sort(times);
        long firstTime = times.get(0);
        long thirdTime = times.get(2);
        long fifthTime = times.get(4);

        long diff1 = thirdTime - firstTime;
        long diff2 = fifthTime - firstTime;

        assertTrue(diff1 >= 800);
        assertTrue(diff2 >= 1800);

        rateLimitedRunner.shutdown();

        await().atMost(Duration.ofSeconds(5))
            .until(() -> !rateLimitedRunner.hasTask("task1") && !rateLimitedRunner.hasTask("task2") &&
                !rateLimitedRunner.hasTask("task3") && !rateLimitedRunner.hasTask("task4") &&
                !rateLimitedRunner.hasTask("task5") && !rateLimitedRunner.hasTask("task6"));
    }

    @Test
    public void testCancelAllTasks() {
        TestDelayedTask task1 = new TestDelayedTask(Duration.ofMillis(500), taskFunction1);
        TestDelayedTask task2 = new TestDelayedTask(Duration.ofMillis(500), taskFunction2);

        runner.schedule("cancelKey1", task1);
        runner.schedule("cancelKey2", task2);

        await().until(() -> runner.hasTask("cancelKey1"));
        await().until(() -> runner.hasTask("cancelKey2"));

        runner.cancelAll(Set.of("cancelKey1", "cancelKey2"));

        await().atMost(Duration.ofMillis(200))
            .until(() -> !runner.hasTask("cancelKey1") && !runner.hasTask("cancelKey2"));

        verify(taskFunction1, timeout(600).times(0)).apply("cancelKey1", runner);
        verify(taskFunction2, timeout(600).times(0)).apply("cancelKey2", runner);
    }

    @Test
    public void testShutdown() {
        TestDelayedTask task = new TestDelayedTask(Duration.ofMillis(50), taskFunction1);
        runner.schedule("shutdownKey2", task);
        runner.shutdown();
        verify(taskFunction1, timeout(100).times(0)).apply("shutdownKey2", runner);
    }

    private static class RecordingTask implements IDelayedTask<String> {
        private final Duration delay;
        private final List<Long> executionTimes;

        RecordingTask(Duration delay, List<Long> executionTimes) {
            this.delay = delay;
            this.executionTimes = executionTimes;
        }

        @Override
        public Duration getDelay() {
            return delay;
        }

        @Override
        public CompletableFuture<Void> run(String key, IDelayTaskRunner<String> runner) {
            executionTimes.add(System.currentTimeMillis());
            return CompletableFuture.completedFuture(null);
        }
    }

    private static class TestDelayedTask implements IDelayedTask<String> {
        private final Duration delay;
        private final BiFunction<String, IDelayTaskRunner<String>, CompletableFuture<Void>> taskFunction;

        private TestDelayedTask(Duration delay,
                                BiFunction<String, IDelayTaskRunner<String>, CompletableFuture<Void>> taskFunction) {
            this.delay = delay;
            this.taskFunction = taskFunction;
        }

        @Override
        public Duration getDelay() {
            return delay;
        }

        @Override
        public CompletableFuture<Void> run(String key, IDelayTaskRunner<String> runner) {
            return taskFunction.apply(key, runner);
        }
    }
}
