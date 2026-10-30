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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MonitoredExecutorsTest {
    private final List<ExecutorService> executors = new ArrayList<>();
    private final List<SimpleMeterRegistry> registries = new ArrayList<>();
    private SimpleMeterRegistry registry;

    @BeforeMethod
    public void setup() {
        registry = registry(new SimpleMeterRegistry());
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws InterruptedException {
        for (int i = executors.size() - 1; i >= 0; i--) {
            executors.get(i).shutdownNow();
        }
        for (ExecutorService executor : executors) {
            assertTrue(executor.awaitTermination(5, SECONDS));
        }
        registries.forEach(SimpleMeterRegistry::close);
        executors.clear();
        registries.clear();
    }

    @DataProvider
    public Object[][] poolKinds() {
        return new Object[][] {{0, 9}, {1, 11}, {2, 6}};
    }

    @DataProvider
    public Object[][] shutdownPaths() {
        return new Object[][] {{0, 9, false}, {0, 9, true}, {1, 11, false}, {1, 11, true},
            {2, 6, false}, {2, 6, true}};
    }

    @Test(dataProvider = "shutdownPaths")
    public void removesAllMeters(int kind, int expected, boolean immediate) throws Exception {
        ExecutorService service = monitor(pool(kind), "test");
        service.submit(() -> 7).get(5, SECONDS);
        assertEquals(registry.getMeters().size(), expected);
        if (immediate) {
            service.shutdownNow();
        } else {
            service.shutdown();
        }
        assertTrue(service.isShutdown());
        assertTrue(service.awaitTermination(5, SECONDS));
        assertTrue(service.isTerminated());
        assertTrue(registry.getMeters().isEmpty());
        service.shutdown();
        service.shutdownNow();
        assertTrue(registry.getMeters().isEmpty());
    }

    @Test(dataProvider = "poolKinds")
    public void keepsMicrometerMeterDefinitions(int kind, int count) {
        ExecutorService raw = pool(kind);
        SimpleMeterRegistry baseline = registry(new SimpleMeterRegistry());
        ExecutorService legacy = watch(ExecutorServiceMetrics.monitor(baseline, raw, "test", "prefix",
            Tags.of("store", "s", "name", "ignored")));
        ExecutorService current = watch(MonitoredExecutors.monitor(registry, raw, "test", "prefix.",
            Tags.of("name", "ignored", "store", "s")));
        assertEquals(meterDefinitions(registry), meterDefinitions(baseline));
        assertEquals(registry.getMeters().size(), count);
        assertEquals(current instanceof ScheduledExecutorService, kind == 1);
        legacy.shutdown();
        current.shutdown();
    }

    @Test
    public void normalizesPrefixAndTagsBeforeClaimingIdentity() {
        ExecutorService first = watch(MonitoredExecutors.monitor(registry, pool(0), "test", "prefix",
            Tags.of("b", "2", "a", "1")));
        ExecutorService candidate = pool(1);
        expectThrows(IllegalArgumentException.class, () -> MonitoredExecutors.monitor(registry, candidate, "test",
            "prefix.", Tags.of("a", "1", "b", "2")));
        assertFalse(candidate.isShutdown());
        assertEquals(registry.getMeters().size(), 9);
        first.shutdown();
        watch(MonitoredExecutors.monitor(registry, candidate, "test", "prefix.", Tags.of("b", "2", "a", "1")));
        assertEquals(registry.getMeters().size(), 11);
    }

    @Test
    public void normalizesEmptyPrefixes() {
        ExecutorService first = watch(MonitoredExecutors.monitor(registry, pool(0), "test", null, Tags.empty()));
        ExecutorService candidate = pool(0);
        expectThrows(IllegalArgumentException.class, () -> MonitoredExecutors.monitor(registry, candidate, "test",
            "  ", Tags.empty()));
        first.shutdown();
        watch(MonitoredExecutors.monitor(registry, candidate, "test", Tags.empty()));
        assertNotNull(registry.find("executor").timer());
    }

    @Test
    public void isolatesRegistriesNamesTagsAndUnrelatedMeters() throws Exception {
        SimpleMeterRegistry otherRegistry = registry(new SimpleMeterRegistry());
        ExecutorService first = monitor(pool(0), "first");
        ExecutorService second = monitor(pool(0), "second");
        ExecutorService other = watch(MonitoredExecutors.monitor(otherRegistry, pool(0), "first"));
        ExecutorService tagged = watch(MonitoredExecutors.monitor(registry, pool(0), "first", Tags.of("store", "s")));
        Counter business = registry.counter("executor.business", "name", "first");
        first.shutdown();
        second.submit(() -> 1).get(5, SECONDS);
        other.submit(() -> 1).get(5, SECONDS);
        tagged.submit(() -> 1).get(5, SECONDS);
        business.increment();
        assertEquals(registry.get("executor").tag("name", "second").timer().count(), 1L);
        assertEquals(registry.get("executor").tag("store", "s").timer().count(), 1L);
        assertEquals(otherRegistry.get("executor").timer().count(), 1L);
        assertEquals(business.count(), 1.0);
        assertEquals(registry.getMeters().size(), 19);
    }

    @Test
    public void rejectsUnsupportedAndRepeatedWrappersBeforeRegistration() {
        ExecutorService decorated = watch(Executors.newSingleThreadExecutor());
        expectThrows(IllegalArgumentException.class, () -> MonitoredExecutors.monitor(registry, decorated, "decorated"));
        assertTrue(registry.getMeters().isEmpty());
        assertFalse(decorated.isShutdown());
        ExecutorService first = monitor(pool(0), "first");
        expectThrows(IllegalArgumentException.class, () -> MonitoredExecutors.monitor(registry, first, "second"));
        assertEquals(registry.getMeters().size(), 9);
    }

    @Test
    public void oldTasksAndRepeatedShutdownCannotAffectReplacement() throws Exception {
        ExecutorService old = monitor(pool(0), "test");
        Timer oldTimer = registry.get("executor").timer();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        Future<?> running = old.submit(() -> {
            started.countDown();
            await(finish);
        });
        await(started);
        old.shutdown();
        assertFalse(old.isTerminated());
        assertFalse(old.awaitTermination(0, SECONDS));
        assertTrue(registry.getMeters().isEmpty());
        ThreadPoolExecutor rawReplacement = watch(new ThreadPoolExecutor(3, 3, 0, SECONDS,
            new LinkedBlockingQueue<>()));
        ExecutorService replacement = monitor(rawReplacement, "test");
        Timer newTimer = registry.get("executor").timer();
        assertEquals(registry.get("executor.pool.core").gauge().value(), 3.0);
        assertEquals(newTimer.count(), 0L);
        finish.countDown();
        running.get(5, SECONDS);
        assertTrue(old.awaitTermination(5, SECONDS));
        assertEquals(oldTimer.count(), 1L);
        assertEquals(newTimer.count(), 0L);
        old.shutdownNow();
        old.shutdown();
        assertSame(registry.get("executor").timer(), newTimer);
        replacement.submit(() -> 1).get(5, SECONDS);
        assertEquals(newTimer.count(), 1L);
    }

    @Test
    public void shutdownNowStillInterruptsAfterShutdown() throws Exception {
        ExecutorService service = monitor(pool(0), "test");
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        service.submit(() -> {
            started.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                interrupted.countDown();
                Thread.currentThread().interrupt();
            }
        });
        await(started);
        service.shutdown();
        service.shutdownNow();
        await(interrupted);
        assertTrue(service.awaitTermination(5, SECONDS));
    }

    @Test
    public void rejectsConcurrentOwners() throws Exception {
        ExecutorService callers = watch(Executors.newFixedThreadPool(2));
        ExecutorService firstPool = pool(0);
        ExecutorService secondPool = pool(0);
        CyclicBarrier start = new CyclicBarrier(2);
        List<Future<ExecutorService>> results = new ArrayList<>();
        for (ExecutorService raw : List.of(firstPool, secondPool)) {
            results.add(callers.submit(() -> {
                start.await(5, SECONDS);
                try {
                    return MonitoredExecutors.monitor(registry, raw, "same");
                } catch (IllegalArgumentException e) {
                    return null;
                }
            }));
        }
        int owners = 0;
        for (Future<ExecutorService> result : results) {
            ExecutorService service = result.get(5, SECONDS);
            if (service != null) {
                watch(service);
                owners++;
            }
        }
        assertEquals(owners, 1);
        assertEquals(registry.getMeters().size(), 9);
    }

    @Test
    public void doesNotReleaseIdentityBeforeRemovalCompletes() throws Exception {
        ExecutorService old = monitor(pool(0), "test");
        ExecutorService replacementPool = pool(0);
        ExecutorService callers = watch(Executors.newFixedThreadPool(2));
        CountDownLatch removing = new CountDownLatch(1);
        CountDownLatch allowRemoval = new CountDownLatch(1);
        AtomicInteger removed = new AtomicInteger();
        registry.config().onMeterRemoved(meter -> {
            if (removed.getAndIncrement() == 0) {
                removing.countDown();
                await(allowRemoval);
            }
        });
        Future<?> closing = callers.submit(old::shutdown);
        await(removing);
        CountDownLatch registering = new CountDownLatch(1);
        Future<ExecutorService> creating = callers.submit(() -> {
            registering.countDown();
            return MonitoredExecutors.monitor(registry, replacementPool, "test");
        });
        try {
            await(registering);
            assertFalse(creating.isDone());
        } finally {
            allowRemoval.countDown();
        }
        closing.get(5, SECONDS);
        ExecutorService replacement = watch(creating.get(5, SECONDS));
        assertEquals(registry.getMeters().size(), 9);
        replacement.submit(() -> 1).get(5, SECONDS);
        old.shutdownNow();
        assertEquals(registry.get("executor").timer().count(), 1L);
    }

    @Test
    public void closesDifferentOwnersConcurrently() throws Exception {
        ExecutorService callers = watch(Executors.newFixedThreadPool(3));
        CyclicBarrier registered = new CyclicBarrier(3);
        List<Future<?>> results = new ArrayList<>();
        for (int kind = 0; kind < 3; kind++) {
            ExecutorService raw = pool(kind);
            String name = "owner-" + kind;
            results.add(callers.submit(() -> {
                ExecutorService service = MonitoredExecutors.monitor(registry, raw, name);
                try {
                    registered.await(5, SECONDS);
                    assertEquals(service.submit(() -> name).get(5, SECONDS), name);
                } finally {
                    service.shutdown();
                }
                return null;
            }));
        }
        for (Future<?> result : results) {
            result.get(5, SECONDS);
        }
        assertTrue(registry.getMeters().isEmpty());
    }

    @Test(dataProvider = "poolKinds")
    public void rollsBackRegistrationWithoutClosingCallerPool(int kind, int expectedMeters) throws Exception {
        Gauge conflict = Gauge.builder("executor.idle", () -> 1).tag("name", "test").register(registry);
        ExecutorService raw = pool(kind);
        assertEquals(raw.submit(() -> 1).get(5, SECONDS), Integer.valueOf(1));
        expectThrows(IllegalArgumentException.class, () -> MonitoredExecutors.monitor(registry, raw, "test"));
        assertEquals(registry.getMeters(), List.of(conflict));
        assertFalse(raw.isShutdown());
        assertEquals(raw.submit(() -> 2).get(5, SECONDS), Integer.valueOf(2));
        registry.remove(conflict);
        watch(MonitoredExecutors.monitor(registry, raw, "test"));
        assertEquals(registry.getMeters().size(), expectedMeters);
    }

    @Test
    public void preservesPendingList() throws Exception {
        PendingExecutor raw = watch(new PendingExecutor());
        ExecutorService service = monitor(raw, "test");
        CountDownLatch started = new CountDownLatch(1);
        service.submit(() -> {
            started.countDown();
            new CountDownLatch(1).await();
            return null;
        });
        await(started);
        Future<?> pendingTask = service.submit(() -> 1);
        List<Runnable> pending = service.shutdownNow();
        assertSame(pending, raw.pending);
        assertEquals(pending.size(), 1);
        assertSame(pending.get(0), pendingTask);
        assertFalse(pendingTask.isCancelled());
        assertFalse(pendingTask.isDone());
        assertTrue(registry.getMeters().isEmpty());
        monitor(pool(0), "test");
        assertEquals(registry.getMeters().size(), 9);
    }

    @Test
    public void rejectsNullTasksBeforeSubmission() {
        ExecutorService service = monitor(pool(0), "test");
        expectThrows(NullPointerException.class, () -> service.execute(null));
        expectThrows(NullPointerException.class, () -> service.submit((Runnable) null));
        expectThrows(NullPointerException.class, () -> service.submit((Callable<?>) null));
        assertEquals(registry.get("executor").timer().count(), 0L);
        assertEquals(registry.get("executor.pool.size").gauge().value(), 0.0);
    }

    @Test
    public void supportsDeniedMeters() {
        registry.config().meterFilter(MeterFilter.deny());
        ExecutorService first = monitor(pool(0), "test");
        assertTrue(registry.getMeters().isEmpty());
        first.shutdown();
        monitor(pool(0), "test").shutdownNow();
    }

    @Test(dataProvider = "poolKinds")
    public void forwardsSubmitResultsAndTaskFailures(int kind, int ignored) throws Exception {
        ExecutorService service = monitor(pool(kind), "test");
        AtomicInteger runs = new AtomicInteger();
        assertEquals(service.submit((Callable<Integer>) () -> 7).get(5, SECONDS), Integer.valueOf(7));
        assertEquals(service.submit(runs::incrementAndGet, "result").get(5, SECONDS), "result");
        assertEquals(service.submit((Runnable) runs::incrementAndGet).get(5, SECONDS), null);
        IllegalArgumentException failure = new IllegalArgumentException("Task failed");
        ExecutionException thrown = expectThrows(ExecutionException.class,
            () -> service.submit(() -> { throw failure; }).get(5, SECONDS));
        // ForkJoinPool can copy task exceptions when another thread reads the result.
        assertTrue(thrown.getCause() instanceof IllegalArgumentException);
        assertEquals(runs.get(), 2);
        assertEquals(registry.get("executor").timer().count(), 4L);
        assertEquals(registry.get("executor.idle").timer().count(), 4L);
    }

    @Test
    public void recordsWaitingAndExecutionSeparately() throws Exception {
        MockClock clock = new MockClock();
        SimpleMeterRegistry timedRegistry = registry(new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock));
        ThreadPoolExecutor raw = (ThreadPoolExecutor) pool(0);
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        raw.execute(() -> { blocked.countDown(); await(release); });
        await(blocked);
        ExecutorService service = watch(MonitoredExecutors.monitor(timedRegistry, raw, "test"));
        Future<?> task = service.submit(() -> clock.add(Duration.ofSeconds(2)));
        clock.add(Duration.ofSeconds(3));
        release.countDown();
        task.get(5, SECONDS);
        assertEquals(timedRegistry.get("executor.idle").timer().totalTime(SECONDS), 3.0);
        assertEquals(timedRegistry.get("executor").timer().totalTime(SECONDS), 2.0);
    }

    @Test
    public void forwardsCancellationAndCallerRunsRejectionPolicy() throws Exception {
        ThreadPoolExecutor raw = watch(new ThreadPoolExecutor(1, 1, 0, SECONDS, new ArrayBlockingQueue<>(1),
            new ThreadPoolExecutor.CallerRunsPolicy()));
        ExecutorService service = monitor(raw, "test");
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        Future<?> running = service.submit(() -> {
            started.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                interrupted.countDown();
                Thread.currentThread().interrupt();
            }
        });
        await(started);
        Future<?> queued = service.submit(() -> 2);
        Thread caller = Thread.currentThread();
        assertSame(service.submit(Thread::currentThread).get(5, SECONDS), caller);
        assertEquals(registry.get("executor").timer().count(), 1L);
        assertTrue(queued.cancel(false));
        assertTrue(running.cancel(true));
        await(interrupted);
        assertTrue(running.isCancelled());
    }

    @Test
    public void forwardsBatchResultsTimeoutsAndInterrupts() throws Exception {
        ExecutorService service = monitor(pool(0), "test");
        List<Callable<Integer>> tasks = List.of(() -> 1, () -> 2);
        List<Future<Integer>> all = service.invokeAll(tasks);
        assertEquals(all.get(0).get(), Integer.valueOf(1));
        assertEquals(all.get(1).get(), Integer.valueOf(2));
        assertEquals(service.invokeAll(tasks, 5, SECONDS).get(1).get(), Integer.valueOf(2));
        assertEquals(service.invokeAny(List.of(() -> { throw new IllegalStateException(); }, () -> 3)), Integer.valueOf(3));
        assertEquals(service.invokeAny(tasks, 5, SECONDS), Integer.valueOf(1));
        Callable<Integer> blocked = () -> { new CountDownLatch(1).await(); return 0; };
        List<Future<Integer>> timedOut = service.invokeAll(List.of(blocked, blocked), 20, MILLISECONDS);
        assertTrue(timedOut.stream().allMatch(Future::isCancelled));
        expectThrows(TimeoutException.class, () -> service.invokeAny(List.of(blocked), 20, MILLISECONDS));
        Thread.currentThread().interrupt();
        try {
            expectThrows(InterruptedException.class, () -> service.invokeAll(List.of(blocked)));
        } finally {
            Thread.interrupted();
        }
        service.shutdown();
        expectThrows(RejectedExecutionException.class, () -> service.submit(() -> 1));
    }

    @Test
    public void schedulesWithoutCountingDelayAsIdleTime() throws Exception {
        MockClock clock = new MockClock();
        SimpleMeterRegistry timedRegistry = registry(new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock));
        ScheduledThreadPoolExecutor raw = watch(new ScheduledThreadPoolExecutor(1));
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        raw.execute(() -> { blocked.countDown(); await(release); });
        await(blocked);
        ScheduledExecutorService service = watch(MonitoredExecutors.monitor(timedRegistry, raw, "test"));
        ScheduledFuture<Integer> task = service.schedule(() -> {
            clock.add(Duration.ofSeconds(2));
            return 7;
        }, 1, MILLISECONDS);
        clock.add(Duration.ofHours(1));
        release.countDown();
        assertEquals(task.get(5, SECONDS), Integer.valueOf(7));
        service.schedule(() -> { }, 0, MILLISECONDS).get(5, SECONDS);
        assertEquals(timedRegistry.get("executor.idle").timer().count(), 0L);
        assertEquals(timedRegistry.get("executor").timer().totalTime(SECONDS), 2.0);
        assertEquals(timedRegistry.get("executor.scheduled.once").counter().count(), 2.0);
    }

    @DataProvider
    public Object[][] periodicKinds() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "periodicKinds")
    public void preservesPeriodicFailureAndCancellation(boolean fixedRate) throws Exception {
        ScheduledExecutorService service = watch(MonitoredExecutors.monitor(registry,
            watch(new ScheduledThreadPoolExecutor(1)), "test", Tags.empty()));
        AtomicInteger calls = new AtomicInteger();
        Runnable failing = () -> {
            if (calls.incrementAndGet() == 3) {
                throw new IllegalStateException("Periodic task failed");
            }
        };
        ScheduledFuture<?> future = fixedRate ? service.scheduleAtFixedRate(failing, 0, 1, MILLISECONDS)
            : service.scheduleWithFixedDelay(failing, 0, 1, MILLISECONDS);
        expectThrows(ExecutionException.class, () -> future.get(5, SECONDS));
        assertEquals(calls.get(), 3);
        assertEquals(registry.get("executor").timer().count(), 3L);
        assertEquals(registry.get("executor.idle").timer().count(), 0L);
        ScheduledFuture<?> cancelled = fixedRate ? service.scheduleAtFixedRate(failing, 1, 1, SECONDS)
            : service.scheduleWithFixedDelay(failing, 1, 1, SECONDS);
        assertTrue(cancelled.cancel(false));
        assertTrue(cancelled.isCancelled());
        assertEquals(registry.get("executor.scheduled.repetitively").counter().count(), 2.0);
    }

    @Test
    public void countsRejectedSchedulingCallsWithoutRegisteringMetersAgain() {
        ScheduledExecutorService service = watch(MonitoredExecutors.monitor(registry,
            watch(new ScheduledThreadPoolExecutor(1)), "test"));
        Counter once = registry.get("executor.scheduled.once").counter();
        Counter repeating = registry.get("executor.scheduled.repetitively").counter();
        service.shutdown();
        expectThrows(RejectedExecutionException.class, () -> service.schedule(() -> { }, 0, SECONDS));
        expectThrows(RejectedExecutionException.class, () -> service.schedule(() -> 1, 0, SECONDS));
        expectThrows(RejectedExecutionException.class, () -> service.scheduleAtFixedRate(() -> { }, 0, 1, SECONDS));
        expectThrows(RejectedExecutionException.class, () -> service.scheduleWithFixedDelay(() -> { }, 0, 1, SECONDS));
        assertEquals(once.count(), 2.0);
        assertEquals(repeating.count(), 2.0);
        assertTrue(registry.getMeters().isEmpty());
    }

    @Test
    public void supportsPrefixOnlyOverloads() {
        ExecutorService regular = watch(MonitoredExecutors.monitor(registry, pool(0), "test", "regular"));
        ScheduledExecutorService scheduled = watch(MonitoredExecutors.monitor(registry,
            watch(new ScheduledThreadPoolExecutor(1)), "test", "scheduled"));
        assertNotNull(registry.find("regular.executor").timer());
        assertNotNull(registry.find("scheduled.executor.scheduled.once").counter());
        regular.shutdown();
        scheduled.shutdown();
        assertTrue(registry.getMeters().isEmpty());
    }

    @Test
    public void executeKeepsUncaughtExceptionBehaviorAndRecordsTime() throws Exception {
        CompletableFuture<Throwable> uncaught = new CompletableFuture<>();
        ThreadPoolExecutor raw = watch(new ThreadPoolExecutor(1, 1, 0, SECONDS, new LinkedBlockingQueue<>(), task -> {
            Thread worker = new Thread(task);
            worker.setUncaughtExceptionHandler((thread, error) -> uncaught.complete(error));
            return worker;
        }));
        ExecutorService service = monitor(raw, "test");
        IllegalStateException failure = new IllegalStateException("Task failed");
        service.execute(() -> { throw failure; });
        assertSame(uncaught.get(5, SECONDS), failure);
        assertEquals(registry.get("executor").timer().count(), 1L);
        service.submit(() -> 1).get(5, SECONDS);
        assertEquals(registry.get("executor").timer().count(), 2L);
    }

    @Test
    public void batchFailuresKeepTheirResults() throws Exception {
        ExecutorService service = monitor(pool(0), "test");
        Callable<Integer> failing = () -> { throw new IllegalStateException("Task failed"); };
        List<Future<Integer>> results = service.invokeAll(List.of(failing, () -> 7));
        expectThrows(ExecutionException.class, () -> results.get(0).get());
        assertEquals(results.get(1).get(), Integer.valueOf(7));
        expectThrows(ExecutionException.class, () -> service.invokeAny(List.of(failing)));
        expectThrows(ExecutionException.class, () -> service.invokeAny(List.of(failing), 5, SECONDS));
    }

    @Test
    public void rollsBackTimersWhenScheduledCounterRegistrationFails() {
        Gauge conflict = Gauge.builder("executor.scheduled.repetitively", () -> 1)
            .tag("name", "test").register(registry);
        ScheduledThreadPoolExecutor raw = watch(new ScheduledThreadPoolExecutor(1));
        expectThrows(IllegalArgumentException.class, () -> MonitoredExecutors.monitor(registry, raw, "test"));
        assertEquals(registry.getMeters(), List.of(conflict));
        registry.remove(conflict);
        watch(MonitoredExecutors.monitor(registry, raw, "test"));
        assertEquals(registry.getMeters().size(), 11);
    }

    private ExecutorService monitor(ExecutorService raw, String name) {
        return watch(MonitoredExecutors.monitor(registry, raw, name));
    }

    private ExecutorService pool(int kind) {
        return switch (kind) {
            case 1 -> watch(new ScheduledThreadPoolExecutor(1));
            case 2 -> watch(new ForkJoinPool(1));
            default -> watch(new ThreadPoolExecutor(1, 1, 0, SECONDS, new LinkedBlockingQueue<>()));
        };
    }

    private <T extends ExecutorService> T watch(T executor) {
        executors.add(executor);
        return executor;
    }

    private <T extends SimpleMeterRegistry> T registry(T meterRegistry) {
        registries.add(meterRegistry);
        return meterRegistry;
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, SECONDS), "Task did not reach the expected state");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static Map<String, List<Object>> meterDefinitions(SimpleMeterRegistry registry) {
        Map<String, List<Object>> definitions = new TreeMap<>();
        for (Meter meter : registry.getMeters()) {
            Meter.Id id = meter.getId();
            definitions.put(id.getName(), Arrays.asList(id.getType(), id.getTags(), id.getBaseUnit(), id.getDescription()));
        }
        return definitions;
    }

    private static final class PendingExecutor extends ThreadPoolExecutor {
        private List<Runnable> pending;

        private PendingExecutor() {
            super(1, 1, 0, SECONDS, new LinkedBlockingQueue<>());
        }

        @Override
        public List<Runnable> shutdownNow() {
            pending = super.shutdownNow();
            return pending;
        }
    }

}
