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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Registers executor meters and removes them when the executor shuts down.
 *
 * <p>The caller must own the executor and use the returned service for tasks and shutdown.
 * Shutdown removes meters before queued tasks finish. Use awaitTermination to wait for those tasks.
 * Each registry, prefix, and tag set must have one owner. External registrations and meter filters
 * must not merge the meter identities of different owners.
 *
 * <p>Meter definitions follow Micrometer 1.11.12 ExecutorServiceMetrics (Copyright 2017 VMware, Inc.),
 * under the Apache License, Version 2.0. Task timing follows its TimedExecutorService and
 * TimedScheduledExecutorService (Copyright 2017, 2019 VMware, Inc.).
 * This implementation adds meter ownership and shutdown cleanup.
 * See https://github.com/micrometer-metrics/micrometer/tree/v1.11.12/micrometer-core.
 */
public final class MonitoredExecutors {
    // Registration and removal share this lock. Task submission and execution do not use it.
    private static final Map<MeterRegistry, Map<Identity, Binding>> OWNERS = new IdentityHashMap<>();

    private MonitoredExecutors() {
    }

    /**
     * Monitors an executor without a prefix or caller tags.
     */
    public static ExecutorService monitor(MeterRegistry registry, ExecutorService executor, String name) {
        return monitor(registry, executor, name, "", Tags.empty());
    }

    /**
     * Monitors an executor with caller tags and no prefix.
     */
    public static ExecutorService monitor(MeterRegistry registry, ExecutorService executor, String name,
                                          Iterable<Tag> tags) {
        return monitor(registry, executor, name, "", tags);
    }

    /**
     * Monitors an executor with a prefix and no caller tags.
     */
    public static ExecutorService monitor(MeterRegistry registry, ExecutorService executor, String name,
                                          String metricPrefix) {
        return monitor(registry, executor, name, metricPrefix, Tags.empty());
    }

    /**
     * Monitors a ThreadPoolExecutor, ScheduledThreadPoolExecutor, or owned ForkJoinPool.
     *
     * <p>Unsupported executors and duplicate identities fail before registration. Registration failure
     * rolls back the acquired meters. The caller remains responsible for the supplied executor.
     */
    public static ExecutorService monitor(MeterRegistry registry, ExecutorService executor, String name,
                                          String metricPrefix, Iterable<Tag> tags) {
        if (!(executor instanceof ThreadPoolExecutor) && !(executor instanceof ForkJoinPool)) {
            throw new IllegalArgumentException("Executor type is not supported: " + executor.getClass().getName());
        }
        String prefix = metricPrefix == null || metricPrefix.isBlank() ? ""
            : metricPrefix.endsWith(".") ? metricPrefix : metricPrefix + ".";
        Identity identity = new Identity(prefix, Tags.concat(tags, "name", name));
        synchronized (OWNERS) {
            Map<Identity, Binding> registryOwners = OWNERS.computeIfAbsent(registry, k -> new HashMap<>());
            if (registryOwners.containsKey(identity)) {
                throw new IllegalArgumentException("Executor meters already have an owner: " + identity);
            }
            Binding binding = new Binding(registry, identity);
            registryOwners.put(identity, binding);
            try {
                binding.register(executor);
                Timing timing = new Timing(registry.config().clock(), binding.executionTimer, binding.idleTimer);
                if (executor instanceof ScheduledThreadPoolExecutor scheduled) {
                    return new MonitoredScheduler(scheduled, binding, timing);
                }
                return new MonitoredExecutor(executor, binding, timing);
            } catch (RuntimeException failure) {
                binding.removeMeters();
                throw failure;
            }
        }
    }

    /**
     * Monitors a scheduled executor without a prefix or caller tags.
     */
    public static ScheduledExecutorService monitor(MeterRegistry registry, ScheduledExecutorService executor,
                                                   String name) {
        return monitor(registry, executor, name, "", Tags.empty());
    }

    /**
     * Monitors a scheduled executor with caller tags and no prefix.
     */
    public static ScheduledExecutorService monitor(MeterRegistry registry, ScheduledExecutorService executor,
                                                   String name, Iterable<Tag> tags) {
        return monitor(registry, executor, name, "", tags);
    }

    /**
     * Monitors a scheduled executor with a prefix and no caller tags.
     */
    public static ScheduledExecutorService monitor(MeterRegistry registry, ScheduledExecutorService executor,
                                                   String name, String metricPrefix) {
        return monitor(registry, executor, name, metricPrefix, Tags.empty());
    }

    /**
     * Monitors a scheduled executor and preserves its scheduling interface.
     */
    public static ScheduledExecutorService monitor(MeterRegistry registry, ScheduledExecutorService executor,
                                                   String name, String metricPrefix, Iterable<Tag> tags) {
        return (ScheduledExecutorService) monitor(registry, (ExecutorService) executor, name, metricPrefix, tags);
    }

    private record Identity(String prefix, Tags tags) {
    }

    private static final class Binding {
        private final MeterRegistry registry;
        private final Identity identity;
        private final List<Meter> meters = new ArrayList<>(11);
        private Timer executionTimer;
        private Timer idleTimer;
        private Counter scheduledOnce;
        private Counter scheduledRepetitively;
        private boolean removed;

        private Binding(MeterRegistry registry, Identity identity) {
            this.registry = registry;
            this.identity = identity;
        }

        private void register(ExecutorService executor) {
            String prefix = identity.prefix();
            Tags tags = identity.tags();
            if (executor instanceof ThreadPoolExecutor pool) {
                meters.add(FunctionCounter.builder(prefix + "executor.completed", pool,
                        ThreadPoolExecutor::getCompletedTaskCount)
                    .tags(tags)
                    .description("The approximate total number of tasks that have completed execution")
                    .baseUnit("tasks").register(registry));
                meters.add(Gauge.builder(prefix + "executor.active", pool, ThreadPoolExecutor::getActiveCount)
                    .tags(tags)
                    .description("The approximate number of threads that are actively executing tasks")
                    .baseUnit("threads").register(registry));
                meters.add(Gauge.builder(prefix + "executor.queued", pool, p -> p.getQueue().size())
                    .tags(tags)
                    .description("The approximate number of tasks that are queued for execution")
                    .baseUnit("tasks").register(registry));
                meters.add(Gauge.builder(prefix + "executor.queue.remaining", pool,
                        p -> p.getQueue().remainingCapacity())
                    .tags(tags)
                    .description("The number of additional elements that this queue can ideally accept without "
                        + "blocking")
                    .baseUnit("tasks").register(registry));
                meters.add(Gauge.builder(prefix + "executor.pool.size", pool, ThreadPoolExecutor::getPoolSize)
                    .tags(tags).description("The current number of threads in the pool")
                    .baseUnit("threads").register(registry));
                meters.add(Gauge.builder(prefix + "executor.pool.core", pool, ThreadPoolExecutor::getCorePoolSize)
                    .tags(tags).description("The core number of threads for the pool")
                    .baseUnit("threads").register(registry));
                meters.add(Gauge.builder(prefix + "executor.pool.max", pool, ThreadPoolExecutor::getMaximumPoolSize)
                    .tags(tags).description("The maximum allowed number of threads in the pool")
                    .baseUnit("threads").register(registry));
            } else {
                ForkJoinPool pool = (ForkJoinPool) executor;
                meters.add(FunctionCounter.builder(prefix + "executor.steals", pool, ForkJoinPool::getStealCount)
                    .tags(tags)
                    .description("Estimate of the total number of tasks stolen from "
                        + "one thread's work queue by another. The reported value "
                        + "underestimates the actual total number of steals when the pool is not quiescent")
                    .register(registry));
                meters.add(Gauge.builder(prefix + "executor.queued", pool, ForkJoinPool::getQueuedTaskCount)
                    .tags(tags)
                    .description("An estimate of the total number of tasks currently held in queues by worker threads")
                    .register(registry));
                meters.add(Gauge.builder(prefix + "executor.active", pool, ForkJoinPool::getActiveThreadCount)
                    .tags(tags)
                    .description("An estimate of the number of threads that are currently stealing or executing tasks")
                    .register(registry));
                meters.add(Gauge.builder(prefix + "executor.running", pool, ForkJoinPool::getRunningThreadCount)
                    .tags(tags)
                    .description("An estimate of the number of worker threads that are not blocked waiting to join "
                        + "tasks or for other managed synchronization threads")
                    .register(registry));
            }
            executionTimer = Timer.builder(prefix + "executor").tags(tags).register(registry);
            meters.add(executionTimer);
            idleTimer = Timer.builder(prefix + "executor.idle").tags(tags).register(registry);
            meters.add(idleTimer);
            if (executor instanceof ScheduledThreadPoolExecutor) {
                scheduledOnce = Counter.builder(prefix + "executor.scheduled.once").tags(tags).register(registry);
                meters.add(scheduledOnce);
                scheduledRepetitively = Counter.builder(prefix + "executor.scheduled.repetitively")
                    .tags(tags).register(registry);
                meters.add(scheduledRepetitively);
            }
        }

        private void close() {
            synchronized (OWNERS) {
                removeMeters();
            }
        }

        // The caller holds OWNERS until removal finishes and the identity becomes available.
        private void removeMeters() {
            if (removed) {
                return;
            }
            meters.forEach(registry::remove);
            meters.clear();
            Map<Identity, Binding> registryOwners = OWNERS.get(registry);
            registryOwners.remove(identity);
            if (registryOwners.isEmpty()) {
                OWNERS.remove(registry);
            }
            removed = true;
        }
    }

    private static class MonitoredExecutor implements ExecutorService {
        private final ExecutorService delegate;
        private final Binding binding;
        private final Timing timing;

        private MonitoredExecutor(ExecutorService delegate, Binding binding, Timing timing) {
            this.delegate = delegate;
            this.binding = binding;
            this.timing = timing;
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
            binding.close();
        }

        @Override
        public List<Runnable> shutdownNow() {
            List<Runnable> pending = delegate.shutdownNow();
            binding.close();
            return pending;
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(timing.wrap(command));
        }

        @Override
        public Future<?> submit(Runnable task) {
            return delegate.submit(timing.wrap(task));
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return delegate.submit(timing.wrap(task), result);
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return delegate.submit(timing.wrap(task));
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
            return delegate.invokeAll(wrapAll(tasks));
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
            return delegate.invokeAll(wrapAll(tasks), timeout, unit);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException {
            return delegate.invokeAny(wrapAll(tasks));
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
            return delegate.invokeAny(wrapAll(tasks), timeout, unit);
        }

        private <T> Collection<? extends Callable<T>> wrapAll(Collection<? extends Callable<T>> tasks) {
            List<Callable<T>> wrapped = new ArrayList<>(tasks.size());
            for (Callable<T> task : tasks) {
                wrapped.add(timing.wrap(task));
            }
            return wrapped;
        }
    }

    private static final class MonitoredScheduler extends MonitoredExecutor implements ScheduledExecutorService {
        private final ScheduledExecutorService scheduler;
        private final Timer executionTimer;
        private final Counter scheduledOnce;
        private final Counter scheduledRepetitively;

        private MonitoredScheduler(ScheduledExecutorService scheduler, Binding binding, Timing timing) {
            super(scheduler, binding, timing);
            this.scheduler = scheduler;
            executionTimer = binding.executionTimer;
            scheduledOnce = binding.scheduledOnce;
            scheduledRepetitively = binding.scheduledRepetitively;
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            scheduledOnce.increment();
            return scheduler.schedule(executionTimer.wrap(command), delay, unit);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            scheduledOnce.increment();
            return scheduler.schedule(executionTimer.wrap(callable), delay, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period,
                                                       TimeUnit unit) {
            scheduledRepetitively.increment();
            return scheduler.scheduleAtFixedRate(executionTimer.wrap(command), initialDelay, period, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
                                                         TimeUnit unit) {
            scheduledRepetitively.increment();
            return scheduler.scheduleWithFixedDelay(executionTimer.wrap(command), initialDelay, delay, unit);
        }
    }

    private record Timing(Clock clock, Timer executionTimer, Timer idleTimer) {
        private Runnable wrap(Runnable task) {
            Objects.requireNonNull(task, "task");
            long submitted = clock.monotonicTime();
            return () -> {
                idleTimer.record(clock.monotonicTime() - submitted, NANOSECONDS);
                long started = clock.monotonicTime();
                try {
                    task.run();
                } finally {
                    executionTimer.record(clock.monotonicTime() - started, NANOSECONDS);
                }
            };
        }

        private <T> Callable<T> wrap(Callable<T> task) {
            Objects.requireNonNull(task, "task");
            long submitted = clock.monotonicTime();
            return () -> {
                idleTimer.record(clock.monotonicTime() - submitted, NANOSECONDS);
                long started = clock.monotonicTime();
                try {
                    return task.call();
                } finally {
                    executionTimer.record(clock.monotonicTime() - started, NANOSECONDS);
                }
            };
        }
    }
}
