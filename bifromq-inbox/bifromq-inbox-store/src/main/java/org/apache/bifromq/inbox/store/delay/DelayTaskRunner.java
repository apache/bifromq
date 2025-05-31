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

package org.apache.bifromq.inbox.store.delay;

import org.apache.bifromq.baseenv.EnvProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * DelayTaskRunner is a utility class to schedule tasks to be executed after a specified delay.
 *
 * @param <KeyT> the type of the key used to identify the task.
 */
public class DelayTaskRunner<KeyT extends Comparable<KeyT>> implements IDelayTaskRunner<KeyT> {
    // sorted by deadlineTS and then inboxId
    private final NavigableMap<SortKey<KeyT>, IDelayedTask<KeyT>> sortedDeadlines;
    // key: inboxId, value: deadlineTS
    private final Map<KeyT, Long> deadLines = new ConcurrentHashMap<>();
    private final Supplier<Long> currentMillisSupplier;
    private final ScheduledExecutorService executor;
    private final RateLimiter rateLimiter;
    private long nextTriggerTS;
    private ScheduledFuture<?> triggerTask;
    private volatile boolean isShutdown;

    /**
     * Constructor for DelayTaskRunner.
     *
     * @param comparator            the comparator to be used for sorting the keys
     * @param currentMillisSupplier a supplier to get the current time in milliseconds
     * @param rateLimit             the rate limit for triggering the scheduled tasks
     */
    public DelayTaskRunner(Comparator<KeyT> comparator,
                           Supplier<Long> currentMillisSupplier,
                           int rateLimit) {
        this.currentMillisSupplier = currentMillisSupplier;
        this.sortedDeadlines = new TreeMap<>(
            Comparator.comparingLong((SortKey<KeyT> sk) -> sk.deadlineTS).thenComparing(sk -> sk.key, comparator));
        this.rateLimiter = RateLimiter.create(rateLimit);
        executor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ScheduledThreadPoolExecutor(1, EnvProvider.INSTANCE.newThreadFactory("delay-task-runner")),
            "delay-task-runner");
    }

    @VisibleForTesting
    boolean hasTask(KeyT key) {
        return deadLines.containsKey(key);
    }

    @Override
    public <TaskT extends IDelayedTask<KeyT>> void schedule(KeyT key, TaskT delayedTask) {
        if (isShutdown) {
            return;
        }
        executor.submit(() -> {
            if (isShutdown) {
                return;
            }
            Long prevDeadlineTS = deadLines.remove(key);
            long now = currentMillisSupplier.get();
            if (prevDeadlineTS != null) {
                sortedDeadlines.remove(new SortKey<>(key, prevDeadlineTS));
            }
            Duration delayInterval = delayedTask.getDelay();
            long deadlineTS = deadline(now, delayInterval);
            deadLines.put(key, deadlineTS);
            sortedDeadlines.put(new SortKey<>(key, deadlineTS), delayedTask);

            Map.Entry<SortKey<KeyT>, IDelayedTask<KeyT>> firstEntry = sortedDeadlines.firstEntry();
            long earliestDeadline = firstEntry.getKey().deadlineTS;
            if (nextTriggerTS == 0 || earliestDeadline < nextTriggerTS) {
                // postpone trigger task
                if (triggerTask != null) {
                    triggerTask.cancel(true);
                }
                nextTriggerTS = earliestDeadline;
                triggerTask = executor.schedule(this::trigger, earliestDeadline - now, TimeUnit.MILLISECONDS);
            }
        });
    }

    @Override
    public <TaskT extends IDelayedTask<KeyT>> void scheduleIfAbsent(KeyT key, TaskT delayedTask) {
        if (isShutdown) {
            return;
        }
        executor.submit(() -> {
            if (isShutdown) {
                return;
            }
            if (deadLines.containsKey(key)) {
                return;
            }
            Long prevDeadlineTS = deadLines.remove(key);
            long now = currentMillisSupplier.get();
            if (prevDeadlineTS != null) {
                sortedDeadlines.remove(new SortKey<>(key, prevDeadlineTS));
            }
            Duration delayInterval = delayedTask.getDelay();
            long deadlineTS = deadline(now, delayInterval);
            deadLines.put(key, deadlineTS);
            sortedDeadlines.put(new SortKey<>(key, deadlineTS), delayedTask);

            Map.Entry<SortKey<KeyT>, IDelayedTask<KeyT>> firstEntry = sortedDeadlines.firstEntry();
            long earliestDeadline = firstEntry.getKey().deadlineTS;
            if (nextTriggerTS == 0 || earliestDeadline < nextTriggerTS) {
                // postpone trigger task
                if (triggerTask != null) {
                    triggerTask.cancel(true);
                }
                nextTriggerTS = earliestDeadline;
                triggerTask = executor.schedule(this::trigger, earliestDeadline - now, TimeUnit.MILLISECONDS);
            }
        });
    }

    @Override
    public void cancelAll(Set<KeyT> keys) {
        executor.submit(() -> {
            executor.submit(() -> {
                boolean removed = false;
                for (KeyT key : keys) {
                    Long deadline = deadLines.remove(key);
                    if (deadline != null) {
                        sortedDeadlines.remove(new SortKey<>(key, deadline));
                        removed = true;
                    }
                }
                if (removed) {
                    Map.Entry<SortKey<KeyT>, IDelayedTask<KeyT>> firstEntry = sortedDeadlines.firstEntry();
                    if (firstEntry != null) {
                        long earliestDeadline = firstEntry.getKey().deadlineTS;
                        if (nextTriggerTS == 0 || earliestDeadline < nextTriggerTS) {
                            if (triggerTask != null) {
                                triggerTask.cancel(true);
                            }
                            long now = currentMillisSupplier.get();
                            nextTriggerTS = earliestDeadline;
                            triggerTask = executor.schedule(this::trigger, Math.max(earliestDeadline - now, 0),
                                TimeUnit.MILLISECONDS);
                        }
                    } else {
                        if (triggerTask != null) {
                            triggerTask.cancel(true);
                        }
                        nextTriggerTS = 0;
                        triggerTask = null;
                    }
                }
            });
        });
    }

    @Override
    public void shutdown() {
        executor.submit(() -> {
            isShutdown = true;
            executor.shutdown();
        });
    }

    private void trigger() {
        long now = currentMillisSupplier.get();
        Map.Entry<SortKey<KeyT>, IDelayedTask<KeyT>> entry;
        while ((entry = sortedDeadlines.firstEntry()) != null && rateLimiter.tryAcquire()) {
            SortKey<KeyT> sortKey = entry.getKey();
            IDelayedTask<KeyT> delayedTask = entry.getValue();
            if (entry.getKey().deadlineTS <= now) {
                deadLines.remove(entry.getKey().key);
                sortedDeadlines.remove(entry.getKey());
                delayedTask.run(sortKey.key, this);
            } else {
                nextTriggerTS = entry.getKey().deadlineTS;
                triggerTask = executor.schedule(this::trigger, nextTriggerTS - now, TimeUnit.MILLISECONDS);
                return;
            }
        }
        if (entry != null) {
            nextTriggerTS = entry.getKey().deadlineTS;
            long delay = Math.max(nextTriggerTS - now, 1000);
            triggerTask = executor.schedule(this::trigger, delay, TimeUnit.MILLISECONDS);
        } else {
            // no deadlines
            nextTriggerTS = 0;
            triggerTask = null;
        }
    }

    private long deadline(long now, Duration delayInterval) {
        return delayInterval.plusMillis(now).toMillis();
    }

    private record SortKey<K extends Comparable<K>>(K key, long deadlineTS) {
    }
}
