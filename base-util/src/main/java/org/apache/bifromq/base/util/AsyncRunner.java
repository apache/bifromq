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

package org.apache.bifromq.base.util;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.lang.ref.Cleaner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;

/**
 * AsyncRunner is a utility class that allows for the execution of tasks asynchronously in submission order.
 */
public final class AsyncRunner {
    private static final Cleaner CLEANER = Cleaner.create();
    private final ConcurrentLinkedDeque<Task<?>> taskQueue;
    private final Executor executor;
    private final AtomicReference<State> state = new AtomicReference<>(State.EMPTY_STOP);
    private final Timer queueingTimer;
    private final Timer execTimer;
    private volatile CompletableFuture<Void> whenDone = CompletableFuture.completedFuture(null);

    public AsyncRunner(Executor executor) {
        this("async.runner", executor);
    }

    public AsyncRunner(String name, Executor executor, String... tags) {
        this.taskQueue = new ConcurrentLinkedDeque<>();
        this.executor = executor;
        this.queueingTimer = Timer.builder(name)
            .tags("type", "queueing")
            .tags(tags)
            .register(Metrics.globalRegistry);
        this.execTimer = Timer.builder(name)
            .tags("type", "exec")
            .tags(tags)
            .register(Metrics.globalRegistry);
        CLEANER.register(this, new ClosableState(queueingTimer));
        CLEANER.register(this, new ClosableState(execTimer));
    }

    public CompletableFuture<Void> add(Runnable runnable) {
        return add(runnable, false);
    }

    public <T> CompletableFuture<T> add(Supplier<CompletableFuture<T>> taskSupplier) {
        return add(taskSupplier, false);
    }

    private CompletableFuture<Void> add(Runnable runnable, boolean first) {
        return add(() -> {
            CompletableFuture<Void> onDone = new CompletableFuture<>();
            if (onDone.isCancelled()) {
                return CompletableFuture.completedFuture(null);
            }
            try {
                runnable.run();
            } catch (Throwable e) {
                onDone.completeExceptionally(e);
            } finally {
                onDone.complete(null);
            }
            return onDone;
        }, first);
    }

    private <T> CompletableFuture<T> add(Supplier<CompletableFuture<T>> taskSupplier, boolean first) {
        CompletableFuture<T> f = new CompletableFuture<>();
        if (first) {
            taskQueue.addFirst(new Task<>(taskSupplier, f));
        } else {
            taskQueue.addLast(new Task<>(taskSupplier, f));
        }
        while (true) {
            if (state.compareAndSet(State.EMPTY_STOP, State.NONEMPTY_STOP)) {
                whenDone = new CompletableFuture<>();
                executor.execute(this::runTask);
                break;
            }
            if (state.get() == State.NONEMPTY_STOP || state.get() == State.NONEMPTY_RUNNING) {
                break;
            }
            if (state.compareAndSet(State.EMPTY_RUNNING, State.NONEMPTY_RUNNING)) {
                break;
            }
        }
        return f;
    }

    public CompletableFuture<Void> addFirst(Runnable runnable) {
        return add(runnable, true);
    }

    public <T> CompletableFuture<T> addFirst(Supplier<CompletableFuture<T>> taskSupplier) {
        return add(taskSupplier, true);
    }

    public void cancelAll() {
        taskQueue.descendingIterator().forEachRemaining(t -> t.future.cancel(true));
    }

    private <T> void runTask() {
        while (true) {
            if (state.compareAndSet(State.NONEMPTY_STOP, State.NONEMPTY_RUNNING)
                || state.get() == State.NONEMPTY_RUNNING) {
                @SuppressWarnings("unchecked")
                Task<T> task = (Task<T>) taskQueue.peek();
                if (task != null) {
                    Supplier<CompletableFuture<T>> taskSupplier = task.supplier;
                    executor.execute(() -> {
                        if (task.future.isDone()) {
                            taskQueue.removeFirstOccurrence(task);
                            runTask();
                            return;
                        }
                        try {
                            long now = System.nanoTime();
                            queueingTimer.record(now - task.submitAtNanos, TimeUnit.NANOSECONDS);
                            CompletableFuture<T> taskFuture = taskSupplier.get();
                            task.future.whenCompleteAsync((v, e) -> {
                                if (task.future.isCancelled()) {
                                    taskFuture.cancel(true);
                                }
                            }, executor);
                            taskFuture.whenCompleteAsync((v, e) -> {
                                if (!task.future.isDone()) {
                                    if (e != null) {
                                        task.future.completeExceptionally(e);
                                    } else {
                                        execTimer.record(System.nanoTime() - now, TimeUnit.NANOSECONDS);
                                        task.future.complete(v);
                                    }
                                }
                                taskQueue.removeFirstOccurrence(task);
                                runTask();
                            }, executor);
                        } catch (Throwable e) {
                            task.future.completeExceptionally(e);
                            taskQueue.removeFirstOccurrence(task);
                            runTask();
                        }
                    });
                    break;
                } else if (state.compareAndSet(State.NONEMPTY_RUNNING, State.EMPTY_RUNNING)) {
                    if (!taskQueue.isEmpty()) {
                        state.set(State.NONEMPTY_RUNNING);
                    }
                    executor.execute(this::runTask);
                    break;
                }
            }
            if (state.compareAndSet(State.EMPTY_RUNNING, State.EMPTY_STOP)) {
                whenDone.complete(null);
                whenDone = CompletableFuture.completedFuture(null);
                break;
            }
        }
    }

    /**
     * Await current submitted tasks done.
     *
     * @return A CompletableFuture that will be completed when all submitted tasks are done.
     */
    public CompletionStage<Void> awaitDone() {
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        whenDone.whenComplete((v, e) -> onDone.complete(null));
        return onDone;
    }

    private enum State {
        EMPTY_STOP,
        NONEMPTY_STOP,
        NONEMPTY_RUNNING,
        EMPTY_RUNNING
    }

    private record ClosableState(Meter meter) implements Runnable {
        @Override
        public void run() {
            Metrics.globalRegistry.remove(meter);
        }
    }

    @AllArgsConstructor
    private static class Task<T> {
        final long submitAtNanos = System.nanoTime();
        final Supplier<CompletableFuture<T>> supplier;
        final CompletableFuture<T> future;
    }
}
