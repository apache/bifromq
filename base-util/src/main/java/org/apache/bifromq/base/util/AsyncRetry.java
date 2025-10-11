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

import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import org.apache.bifromq.base.util.exception.NeedRetryException;
import org.apache.bifromq.base.util.exception.RetryTimeoutException;

/**
 * An asynchronous retry utility with exponential backoff, supporting cooperative cancellation:
 * cancel() on the returned future cancels the in-flight task and stops further retries.
 */
public class AsyncRetry {

    /**
     * Executes an asynchronous task with exponential backoff retry. If the async task failed with NeedRetryException,
     * it will be retried.
     *
     * @param taskSupplier A supplier that returns a CompletableFuture representing the asynchronous task.
     * @param retryTimeoutNanos The maximum allowed retry timeout (in nanoseconds) before timing out.
     * @param <T> The type of the task result.
     *
     * @return A CompletableFuture that completes with the task result if successful, or exceptionally with a RetryTimeoutException
     */
    public static <T> CompletableFuture<T> exec(Supplier<CompletableFuture<T>> taskSupplier, long retryTimeoutNanos) {
        return exec(taskSupplier, (result, t) -> {
            if (t != null) {
                Throwable cause = t instanceof CompletionException ? t.getCause() : t;
                return (cause instanceof NeedRetryException)
                    || (cause != null && cause.getCause() instanceof NeedRetryException);
            }
            return false;
        }, retryTimeoutNanos / 5, retryTimeoutNanos);
    }

    /**
     * Executes an asynchronous task with exponential backoff retry.
     *
     * <p>The method repeatedly invokes the given {@code taskSupplier} until the result satisfies
     * the {@code successPredicate}. The delay between retries increases exponentially (starting from
     * {@code initialBackoffMillis}). If the total accumulated delay exceeds {@code maxDelayMillis}, the returned
     * CompletableFuture is completed exceptionally with a RetryTimeoutException.
     *
     * @param taskSupplier        A supplier that returns a CompletableFuture representing the asynchronous task.
     * @param retryPredicate    A predicate to test whether the task's result should be retried.
     * @param initialBackoffNanos The initial delay (in nanoseconds) for retrying. 0 means no retry.
     * @param maxDelayNanos       The maximum allowed accumulated delay (in nanoseconds) before timing out.
     * @param <T>                 The type of the task result.
     * @return A CompletableFuture that completes with the task result if successful, or exceptionally with a
     * TimeoutException if max delay is exceeded.
     */
    public static <T> CompletableFuture<T> exec(Supplier<CompletableFuture<T>> taskSupplier,
                                                BiPredicate<T, Throwable> retryPredicate,
                                                long initialBackoffNanos,
                                                long maxDelayNanos) {
        if (initialBackoffNanos < 0 || maxDelayNanos < 0 || initialBackoffNanos > maxDelayNanos) {
            throw new IllegalArgumentException("Invalid backoff/timeout settings");
        }

        AtomicReference<CompletableFuture<T>> current = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);
        CancellableRetryFuture<T> onDone = new CancellableRetryFuture<>(current, cancelled);

        // kick off
        execLoop(taskSupplier, retryPredicate, initialBackoffNanos, maxDelayNanos, 0, 0L, current, cancelled, onDone);

        return CascadeCancelCompletableFuture.fromRoot(onDone);
    }

    private static <T> void execLoop(Supplier<CompletableFuture<T>> taskSupplier,
                                     BiPredicate<T, Throwable> retryPredicate,
                                     long initialBackoffNanos,
                                     long maxDelayNanos,
                                     int retryCount,
                                     long delayNanosSoFar,
                                     AtomicReference<CompletableFuture<T>> current,
                                     AtomicBoolean cancelled,
                                     CompletableFuture<T> onDone) {
        // If already cancelled, stop immediately.
        if (cancelled.get()) {
            onDone.completeExceptionally(new CancellationException());
            return;
        }

        if (initialBackoffNanos > 0 && delayNanosSoFar >= maxDelayNanos) {
            onDone.completeExceptionally(new RetryTimeoutException("Max retry delay exceeded"));
            return;
        }

        // Execute one attempt
        CompletableFuture<T> attempt = executeTask(taskSupplier);
        current.set(attempt);

        attempt.whenComplete(unwrap((result, t) -> {
            // If caller cancelled during the task, respect it and stop.
            if (cancelled.get()) {
                // make sure attempt is cancelled
                attempt.cancel(true);
                return;
            }

            boolean shouldRetry = false;
            if (initialBackoffNanos > 0) {
                // decide retry only when backoff enabled
                try {
                    shouldRetry = retryPredicate.test(result, t);
                } catch (Throwable predicateError) {
                    // defensive: if predicate misbehaves, treat as terminal failure
                    onDone.completeExceptionally(predicateError);
                    return;
                }
            }

            if (!shouldRetry) {
                // terminal path
                if (t != null) {
                    onDone.completeExceptionally(t);
                } else {
                    onDone.complete(result);
                }
                return;
            }

            // compute next delay (exponential, capped by remaining budget)
            long delay = initialBackoffNanos * (1L << Math.min(retryCount, 30)); // guard overflow
            long remaining = maxDelayNanos - delayNanosSoFar;
            if (delay > remaining) {
                delay = remaining;
            }

            long nextDelaySoFar = delayNanosSoFar + delay;

            // schedule next attempt after delay; if cancelled in the meantime, the runnable will no-op.
            Executor delayExecutor = CompletableFuture.delayedExecutor(delay, TimeUnit.NANOSECONDS);
            CompletableFuture.runAsync(() -> execLoop(
                taskSupplier, retryPredicate,
                initialBackoffNanos, maxDelayNanos,
                retryCount + 1, nextDelaySoFar,
                current, cancelled, onDone
            ), delayExecutor);
        }));
    }

    private static <T> CompletableFuture<T> executeTask(Supplier<CompletableFuture<T>> taskSupplier) {
        try {
            return Objects.requireNonNull(taskSupplier.get(),
                "taskSupplier returned null CompletableFuture");
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * A CompletableFuture that can cancel the in-flight task of the retry loop.
     */
    private static final class CancellableRetryFuture<T> extends CompletableFuture<T> {
        private final AtomicReference<CompletableFuture<T>> currentTask;
        private final AtomicBoolean cancelled;

        CancellableRetryFuture(AtomicReference<CompletableFuture<T>> currentTask, AtomicBoolean cancelled) {
            this.currentTask = currentTask;
            this.cancelled = cancelled;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            // idempotent: set cancelled flag first
            cancelled.set(true);
            // best effort: cancel the current in-flight task if any
            CompletableFuture<T> inFlight = currentTask.get();
            if (inFlight != null) {
                inFlight.cancel(mayInterruptIfRunning);
            }
            // complete this future as cancelled
            return super.cancel(mayInterruptIfRunning);
        }
    }
}