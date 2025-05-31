/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;

import org.apache.bifromq.base.util.exception.NeedRetryException;
import org.apache.bifromq.base.util.exception.RetryTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

/**
 * An asynchronous retry utility with exponential backoff.
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
                return t instanceof NeedRetryException || t.getCause() instanceof NeedRetryException;
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
        assert initialBackoffNanos <= maxDelayNanos;
        CompletableFuture<T> onDone = new CompletableFuture<>();
        exec(taskSupplier, retryPredicate, initialBackoffNanos, maxDelayNanos, 0, 0, onDone);
        return onDone;
    }

    private static <T> void exec(Supplier<CompletableFuture<T>> taskSupplier,
                                 BiPredicate<T, Throwable> retryPredicate,
                                 long initialBackoffNanos,
                                 long maxDelayNanos,
                                 int retryCount,
                                 long delayNanosSoFar,
                                 CompletableFuture<T> onDone) {

        if (initialBackoffNanos > 0 && delayNanosSoFar >= maxDelayNanos) {
            onDone.completeExceptionally(new RetryTimeoutException("Max retry delay exceeded"));
            return;
        }

        // Execute the asynchronous task.
        executeTask(taskSupplier).whenComplete(unwrap((result, t) -> {
            // If the result satisfies the retry predicate, return it.
            if (initialBackoffNanos == 0 || !retryPredicate.test(result, t)) {
                if (t != null) {
                    onDone.completeExceptionally(t);
                } else {
                    onDone.complete(result);
                }
            } else {
                long delay = initialBackoffNanos * (1L << retryCount);
                if (delayNanosSoFar + delay > maxDelayNanos) {
                    delay = maxDelayNanos - delayNanosSoFar;
                }
                long delayMillisSoFarNew = delayNanosSoFar + delay;
                // Otherwise, schedule a retry after the calculated delay.
                Executor delayExecutor = CompletableFuture.delayedExecutor(delay, TimeUnit.NANOSECONDS);
                CompletableFuture.runAsync(() -> exec(
                    taskSupplier,
                    retryPredicate,
                    initialBackoffNanos,
                    maxDelayNanos,
                    retryCount + 1,
                    delayMillisSoFarNew, onDone), delayExecutor);
            }
        }));
    }

    private static <T> CompletableFuture<T> executeTask(Supplier<CompletableFuture<T>> taskSupplier) {
        try {
            return taskSupplier.get();
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}