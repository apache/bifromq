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

import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Utility class for CompletableFuture operations.
 */
public class CompletableFutureUtil {
    /**
     * Wraps a BiConsumer to handle CompletionException and unwraps it to get the real cause.
     *
     * @param action the BiConsumer to wrap
     * @param <T>    the type of the result
     * @return a BiConsumer that unwraps CompletionException
     */
    public static <T> BiConsumer<T, Throwable> unwrap(BiConsumer<T, Throwable> action) {
        return (res, ex) -> {
            Throwable real = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
            action.accept(res, real);
        };
    }

    /**
     * Wraps a BiFunction to handle CompletionException and unwraps it to get the real cause.
     *
     * @param fn the BiFunction to wrap
     * @param <T> the type of the first argument
     * @param <U> the type of the second argument
     *
     * @return a BiFunction that unwraps CompletionException
     */
    public static <T, U> BiFunction<T, Throwable, U> unwrap(BiFunction<T, Throwable, U> fn) {
        return (res, ex) -> {
            Throwable real = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
            return fn.apply(res, real);
        };
    }

    /**
     * Wraps a Consumer to handle CompletionException and unwraps it to get the real cause.
     *
     * @param action the Consumer to wrap
     * @return a Consumer that unwraps CompletionException
     */
    public static <T> Function<Throwable, T> unwrap(Function<Throwable, T> action) {
        return ex -> {
            Throwable real = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
            return action.apply(real);
        };
    }
}
