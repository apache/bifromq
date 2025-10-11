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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A CompletableFuture that supports "cascade cancel" to the first not-yet-completed upstream cancellable stage.
 *
 * <p>Semantics on cancel(tail):
 * <ul>
 *     <li>If root is not completed, cancel root;</li>
 *     <li>Otherwise, scan forward the recorded upstream cancellable stages in creation order and
 *         cancel the first stage that is not yet completed (e.g. A, then B, then D...);</li>
 *     <li>If all recorded stages are completed, just cancel the current stage.</li>
 * </ul>
 *
 * <p>Only stages that return a {@link CompletableFuture} (via thenCompose/exceptionallyCompose and their async variants)
 * are considered "cancellable" and recorded for cascade cancellation. Pure mapping stages (thenApply, thenAccept, etc.)
 * are not recorded and thus won't be directly cancelled.
 */
public final class CascadeCancelCompletableFuture<T> extends CompletableFuture<T> {
    private final Shared shared;

    private CascadeCancelCompletableFuture(Shared shared) {
        this.shared = shared;
    }

    private CascadeCancelCompletableFuture(CompletableFuture<?> root) {
        this.shared = new Shared(root);
    }

    /**
     * Start a cascade-cancellable chain from a root future.
     */
    public static <T> CompletableFuture<T> fromRoot(CompletableFuture<T> root) {
        CascadeCancelCompletableFuture<T> head = new CascadeCancelCompletableFuture<>(root);
        root.whenComplete((v, ex) -> {
            if (ex == null) {
                head.complete(v);
            } else {
                head.completeExceptionally(ex);
            }
        });
        return head;
    }

    @Override
    public <U> CompletableFuture<U> newIncompleteFuture() {
        return new CascadeCancelCompletableFuture<>(shared);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        cancelUpstream(mayInterruptIfRunning);
        return super.cancel(mayInterruptIfRunning);
    }

    private void cancelUpstream(boolean mayInterruptIfRunning) {
        // Scan from root to find the first not-yet-completed cancellable
        for (Node n = shared.head; n != null; n = n.next) {
            CompletableFuture<?> f = n.future;
            if (!f.isDone()) {
                f.cancel(mayInterruptIfRunning);
                break;
            }
        }
    }

    private void addCancelable(CompletableFuture<?> cf) {
        shared.append(cf);
    }

    private <U> Function<T, CompletionStage<U>> wrapCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return (T t) -> {
            CompletionStage<U> stage = fn.apply(t);
            if (stage instanceof CompletableFuture) {
                addCancelable((CompletableFuture<?>) stage);
            }
            return stage;
        };
    }

    private Function<Throwable, CompletionStage<T>> wrapExCompose(
        Function<Throwable, ? extends CompletionStage<T>> fn) {
        return (Throwable ex) -> {
            CompletionStage<T> stage = fn.apply(ex);
            if (stage instanceof CompletableFuture) {
                addCancelable((CompletableFuture<?>) stage);
            }
            return stage;
        };
    }

    // thenCompose overrides
    @Override
    public <U> CompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return super.thenCompose(wrapCompose(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
        return super.thenComposeAsync(wrapCompose(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn,
                                                     Executor executor) {
        return super.thenComposeAsync(wrapCompose(fn), executor);
    }

    // exceptionallyCompose
    @Override
    public CompletableFuture<T> exceptionallyCompose(Function<Throwable, ? extends CompletionStage<T>> fn) {
        return super.exceptionallyCompose(wrapExCompose(fn));
    }

    @Override
    public CompletableFuture<T> exceptionallyComposeAsync(Function<Throwable, ? extends CompletionStage<T>> fn) {
        return super.exceptionallyComposeAsync(wrapExCompose(fn));
    }

    @Override
    public CompletableFuture<T> exceptionallyComposeAsync(Function<Throwable, ? extends CompletionStage<T>> fn,
                                                          Executor executor) {
        return super.exceptionallyComposeAsync(wrapExCompose(fn), executor);
    }

    private static final class Node {
        final CompletableFuture<?> future;
        volatile Node next;

        Node(CompletableFuture<?> future) {
            this.future = future;
        }
    }

    private static final class Shared {
        private static final VarHandle TAIL;

        static {
            try {
                TAIL = MethodHandles.lookup().findVarHandle(Shared.class, "tail", Node.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        final Node head;
        volatile Node tail;

        Shared(CompletableFuture<?> root) {
            Node n = new Node(root);
            this.head = n;
            this.tail = n;
        }

        void append(CompletableFuture<?> cf) {
            Node n = new Node(cf);
            Node prev = (Node) TAIL.getAndSet(this, n);
            prev.next = n;
        }
    }
}
