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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class CascadeCancelCompletableFutureTest {
    private ExecutorService single;

    private static void assertCancelledSemantics(CompletableFuture<?> f) {
        await().atMost(1, SECONDS).until(() -> f.isCancelled() || f.isCompletedExceptionally());
        if (!f.isCancelled()) {
            assertTrue(f.isCompletedExceptionally());
            try {
                f.join();
                fail();
            } catch (CompletionException e) {
                assertTrue(e.getCause() instanceof CancellationException);
            }
        }
    }

    private static void assertDoneOrCancelled(CompletableFuture<?> f) {
        await().atMost(1, SECONDS).until(f::isDone);
        if (!f.isCancelled() && !f.isCompletedExceptionally()) {
            // done normally is acceptable for this scenario
            return;
        }
        // otherwise, must satisfy cancelled semantics
        assertCancelledSemantics(f);
    }

    @BeforeClass
    public void setup() {
        single = Executors.newSingleThreadExecutor();
    }

    @AfterClass
    public void teardown() {
        if (single != null) {
            single.shutdownNow();
        }
    }

    @Test
    public void fromRootPropagatesCompletion() throws Exception {
        CompletableFuture<String> root = new CompletableFuture<>();
        CompletableFuture<String> head = CascadeCancelCompletableFuture.fromRoot(root);

        root.complete("ok");
        assertEquals(head.join(), "ok");

        CompletableFuture<String> root2 = new CompletableFuture<>();
        CompletableFuture<String> head2 = CascadeCancelCompletableFuture.fromRoot(root2);
        RuntimeException ex = new RuntimeException("Mocked");
        root2.completeExceptionally(ex);
        assertTrue(head2.isCompletedExceptionally());
        try {
            head2.get();
            fail();
        } catch (ExecutionException e) {
            assertSame(e.getCause(), ex);
        }
    }

    @Test
    public void typePreservedAcrossStages() {
        CompletableFuture<String> root = new CompletableFuture<>();
        CompletableFuture<String> head = CascadeCancelCompletableFuture.fromRoot(root);

        CompletableFuture<Integer> s1 = head.thenCompose(v -> CompletableFuture.completedFuture(1));
        CompletableFuture<Integer> s2 = head.thenComposeAsync(v -> CompletableFuture.completedFuture(2));
        CompletableFuture<String> s3 = head.thenApply(v -> v);
        CompletableFuture<String> s4 = head.exceptionallyCompose(ex -> CompletableFuture.completedFuture("x"));
        CompletableFuture<String> s5 = head.exceptionallyComposeAsync(ex -> CompletableFuture.completedFuture("y"));

        assertTrue(s1 instanceof CascadeCancelCompletableFuture);
        assertTrue(s2 instanceof CascadeCancelCompletableFuture);
        assertTrue(s3 instanceof CascadeCancelCompletableFuture);
        assertTrue(s4 instanceof CascadeCancelCompletableFuture);
        assertTrue(s5 instanceof CascadeCancelCompletableFuture);
    }

    @Test
    public void cancelHitsRootWhenRootPending() {
        RecordingFuture<String> root = new RecordingFuture<>();
        CompletableFuture<String> head = CascadeCancelCompletableFuture.fromRoot(root);

        RecordingFuture<String> a = new RecordingFuture<>();
        CompletableFuture<String> aStage = head.thenCompose(v -> a);
        RecordingFuture<String> b = new RecordingFuture<>();
        CompletableFuture<String> bStage = aStage.thenCompose(v -> b);
        CompletableFuture<String> cStage = bStage.thenApply(v -> v);
        RecordingFuture<String> d = new RecordingFuture<>();
        CompletableFuture<String> dStage = cStage.exceptionallyCompose(ex -> d);
        CompletableFuture<String> e = dStage.thenApply(v -> v);

        e.cancel(true);
        // root should be cancelled; others untouched
        assertEquals(root.cancelCount.get(), 1);
        assertEquals(root.cancelFlags.size(), 1);
        assertTrue(root.cancelFlags.get(0));
        assertEquals(a.cancelCount.get(), 0);
        assertEquals(b.cancelCount.get(), 0);
        assertEquals(d.cancelCount.get(), 0);
        assertCancelledSemantics(e);
    }

    @Test
    public void cancelHitsAWhenAExecuting() {
        RecordingFuture<String> root = new RecordingFuture<>();
        CompletableFuture<String> head = CascadeCancelCompletableFuture.fromRoot(root);

        RecordingFuture<String> a = new RecordingFuture<>();
        CompletableFuture<String> aStage = head.thenCompose(v -> a);
        RecordingFuture<String> b = new RecordingFuture<>();
        CompletableFuture<String> bStage = aStage.thenCompose(v -> b);
        CompletableFuture<String> cStage = bStage.thenApply(v -> v);
        RecordingFuture<String> d = new RecordingFuture<>();
        CompletableFuture<String> dStage = cStage.exceptionallyCompose(ex -> d);
        CompletableFuture<String> e = dStage.thenApply(v -> v);

        root.complete("rootDone");
        // ensure A is created and pending
        await().atMost(1, SECONDS).until(() -> !a.isDone());

        e.cancel(false);
        assertEquals(a.cancelCount.get(), 1);
        assertEquals(a.cancelFlags.size(), 1);
        assertFalse(a.cancelFlags.get(0));
        assertEquals(root.cancelCount.get(), 0); // root already completed, should not be cancelled now
        assertEquals(b.cancelCount.get(), 0);
        assertEquals(d.cancelCount.get(), 0);
        assertCancelledSemantics(e);
    }

    @Test
    public void cancelHitsB_whenBExecuting() {
        RecordingFuture<String> root = new RecordingFuture<>();
        CompletableFuture<String> head = CascadeCancelCompletableFuture.fromRoot(root);

        RecordingFuture<String> a = new RecordingFuture<>();
        CompletableFuture<String> aStage = head.thenCompose(v -> a);
        RecordingFuture<String> b = new RecordingFuture<>();
        CompletableFuture<String> bStage = aStage.thenCompose(v -> b);
        CompletableFuture<String> cStage = bStage.thenApply(v -> v);
        RecordingFuture<String> d = new RecordingFuture<>();
        CompletableFuture<String> dStage = cStage.exceptionallyCompose(ex -> d);
        CompletableFuture<String> e = dStage.thenApply(v -> v);

        root.complete("rootDone");
        await().atMost(1, SECONDS).until(() -> !a.isDone());
        a.complete("aDone"); // advance to B
        await().atMost(1, SECONDS).until(() -> !b.isDone());

        e.cancel(true);
        assertEquals(b.cancelCount.get(), 1);
        assertEquals(b.cancelFlags.size(), 1);
        assertTrue(b.cancelFlags.get(0));
        assertEquals(root.cancelCount.get(), 0);
        assertEquals(a.cancelCount.get(), 0);
        assertEquals(d.cancelCount.get(), 0);
        assertCancelledSemantics(e);
    }

    @Test
    public void cancelHitsDWhenDExecutingExceptionPath() {
        RecordingFuture<String> root = new RecordingFuture<>();
        CompletableFuture<String> head = CascadeCancelCompletableFuture.fromRoot(root);

        RecordingFuture<String> a = new RecordingFuture<>();
        CompletableFuture<String> aStage = head.thenCompose(v -> a);
        RecordingFuture<String> b = new RecordingFuture<>();
        CompletableFuture<String> bStage = aStage.thenCompose(v -> b);
        // C throws, to enter exceptionallyCompose(D)
        CompletableFuture<String> cStage = bStage.thenApply(v -> {
            throw new RuntimeException("C failed");
        });
        RecordingFuture<String> d = new RecordingFuture<>();
        CompletableFuture<String> dStage = cStage.exceptionallyCompose(ex -> d);
        CompletableFuture<String> e = dStage.thenApply(v -> v);

        root.complete("rootDone");
        a.complete("aDone");
        b.complete("bDone");
        // ensure D exists and pending
        await().atMost(1, SECONDS).until(() -> !d.isDone());

        e.cancel(false);
        assertEquals(d.cancelCount.get(), 1);
        assertEquals(d.cancelFlags.size(), 1);
        assertFalse(d.cancelFlags.get(0));
        assertEquals(root.cancelCount.get(), 0);
        assertEquals(a.cancelCount.get(), 0);
        assertEquals(b.cancelCount.get(), 0);
        assertCancelledSemantics(e);
    }

    @Test
    public void cancelOnlyTailWhenAllUpstreamDone() {
        RecordingFuture<String> root = new RecordingFuture<>();
        CompletableFuture<String> head = CascadeCancelCompletableFuture.fromRoot(root);

        RecordingFuture<String> a = new RecordingFuture<>();
        CompletableFuture<String> aStage = head.thenCompose(v -> a);
        RecordingFuture<String> b = new RecordingFuture<>();
        CompletableFuture<String> bStage = aStage.thenCompose(v -> b);
        CompletableFuture<String> cStage = bStage.thenApply(v -> v);
        RecordingFuture<String> d = new RecordingFuture<>();
        CompletableFuture<String> dStage = cStage.exceptionallyCompose(ex -> d);
        CompletableFuture<String> e = dStage.thenApply(v -> v);

        root.complete("rootDone");
        a.complete("aDone");
        b.complete("bDone");

        // cancel tail
        e.cancel(true);
        assertEquals(root.cancelCount.get(), 0);
        assertEquals(a.cancelCount.get(), 0);
        assertEquals(b.cancelCount.get(), 0);
        assertEquals(d.cancelCount.get(), 0);
        assertDoneOrCancelled(e);
    }

    @Test
    public void asyncComposeCancelHitsFirstPending() {
        RecordingFuture<String> root = new RecordingFuture<>();
        CompletableFuture<String> head = CascadeCancelCompletableFuture.fromRoot(root);

        RecordingFuture<String> a = new RecordingFuture<>();
        CompletableFuture<String> aStage = head.thenComposeAsync(v -> a, single);
        RecordingFuture<String> b = new RecordingFuture<>();
        CompletableFuture<String> bStage = aStage.thenComposeAsync(v -> b, single);
        CompletableFuture<String> cStage = bStage.thenApplyAsync(v -> v, single);
        RecordingFuture<String> d = new RecordingFuture<>();
        CompletableFuture<String> dStage = cStage.exceptionallyComposeAsync(ex -> d, single);
        CompletableFuture<String> e = dStage.thenApplyAsync(v -> v, single);

        root.complete("rootDone");
        await().atMost(1, SECONDS).until(() -> !a.isDone());

        e.cancel(false);
        assertEquals(a.cancelCount.get(), 1);
        assertFalse(a.cancelFlags.get(0));
        assertCancelledSemantics(e);
    }

    @Test
    public void immediateCompletedComposeSkipsToNextPending() {
        RecordingFuture<String> root = new RecordingFuture<>();
        CompletableFuture<String> head = CascadeCancelCompletableFuture.fromRoot(root);

        CompletableFuture<String> aCompleted = CompletableFuture.completedFuture("aDone");
        CompletableFuture<String> aStage = head.thenCompose(v -> aCompleted);
        RecordingFuture<String> b = new RecordingFuture<>();
        CompletableFuture<String> bStage = aStage.thenCompose(v -> b);
        CompletableFuture<String> cStage = bStage.thenApply(v -> v);
        CompletableFuture<String> dStage = cStage.exceptionallyCompose(ex -> CompletableFuture.completedFuture("dv"));
        CompletableFuture<String> e = dStage.thenApply(v -> v);

        root.complete("rootDone");
        await().atMost(1, SECONDS).until(() -> !b.isDone());

        e.cancel(true);
        assertEquals(b.cancelCount.get(), 1);
        assertTrue(b.cancelFlags.get(0));
        // exceptionallyCompose recovers to a normal value,
        assertDoneOrCancelled(e);
    }

    @Test
    public void idempotentCancelNoThrow() {
        RecordingFuture<String> root = new RecordingFuture<>();
        CompletableFuture<String> head = CascadeCancelCompletableFuture.fromRoot(root);

        RecordingFuture<String> a = new RecordingFuture<>();
        CompletableFuture<String> aStage = head.thenCompose(v -> a);
        RecordingFuture<String> b = new RecordingFuture<>();
        CompletableFuture<String> bStage = aStage.thenCompose(v -> b);
        CompletableFuture<String> cStage = bStage.thenApply(v -> v);
        RecordingFuture<String> d = new RecordingFuture<>();
        CompletableFuture<String> dStage = cStage.exceptionallyCompose(ex -> d);
        CompletableFuture<String> e = dStage.thenApply(v -> v);

        root.complete("rootDone");
        await().atMost(1, SECONDS).until(() -> !a.isDone());

        e.cancel(false);
        e.cancel(false);
        // A should be cancelled at least once; no exception thrown
        assertTrue(a.cancelCount.get() >= 1);
        assertCancelledSemantics(e);
    }

    private static class RecordingFuture<T> extends CompletableFuture<T> {
        final List<Boolean> cancelFlags = new CopyOnWriteArrayList<>();
        final AtomicInteger cancelCount = new AtomicInteger();

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            cancelFlags.add(mayInterruptIfRunning);
            cancelCount.incrementAndGet();
            return super.cancel(mayInterruptIfRunning);
        }
    }
}
