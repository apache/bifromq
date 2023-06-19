/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.basekv.store.util;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class AsyncRunnerTest {
    private ExecutorService executor;

    @BeforeMethod
    public void setup() {
        executor = Executors.newWorkStealingPool();
    }

    @AfterMethod
    public void teardown() {
        MoreExecutors.shutdownAndAwaitTermination(executor, 5, TimeUnit.SECONDS);
    }

    @Test
    public void testAwait() {
        AsyncRunner queue = new AsyncRunner(executor);
        assertTrue(queue.awaitDone().toCompletableFuture().isDone());
    }

    @Test
    public void testAwaitTask() {
        AsyncRunner queue = new AsyncRunner(executor);
        AtomicInteger counter = new AtomicInteger();
        for (int i = 0; i < 10; i++) {
            queue.add(() -> new CompletableFuture<Void>()
                .orTimeout(10, TimeUnit.MILLISECONDS)
                .whenComplete((v, e) -> counter.incrementAndGet()));
        }
        queue.awaitDone().toCompletableFuture().join();
        assertEquals(10, counter.get());

        for (int i = 0; i < 10; i++) {
            queue.add(() -> new CompletableFuture<Void>()
                .orTimeout(10, TimeUnit.MILLISECONDS)
                .whenComplete((v, e) -> counter.incrementAndGet()));
        }
        queue.awaitDone().toCompletableFuture().join();
        assertEquals(20, counter.get());
    }

    @SneakyThrows
    @Test
    public void testCancelRunningAsyncTask() {
        AtomicBoolean canceled = new AtomicBoolean();
        AsyncRunner queue = new AsyncRunner(executor);
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);

        CompletableFuture f1 = queue.add(() -> {
            CompletableFuture f = new CompletableFuture<>();
            f.whenComplete((v, e) -> {
                if (f.isCancelled()) {
                    canceled.set(true);
                    latch2.countDown();
                }
            });
            latch1.countDown();
            return f;
        });
        latch1.await();
        // wait for submitted async task triggered
        f1.cancel(true);
        // wait for submitted async task canceled
        latch2.await();
        assertTrue(canceled.get());
    }

    @Test
    public void testCancelNotRunning() {
        AsyncRunner queue = new AsyncRunner(executor);
        CompletableFuture<Void> f1 = new CompletableFuture<>();
        queue.add(() -> f1);
        CompletableFuture<Void> f2 = queue.add(() -> fail());
        CompletableFuture<Void> f3 = queue.add(() -> {
        });
        f2.cancel(true);
        f1.complete(null);
        f3.join();
    }

    @Test
    public void testCancelAll() {
        AsyncRunner queue = new AsyncRunner(executor);
        CompletableFuture<Void> f1 = queue.add(() -> new CompletableFuture());
        CompletableFuture<Void> f2 = queue.add(() -> new CompletableFuture());
        CompletableFuture<Void> f3 = queue.add(() -> new CompletableFuture());
        queue.cancelAll();
        queue.awaitDone().toCompletableFuture().join();
        assertTrue(f1.isCancelled());
        assertTrue(f2.isCancelled());
        assertTrue(f3.isCancelled());
    }

    @Test
    public void testAddFirst() {
        AsyncRunner queue = new AsyncRunner(executor);
        List<Integer> result = new ArrayList<>();
        queue.add(() -> {
            queue.addFirst(() -> result.add(1));
        });
        queue.add(() -> result.add(2));
        queue.awaitDone().toCompletableFuture().join();
        assertEquals(1, result.get(0).intValue());
        assertEquals(2, result.get(1).intValue());
    }

    @Test
    public void testAddRunnable() {
        AsyncRunner queue = new AsyncRunner(executor);
        queue.add(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).join();
        queue.awaitDone().toCompletableFuture().join();
    }

    @Test
    public void testRunnableException() {
        AsyncRunner queue = new AsyncRunner(executor);
        RuntimeException exp = new RuntimeException();
        try {
            queue.add(() -> {
                throw exp;
            }).join();
            fail();
        } catch (Throwable e) {
            assertEquals(e.getCause(), exp);
        }
        queue.awaitDone().toCompletableFuture().join();
    }

    @Test
    public void testTaskSupplierException() {
        AsyncRunner queue = new AsyncRunner(executor);
        RuntimeException exp = new RuntimeException();
        try {
            queue.add(() -> {
                if (true) {
                    throw exp;
                }
                return new CompletableFuture<>();
            }).join();
            fail();
        } catch (Throwable e) {
            assertEquals(e.getCause(), exp);
        }
        queue.awaitDone().toCompletableFuture().join();
    }
}
