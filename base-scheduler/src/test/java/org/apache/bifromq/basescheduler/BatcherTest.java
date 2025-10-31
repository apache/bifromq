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

package org.apache.bifromq.basescheduler;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.basescheduler.exception.BackPressureException;
import org.apache.bifromq.basescheduler.spi.IBatchCallWeighter;
import org.apache.bifromq.basescheduler.spi.ICapacityEstimator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BatcherTest {
    private RecordingBatchCallBuilder builder;
    private TestCapacityEstimator estimator;
    private IBatchCallWeighter<Integer> batchCallWeighter;
    private Batcher<Integer, Integer, Integer> batcher;

    @BeforeMethod
    public void setup() {
        builder = new RecordingBatchCallBuilder();
        estimator = new TestCapacityEstimator(16);
        batchCallWeighter = new TestBatchCallWeighter();
        batcher = new Batcher<>("test", 1, builder, Duration.ofSeconds(1).toNanos(), estimator, batchCallWeighter);
    }

    @AfterMethod
    public void tearDown() {
        batcher.close().join();
    }

    @Test
    public void submitAcceptsWhenUnderBurstLatency() {
        // success immediate
        builder.setSuccessMode();
        estimator.maxCapacity = 10;
        int n = 7;
        List<Integer> req = new ArrayList<>();
        List<Integer> resp = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            req.add(i);
            futures.add(batcher.submit(0, i).whenComplete((v, e) -> {
                if (e == null) {
                    resp.add(v);
                }
            }));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        assertEquals(resp, req);
        assertTrue(builder.newBatchCallCount.get() >= 1);
        assertEquals(builder.resetAbortFalseCount.get(), builder.executeCount.get());
    }

    @Test
    public void batchingRespectsMaxCapacity() {
        builder.setSuccessMode();
        estimator.maxCapacity = 3;
        int n = 7;
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            futures.add(batcher.submit(0, i));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        int sum = builder.batchSizes.stream().mapToInt(Integer::intValue).sum();
        assertEquals(sum, n);
        assertTrue(builder.batchSizes.stream().allMatch(sz -> sz <= 3));
        assertTrue(builder.executeCount.get() >= (int) Math.ceil(n / 3.0));
    }

    @Test
    public void timeoutCompletesTasksWithBackPressureExceptionAndCancelsFuture() {
        batcher.close().join();
        builder = new RecordingBatchCallBuilder();
        estimator = new TestCapacityEstimator(2);
        batcher = new Batcher<>("test", 1, builder, Duration.ofMillis(20).toNanos(), estimator, batchCallWeighter);
        builder.setHoldMode();
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        futures.add(batcher.submit(0, 1));
        futures.add(batcher.submit(0, 2));
        await().atMost(Duration.ofSeconds(3))
            .until(() -> futures.stream().allMatch(CompletableFuture::isDone));
        for (CompletableFuture<Integer> f : futures) {
            try {
                f.join();
                fail();
            } catch (Throwable ex) {
                assertTrue(ex.getCause() instanceof BackPressureException);
            }
        }
        assertTrue(builder.resetAbortTrueCount.get() >= 1);
    }

    @Test
    public void executeExceptionPropagatesToAllTasksAndAbortReset() {
        builder.setFailureMode(new IllegalStateException("failure"));
        estimator.maxCapacity = 3;
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        futures.add(batcher.submit(0, 1));
        futures.add(batcher.submit(0, 2));
        futures.add(batcher.submit(0, 3));
        CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
        try {
            all.join();
        } catch (Throwable e) {
            // ignore
        }
        for (CompletableFuture<Integer> f : futures) {
            try {
                f.join();
            } catch (Throwable ex) {
                assertTrue(ex.getCause() instanceof IllegalStateException);
            }
        }
        assertTrue(builder.resetAbortTrueCount.get() >= 1);
    }

    @Test
    public void closeImmediateWhenIdle() {
        CompletableFuture<Void> shutdown = batcher.close();
        assertTrue(shutdown.isDone());
        CompletableFuture<Integer> f = batcher.submit(0, 1);
        try {
            f.join();
            throw new AssertionError("Expected RejectedExecutionException");
        } catch (Throwable ex) {
            assertTrue(ex.getCause() instanceof RejectedExecutionException);
        }
    }

    @Test
    public void batchCallObjectReusedOnSuccess() {
        builder.setSuccessMode();
        estimator.maxCapacity = 1;
        // two sequential singleton batches
        assertEquals(batcher.submit(0, 1).join(), (Integer) 1);
        assertEquals(batcher.submit(0, 2).join(), (Integer) 2);
        // only one object should be created and reused
        assertEquals(builder.newBatchCallCount.get(), 1);
        assertEquals(builder.resetAbortFalseCount.get(), 2);
    }

    @Test
    public void batchCallObjectReusedOnFailure() {
        builder.setFailureMode(new RuntimeException("x"));
        estimator.maxCapacity = 1;
        CompletableFuture<Integer> f1 = batcher.submit(0, 1);
        await().atMost(Duration.ofSeconds(3)).until(f1::isDone);
        try {
            f1.join();
        } catch (Throwable e) {
            // ignore
        }
        builder.setSuccessMode();
        assertEquals(batcher.submit(0, 2).join(), (Integer) 2);
        // reused same object
        assertEquals(builder.newBatchCallCount.get(), 1);
        assertTrue(builder.resetAbortTrueCount.get() >= 1);
        assertTrue(builder.resetAbortFalseCount.get() >= 1);
    }

    @Test
    public void closeWaitsForInFlightAndCleansResources() {
        builder.setHoldMode();
        estimator.maxCapacity = 2;
        CompletableFuture<Integer> f1 = batcher.submit(0, 1);
        CompletableFuture<Integer> f2 = batcher.submit(0, 2);
        await().atMost(Duration.ofSeconds(3)).until(() -> !builder.heldFutures.isEmpty());

        CompletableFuture<Void> shutdown = batcher.close();
        assertFalse(shutdown.isDone());

        // release held batch and complete successfully
        builder.releaseHeldSuccess();
        assertEquals(f1.join(), (Integer) 1);
        assertEquals(f2.join(), (Integer) 2);

        await().atMost(Duration.ofSeconds(3)).until(shutdown::isDone);
        assertTrue(builder.resetAbortFalseCount.get() >= 1);
        assertTrue(builder.destroyCount.get() >= 1);
        assertEquals(builder.closeCount.get(), 1);
    }

    @Test
    public void submitRejectedAfterCloseWhileBusy() {
        builder.setHoldMode();
        estimator.maxCapacity = 1;
        CompletableFuture<Integer> f = batcher.submit(0, 1);
        await().atMost(Duration.ofSeconds(3))
            .until(() -> !builder.heldFutures.isEmpty());

        CompletableFuture<Void> shutdown = batcher.close();
        assertFalse(shutdown.isDone());

        CompletableFuture<Integer> rejected = batcher.submit(0, 2);
        try {
            rejected.join();
            fail();
        } catch (Throwable ex) {
            assertTrue(ex.getCause() instanceof RejectedExecutionException);
        }

        // cleanup to finish shutdown
        builder.releaseHeldSuccess();
        await().atMost(Duration.ofSeconds(3)).until(shutdown::isDone);
        assertEquals(f.join(), (Integer) 1);
    }

    @Test
    public void executeThrowsSynchronouslyHandled() {
        RuntimeException boom = new RuntimeException("sync");
        builder.setSyncThrowMode(boom);
        estimator.maxCapacity = 3;
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        futures.add(batcher.submit(0, 1));
        futures.add(batcher.submit(0, 2));
        futures.add(batcher.submit(0, 3));
        // ensure all futures completed to avoid indefinite join
        await().atMost(Duration.ofSeconds(3))
            .until(() -> futures.stream().allMatch(CompletableFuture::isDone));
        for (CompletableFuture<Integer> f : futures) {
            try {
                f.join();
                fail();
            } catch (Throwable ex) {
                assertTrue(ex.getCause() instanceof RuntimeException);
                assertEquals(ex.getCause().getMessage(), "sync");
            }
        }
        assertTrue(builder.resetAbortTrueCount.get() >= 1);
        assertEquals(builder.newBatchCallCount.get(), 1);
    }

    @Test(enabled = false)
    public void weightBasedCapacityRespectedWithHeavierRequests() {
        builder.setSuccessMode();
        batcher.close().join();
        builder = new RecordingBatchCallBuilder();
        estimator = new TestCapacityEstimator(3);
        batchCallWeighter = new HeavierBatchCallWeighter();
        batcher = new Batcher<>("test", 1, builder, Duration.ofSeconds(1).toNanos(), estimator, batchCallWeighter);

        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        futures.add(batcher.submit(0, 1));
        futures.add(batcher.submit(0, 2));
        futures.add(batcher.submit(0, 3));
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();

        assertTrue(builder.batchSizes.stream().allMatch(sz -> sz <= 1));
    }

    private static class TestCapacityEstimator implements ICapacityEstimator<Integer> {
        final AtomicInteger recordCount = new AtomicInteger();
        volatile long maxCapacity;
        volatile long lastRecordedWeight;
        volatile long lastRecordedLatency;

        TestCapacityEstimator(long maxCapacity) {
            this.maxCapacity = maxCapacity;
        }

        @Override
        public void record(long weightedSize, long latencyNs) {
            lastRecordedWeight = weightedSize;
            lastRecordedLatency = latencyNs;
            recordCount.incrementAndGet();
        }

        @Override
        public boolean hasCapacity(long currentOccupation, Integer key) {
            return maxCapacity - currentOccupation > 0;
        }

        @Override
        public long maxCapacity(Integer key) {
            return maxCapacity;
        }

        @Override
        public void onBackPressure() {
        }
    }

    private static class TestBatchCallWeighter implements IBatchCallWeighter<Integer> {
        private int count = 0;

        @Override
        public void add(Integer req) {
            count++;
        }

        @Override
        public long weight() {
            return count;
        }

        @Override
        public void reset() {
            count = 0;
        }
    }

    private static class HeavierBatchCallWeighter implements IBatchCallWeighter<Integer> {
        private int weight;

        @Override
        public void add(Integer req) {
            weight += 2;
        }

        @Override
        public long weight() {
            return weight;
        }

        @Override
        public void reset() {
            weight = 0;
        }
    }

    private static class RecordingBatchCallBuilder implements IBatchCallBuilder<Integer, Integer, Integer> {
        final AtomicInteger newBatchCallCount = new AtomicInteger();
        final AtomicInteger executeCount = new AtomicInteger();
        final AtomicInteger resetAbortTrueCount = new AtomicInteger();
        final AtomicInteger resetAbortFalseCount = new AtomicInteger();
        final AtomicInteger destroyCount = new AtomicInteger();
        final AtomicInteger closeCount = new AtomicInteger();
        final List<Integer> batchSizes = new CopyOnWriteArrayList<>();
        final AtomicReference<Mode> mode = new AtomicReference<>(Mode.SUCCESS);
        final AtomicReference<RuntimeException> failure = new AtomicReference<>(new RuntimeException("x"));
        // held calls and futures
        final Queue<RecordingBatchCall> heldCalls = new ConcurrentLinkedQueue<>();
        final Queue<CompletableFuture<Void>> heldFutures = new ConcurrentLinkedQueue<>();
        volatile Duration delay = Duration.ofMillis(100);

        @Override
        public IBatchCall<Integer, Integer, Integer> newBatchCall() {
            newBatchCallCount.incrementAndGet();
            return new RecordingBatchCall(this);
        }

        void setSuccessMode() {
            mode.set(Mode.SUCCESS);
        }

        void setFailureMode(RuntimeException e) {
            failure.set(e);
            mode.set(Mode.FAILURE);
        }

        void setHoldMode() {
            mode.set(Mode.HOLD);
        }

        void setDelaySuccessMode(Duration d) {
            this.delay = d;
            mode.set(Mode.DELAY);
        }

        void setSyncThrowMode(RuntimeException e) {
            failure.set(e);
            mode.set(Mode.SYNC_THROW);
        }

        void releaseHeldSuccess() {
            RecordingBatchCall c;
            CompletableFuture<Void> f;
            while ((c = heldCalls.poll()) != null && (f = heldFutures.poll()) != null) {
                c.completeTasksSuccess();
                f.complete(null);
            }
        }

        @Override
        public void close() {
            // record builder close
            closeCount.incrementAndGet();
        }

        enum Mode { SUCCESS, FAILURE, HOLD, DELAY, SYNC_THROW }
    }

    private static class RecordingBatchCall implements IBatchCall<Integer, Integer, Integer> {
        private final RecordingBatchCallBuilder owner;
        private final Queue<ICallTask<Integer, Integer, Integer>> tasks = new ConcurrentLinkedQueue<>();

        RecordingBatchCall(RecordingBatchCallBuilder owner) {
            this.owner = owner;
        }

        @Override
        public void add(ICallTask<Integer, Integer, Integer> task) {
            tasks.add(task);
        }

        @Override
        public void reset(boolean abort) {
            if (abort) {
                owner.resetAbortTrueCount.incrementAndGet();
            } else {
                owner.resetAbortFalseCount.incrementAndGet();
            }
            tasks.clear();
        }

        @Override
        public CompletableFuture<Void> execute() {
            owner.executeCount.incrementAndGet();
            owner.batchSizes.add(tasks.size());
            RecordingBatchCallBuilder.Mode mode = owner.mode.get();
            return switch (mode) {
                case SUCCESS -> {
                    completeTasksSuccess();
                    yield CompletableFuture.completedFuture(null);
                }
                case FAILURE -> CompletableFuture.failedFuture(owner.failure.get());
                case HOLD -> {
                    CompletableFuture<Void> f = new CompletableFuture<>();
                    owner.heldCalls.add(this);
                    owner.heldFutures.add(f);
                    yield f;
                }
                case DELAY -> CompletableFuture.runAsync(this::completeTasksSuccess,
                    CompletableFuture.delayedExecutor(owner.delay.toMillis(), TimeUnit.MILLISECONDS));
                case SYNC_THROW -> {
                    // throw directly to simulate synchronous failure
                    throw owner.failure.get();
                }
            };
        }

        void completeTasksSuccess() {
            for (ICallTask<Integer, Integer, Integer> t : tasks) {
                t.resultPromise().complete(t.call());
            }
        }

        @Override
        public void destroy() {
            // record batch destroy
            owner.destroyCount.incrementAndGet();
        }
    }
}
