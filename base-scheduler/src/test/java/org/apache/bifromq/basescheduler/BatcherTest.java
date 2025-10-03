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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.basescheduler.exception.BackPressureException;
import org.apache.bifromq.basescheduler.spi.ICapacityEstimator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BatcherTest {
    private RecordingBatchCallBuilder builder;
    private TestCapacityEstimator estimator;
    private Batcher<Integer, Integer, Integer> batcher;

    @BeforeMethod
    public void setup() {
        builder = new RecordingBatchCallBuilder();
        estimator = new TestCapacityEstimator(1, 16);
        batcher = new Batcher<>("test", builder, Duration.ofSeconds(1).toNanos(), estimator);
    }

    @AfterMethod
    public void tearDown() {
        batcher.close().join();
    }

    @Test
    public void submitAcceptsWhenUnderBurstLatency() {
        // success immediate
        builder.setSuccessMode();
        estimator.maxBatchSize = 10;
        estimator.maxPipelineDepth = 1;
        int n = 7;
        List<Integer> req = new ArrayList<>();
        List<Integer> resp = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            req.add(i);
            futures.add(batcher.submit(0, i).whenComplete((v, e) -> {if (e == null) {resp.add(v);}}));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        assertEquals(resp, req);
        assertTrue(builder.newBatchCallCount.get() >= 1);
        assertEquals(builder.resetAbortFalseCount.get(), builder.executeCount.get());
    }

    @Test
    public void submitDropsWhenBurstLatencyZero() {
        // rebuild with 0 burst to trigger drop
        batcher.close().join();
        builder = new RecordingBatchCallBuilder();
        estimator = new TestCapacityEstimator(1, 16);
        batcher = new Batcher<>("test", builder, 0L, estimator);

        CompletableFuture<Integer> f = batcher.submit(0, 1);
        try {
            f.join();
            fail();
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof BackPressureException);
        }
    }

    @Test
    public void batching_respectsMaxBatchSize() {
        builder.setSuccessMode();
        estimator.maxBatchSize = 3;
        estimator.maxPipelineDepth = 1;
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
    public void pipelineDepthLimitsConcurrentExecute() {
        batcher.close().join();
        builder = new RecordingBatchCallBuilder();
        estimator = new TestCapacityEstimator(1, 2);
        batcher = new Batcher<>("test", builder, Duration.ofSeconds(5).toNanos(), estimator);
        builder.setDelaySuccessMode(Duration.ofMillis(200));
        int n = 4;
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            futures.add(batcher.submit(0, i));
        }
        await().atMost(java.time.Duration.ofSeconds(1)).until(() -> builder.executeCount.get() == 1);
        assertEquals(builder.executeCount.get(), 1);
        await().atMost(java.time.Duration.ofSeconds(2)).until(() -> builder.executeCount.get() == 2);
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        int sum = builder.batchSizes.stream().mapToInt(Integer::intValue).sum();
        assertEquals(sum, n);
        assertTrue(builder.batchSizes.stream().allMatch(sz -> sz <= 2));
    }

    @Test
    public void timeoutCompletesTasksWithBackPressureException_andCancelsFuture() {
        // small burst to trigger timeout
        batcher.close().join();
        builder = new RecordingBatchCallBuilder();
        estimator = new TestCapacityEstimator(1, 2);
        batcher = new Batcher<>("test", builder, Duration.ofMillis(20).toNanos(), estimator);
        builder.setHoldMode();
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        futures.add(batcher.submit(0, 1));
        futures.add(batcher.submit(0, 2));
        await().atMost(java.time.Duration.ofSeconds(3))
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
        estimator.maxBatchSize = 3;
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        futures.add(batcher.submit(0, 1));
        futures.add(batcher.submit(0, 2));
        futures.add(batcher.submit(0, 3));
        CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
        try {
            all.join();
        } catch (Throwable ignore) {
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
    public void closeCompletesWhenDrained() {
        batcher.close().join();
        builder = new RecordingBatchCallBuilder();
        estimator = new TestCapacityEstimator(1, 2);
        batcher = new Batcher<>("test", builder, Duration.ofSeconds(5).toNanos(), estimator);
        builder.setDelaySuccessMode(Duration.ofMillis(200));
        CompletableFuture<Integer> f1 = batcher.submit(0, 1);
        CompletableFuture<Integer> f2 = batcher.submit(0, 2);
        await().atMost(java.time.Duration.ofSeconds(1))
            .until(() -> builder.executeCount.get() == 1);
        CompletableFuture<Void> shutdown = batcher.close();
        assertFalse(shutdown.isDone());
        await().atMost(java.time.Duration.ofSeconds(3)).until(shutdown::isDone);
        // further submit should be rejected
        CompletableFuture<Integer> f3 = batcher.submit(0, 3);
        try {
            f3.join();
        } catch (Throwable ex) {
            assertTrue(ex.getCause() instanceof java.util.concurrent.RejectedExecutionException);
        }
        // original tasks should have completed
        assertEquals(f1.join(), (Integer) 1);
        assertEquals(f2.join(), (Integer) 2);
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
            assertTrue(ex.getCause() instanceof java.util.concurrent.RejectedExecutionException);
        }
    }

    @Test
    public void batchCallObjectReusedOnSuccess() {
        builder.setSuccessMode();
        estimator.maxBatchSize = 1;
        estimator.maxPipelineDepth = 1;
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
        estimator.maxBatchSize = 1;
        estimator.maxPipelineDepth = 1;
        // first fails
        CompletableFuture<Integer> f1 = batcher.submit(0, 1);
        try {
            f1.join();
        } catch (Throwable ignore) {
        }
        // switch to success and submit again
        builder.setSuccessMode();
        assertEquals(batcher.submit(0, 2).join(), (Integer) 2);
        // reused same object
        assertEquals(builder.newBatchCallCount.get(), 1);
        assertTrue(builder.resetAbortTrueCount.get() >= 1);
        assertTrue(builder.resetAbortFalseCount.get() >= 1);
    }

    private static class TestCapacityEstimator implements ICapacityEstimator {
        final AtomicInteger recordCount = new AtomicInteger();
        volatile int maxPipelineDepth;
        volatile int maxBatchSize;
        volatile int lastRecordedBatchSize;
        volatile long lastRecordedLatency;

        TestCapacityEstimator(int maxPipelineDepth, int maxBatchSize) {
            this.maxPipelineDepth = maxPipelineDepth;
            this.maxBatchSize = maxBatchSize;
        }

        @Override
        public void record(int batchSize, long latencyNs) {
            lastRecordedBatchSize = batchSize;
            lastRecordedLatency = latencyNs;
            recordCount.incrementAndGet();
        }

        @Override
        public int maxPipelineDepth() {
            return maxPipelineDepth;
        }

        @Override
        public int maxBatchSize() {
            return maxBatchSize;
        }
    }

    private static class RecordingBatchCallBuilder implements IBatchCallBuilder<Integer, Integer, Integer> {
        final AtomicInteger newBatchCallCount = new AtomicInteger();
        final AtomicInteger executeCount = new AtomicInteger();
        final AtomicInteger resetAbortTrueCount = new AtomicInteger();
        final AtomicInteger resetAbortFalseCount = new AtomicInteger();
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
            failure.set(e); mode.set(Mode.FAILURE);
        }

        void setHoldMode() {
            mode.set(Mode.HOLD);
        }

        void setDelaySuccessMode(Duration d) {
            this.delay = d; mode.set(Mode.DELAY);
        }

        enum Mode { SUCCESS, FAILURE, HOLD, DELAY }
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
            };
        }

        void completeTasksSuccess() {
            for (ICallTask<Integer, Integer, Integer> t : tasks) {
                t.resultPromise().complete(t.call());
            }
        }
    }
}
