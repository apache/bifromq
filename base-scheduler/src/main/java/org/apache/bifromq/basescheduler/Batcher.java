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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basescheduler.exception.BackPressureException;
import org.apache.bifromq.basescheduler.spi.IBatchCallWeighter;
import org.apache.bifromq.basescheduler.spi.ICapacityEstimator;

@Slf4j
final class Batcher<CallT, CallResultT, BatcherKeyT> {
    private final BatcherKeyT key;
    private final IBatchCallBuilder<CallT, CallResultT, BatcherKeyT> batchCallBuilder;
    private final Queue<IBatchCall<CallT, CallResultT, BatcherKeyT>> batchPool;
    private final Queue<ICallTask<CallT, CallResultT, BatcherKeyT>> callTaskBuffers = new ConcurrentLinkedQueue<>();
    private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);
    private final AtomicBoolean triggering = new AtomicBoolean();
    private final AtomicInteger pipelineDepth = new AtomicInteger();
    private final AtomicInteger queuedCallCount = new AtomicInteger(0);
    private final AtomicInteger inFlightCallCount = new AtomicInteger(0);
    private final ICapacityEstimator<BatcherKeyT> capacityEstimator;
    private final IBatchCallWeighter<CallT> batchCallWeighter;
    private final long maxBurstLatency;
    private final EMALong emaQueueingTime;
    private final Gauge pipelineDepthGauge;
    private final Counter dropCounter;
    private final Timer batchCallTimer;
    private final Timer batchExecTimer;
    private final Timer batchBuildTimer;
    private final DistributionSummary batchCountSummary;
    private final DistributionSummary batchWeightSizeSummary;
    private final DistributionSummary queueingTimeSummary;
    private final Gauge maxCapacityGauge;
    private final Gauge queueingCountGauge;
    private final Gauge inflightCountGauge;
    private final Gauge inflightWeightGauge;
    // Future to signal shutdown completion
    private final CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();
    private final AtomicLong inFlightWeight = new AtomicLong(0L);

    Batcher(String name,
            BatcherKeyT key,
            IBatchCallBuilder<CallT, CallResultT, BatcherKeyT> batchCallBuilder,
            long maxBurstLatency,
            ICapacityEstimator<BatcherKeyT> capacityEstimator,
            IBatchCallWeighter<CallT> batchCallWeighter) {
        this.key = key;
        this.batchCallBuilder = batchCallBuilder;
        this.capacityEstimator = capacityEstimator;
        this.batchCallWeighter = batchCallWeighter;
        this.maxBurstLatency = maxBurstLatency;
        this.batchPool = new ConcurrentLinkedDeque<>();
        this.emaQueueingTime = new EMALong(System::nanoTime, 0.1, 0.9, maxBurstLatency);
        Tags tags = Tags.of("name", name, "key", Integer.toUnsignedString(System.identityHashCode(this)));
        pipelineDepthGauge = Gauge.builder("batcher.pipeline.depth", pipelineDepth::get)
            .tags(tags)
            .register(Metrics.globalRegistry);
        dropCounter = Counter.builder("batcher.call.drop.count")
            .tags(tags)
            .register(Metrics.globalRegistry);
        batchCallTimer = Timer.builder("batcher.call.time")
            .tags(tags)
            .register(Metrics.globalRegistry);
        batchExecTimer = Timer.builder("batcher.exec.time")
            .tags(tags)
            .register(Metrics.globalRegistry);
        batchBuildTimer = Timer.builder("batcher.build.time")
            .tags(tags)
            .register(Metrics.globalRegistry);
        batchCountSummary = DistributionSummary.builder("batcher.batch.count")
            .tags(tags)
            .register(Metrics.globalRegistry);
        batchWeightSizeSummary = DistributionSummary.builder("batcher.batch.size")
            .tags(tags)
            .register(Metrics.globalRegistry);
        queueingTimeSummary = DistributionSummary.builder("batcher.queueing.time")
            .tags(tags)
            .register(Metrics.globalRegistry);
        maxCapacityGauge = Gauge.builder("batcher.capacity.max", () -> capacityEstimator.maxCapacity(key))
            .tags(tags)
            .register(Metrics.globalRegistry);
        queueingCountGauge = Gauge.builder("batcher.queueing.count", queuedCallCount::get)
            .tags(tags)
            .register(Metrics.globalRegistry);
        inflightCountGauge = Gauge.builder("batcher.inflight.count", inFlightCallCount::get)
            .tags(tags)
            .register(Metrics.globalRegistry);
        inflightWeightGauge = Gauge.builder("batcher.inflight.size", inFlightWeight::get)
            .tags(tags)
            .register(Metrics.globalRegistry);
    }

    public CompletableFuture<CallResultT> submit(BatcherKeyT batcherKey, CallT request) {
        if (state.get() != State.RUNNING) {
            return CompletableFuture.failedFuture(
                new RejectedExecutionException("Batcher has been shut down"));
        }
        if (emaQueueingTime.get() > maxBurstLatency) {
            dropCounter.increment();
            if (pipelineDepth.get() == 0 && !callTaskBuffers.isEmpty()) {
                trigger();
            }
            return CompletableFuture.failedFuture(new BackPressureException("Batch call busy"));
        }

        ICallTask<CallT, CallResultT, BatcherKeyT> callTask = new CallTask<>(batcherKey, request);
        boolean offered = callTaskBuffers.offer(callTask);
        assert offered;
        queuedCallCount.incrementAndGet();
        trigger();
        return callTask.resultPromise();
    }

    public CompletableFuture<Void> close() {
        if (state.compareAndSet(State.RUNNING, State.SHUTTING_DOWN)) {
            checkShutdownCompletion();
        }
        return shutdownFuture;
    }

    private void checkShutdownCompletion() {
        // If no tasks pending and no pipeline in-flight, complete
        if (callTaskBuffers.isEmpty() && pipelineDepth.get() == 0) {
            cleanupMetrics();
            batchCallWeighter.reset();
            state.set(State.TERMINATED);
            shutdownFuture.complete(null);
        }
    }

    private void cleanupMetrics() {
        Metrics.globalRegistry.remove(pipelineDepthGauge);
        Metrics.globalRegistry.remove(dropCounter);
        Metrics.globalRegistry.remove(batchCallTimer);
        Metrics.globalRegistry.remove(batchExecTimer);
        Metrics.globalRegistry.remove(batchBuildTimer);
        Metrics.globalRegistry.remove(batchCountSummary);
        Metrics.globalRegistry.remove(batchWeightSizeSummary);
        Metrics.globalRegistry.remove(queueingTimeSummary);
        Metrics.globalRegistry.remove(maxCapacityGauge);
        Metrics.globalRegistry.remove(queueingCountGauge);
        Metrics.globalRegistry.remove(inflightCountGauge);
        Metrics.globalRegistry.remove(inflightWeightGauge);
        IBatchCall<CallT, CallResultT, BatcherKeyT> batchCall;
        while ((batchCall = batchPool.poll()) != null) {
            batchCall.destroy();
        }
        batchCallBuilder.close();
    }

    private void trigger() {
        if (triggering.compareAndSet(false, true)) {
            try {
                if (!callTaskBuffers.isEmpty() && capacityEstimator.hasCapacity(inFlightWeight.get(), key)) {
                    batchAndEmit();
                }
            } finally {
                triggering.set(false);
                if (!callTaskBuffers.isEmpty() && capacityEstimator.hasCapacity(inFlightWeight.get(), key)) {
                    this.trigger();
                }
            }
        }
    }

    private void batchAndEmit() {
        pipelineDepth.incrementAndGet();
        long buildStart = System.nanoTime();
        IBatchCall<CallT, CallResultT, BatcherKeyT> batchCall = borrowBatchCall();
        int batchedCallNums = 0;
        LinkedList<ICallTask<CallT, CallResultT, BatcherKeyT>> batchedTasks = new LinkedList<>();
        ICallTask<CallT, CallResultT, BatcherKeyT> callTask;
        batchCallWeighter.reset();
        long avail = capacityEstimator.maxCapacity(key);
        while (batchCallWeighter.weight() < avail && (callTask = callTaskBuffers.poll()) != null) {
            batchCallWeighter.add(callTask.call());
            long queueingTime = System.nanoTime() - callTask.ts();
            emaQueueingTime.update(queueingTime);
            queueingTimeSummary.record(queueingTime);
            batchCall.add(callTask);
            batchedTasks.add(callTask);
            batchedCallNums++;
        }
        final long batchWeight = batchCallWeighter.weight();
        queuedCallCount.addAndGet(-batchedCallNums);
        inFlightCallCount.addAndGet(batchedCallNums);
        batchCountSummary.record(batchedCallNums);
        batchWeightSizeSummary.record(batchWeight);
        long execBegin = System.nanoTime();
        batchBuildTimer.record(execBegin - buildStart, TimeUnit.NANOSECONDS);
        try {
            int finalBatchSize = batchedCallNums;
            inFlightWeight.addAndGet(batchWeight);
            CompletableFuture<Void> future = batchCall.execute();
            future
                .orTimeout(maxBurstLatency, TimeUnit.NANOSECONDS)
                .whenComplete((v, e) -> {
                    long execEnd = System.nanoTime();
                    if (e != null) {
                        if (e instanceof BackPressureException || e instanceof TimeoutException) {
                            capacityEstimator.onBackPressure();
                            batchedTasks.forEach(t -> t.resultPromise()
                                .completeExceptionally(new BackPressureException("Downstream Busy", e)));
                            returnBatchCall(batchCall, true);
                        } else {
                            batchedTasks.forEach(t -> t.resultPromise().completeExceptionally(e));
                            returnBatchCall(batchCall, true);
                        }
                    } else {
                        long execLatency = execEnd - execBegin;
                        batchExecTimer.record(execLatency, TimeUnit.NANOSECONDS);
                        capacityEstimator.record(batchWeight, execLatency);
                        batchedTasks.forEach(t -> {
                            long callLatency = execEnd - t.ts();
                            batchCallTimer.record(callLatency, TimeUnit.NANOSECONDS);
                        });
                        returnBatchCall(batchCall, false);
                    }
                    inFlightCallCount.addAndGet(-finalBatchSize);
                    inFlightWeight.addAndGet(-batchWeight);
                    pipelineDepth.getAndDecrement();
                    // After each completion, check for shutdown
                    if (state.get() == State.SHUTTING_DOWN) {
                        checkShutdownCompletion();
                    }
                    if (!callTaskBuffers.isEmpty()) {
                        trigger();
                    }
                });
        } catch (Throwable e) {
            log.error("Batch call failed unexpectedly", e);
            batchedTasks.forEach(t -> t.resultPromise().completeExceptionally(e));
            returnBatchCall(batchCall, true);
            // decrease in-flight count by completed size on failure path
            inFlightCallCount.addAndGet(-batchedCallNums);
            inFlightWeight.addAndGet(-batchWeight);
            pipelineDepth.getAndDecrement();
            if (state.get() == State.SHUTTING_DOWN) {
                checkShutdownCompletion();
            }
            if (!callTaskBuffers.isEmpty()) {
                trigger();
            }
        }
    }

    private IBatchCall<CallT, CallResultT, BatcherKeyT> borrowBatchCall() {
        IBatchCall<CallT, CallResultT, BatcherKeyT> batchCall = batchPool.poll();
        if (batchCall == null) {
            batchCall = batchCallBuilder.newBatchCall();
        }
        return batchCall;
    }

    private void returnBatchCall(IBatchCall<CallT, CallResultT, BatcherKeyT> batchCall, boolean abort) {
        batchCall.reset(abort);
        batchPool.offer(batchCall);
    }

    private enum State { RUNNING, SHUTTING_DOWN, TERMINATED }
}
