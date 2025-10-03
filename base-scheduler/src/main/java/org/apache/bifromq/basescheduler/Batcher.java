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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basescheduler.exception.BackPressureException;
import org.apache.bifromq.basescheduler.spi.ICapacityEstimator;

@Slf4j
final class Batcher<CallT, CallResultT, BatcherKeyT> {
    private final IBatchCallBuilder<CallT, CallResultT, BatcherKeyT> batchCallBuilder;
    private final Queue<IBatchCall<CallT, CallResultT, BatcherKeyT>> batchPool;
    private final Queue<ICallTask<CallT, CallResultT, BatcherKeyT>> callTaskBuffers = new ConcurrentLinkedQueue<>();
    private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);
    private final AtomicBoolean triggering = new AtomicBoolean();
    private final AtomicInteger pipelineDepth = new AtomicInteger();
    private final ICapacityEstimator capacityEstimator;
    private final long maxBurstLatency;
    private final EMALong emaQueueingTime;
    private final Gauge maxPipelineDepthGauge;
    private final Gauge pipelineDepthGauge;
    private final Counter dropCounter;
    private final Timer batchCallTimer;
    private final Timer batchExecTimer;
    private final Timer batchBuildTimer;
    private final DistributionSummary batchSizeSummary;
    private final DistributionSummary queueingTimeSummary;
    // Future to signal shutdown completion
    private final CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();
    // hold a ref to all running batch calls for debugging
    private final Set<CompletableFuture<Void>> runningBatchCalls = ConcurrentHashMap.newKeySet();

    Batcher(String name,
            IBatchCallBuilder<CallT, CallResultT, BatcherKeyT> batchCallBuilder,
            long maxBurstLatency,
            ICapacityEstimator capacityEstimator) {
        this.batchCallBuilder = batchCallBuilder;
        this.capacityEstimator = capacityEstimator;
        this.maxBurstLatency = maxBurstLatency;
        this.batchPool = new ConcurrentLinkedDeque<>();
        this.emaQueueingTime = new EMALong(System::nanoTime, 0.1, 0.9, maxBurstLatency);
        Tags tags = Tags.of("name", name, "key", Integer.toUnsignedString(System.identityHashCode(this)));
        maxPipelineDepthGauge = Gauge.builder("batcher.pipeline.max", capacityEstimator::maxPipelineDepth)
            .tags(tags)
            .register(Metrics.globalRegistry);
        pipelineDepthGauge = Gauge.builder("batcher.pipeline.depth", pipelineDepth::get)
            .tags(tags)
            .register(Metrics.globalRegistry);
        dropCounter = Counter.builder("batcher.call.drop.count").tags(tags).register(Metrics.globalRegistry);
        batchCallTimer = Timer.builder("batcher.call.time").tags(tags).register(Metrics.globalRegistry);
        batchExecTimer = Timer.builder("batcher.exec.time").tags(tags).register(Metrics.globalRegistry);
        batchBuildTimer = Timer.builder("batcher.build.time").tags(tags).register(Metrics.globalRegistry);
        batchSizeSummary =
            DistributionSummary.builder("batcher.batch.size").tags(tags).register(Metrics.globalRegistry);
        queueingTimeSummary =
            DistributionSummary.builder("batcher.queueing.time").tags(tags).register(Metrics.globalRegistry);
    }

    public CompletableFuture<CallResultT> submit(BatcherKeyT batcherKey, CallT request) {
        if (state.get() != State.RUNNING) {
            return CompletableFuture.failedFuture(
                new RejectedExecutionException("Batcher has been shut down"));
        }
        if (Math.max(emaQueueingTime.get(), headCallWaitingNanos()) < maxBurstLatency) {
            ICallTask<CallT, CallResultT, BatcherKeyT> callTask = new CallTask<>(batcherKey, request);
            boolean offered = callTaskBuffers.offer(callTask);
            assert offered;
            trigger();
            return callTask.resultPromise();
        } else {
            dropCounter.increment();
            return CompletableFuture.failedFuture(new BackPressureException("Batch call busy"));
        }
    }

    private long headCallWaitingNanos() {
        ICallTask<CallT, CallResultT, BatcherKeyT> head = callTaskBuffers.peek();
        if (head != null) {
            return System.nanoTime() - head.ts();
        }
        return 0;
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
            state.set(State.TERMINATED);
            shutdownFuture.complete(null);
        }
    }

    private void cleanupMetrics() {
        Metrics.globalRegistry.remove(maxPipelineDepthGauge);
        Metrics.globalRegistry.remove(pipelineDepthGauge);
        Metrics.globalRegistry.remove(dropCounter);
        Metrics.globalRegistry.remove(batchCallTimer);
        Metrics.globalRegistry.remove(batchExecTimer);
        Metrics.globalRegistry.remove(batchBuildTimer);
        Metrics.globalRegistry.remove(batchSizeSummary);
        Metrics.globalRegistry.remove(queueingTimeSummary);
        IBatchCall<CallT, CallResultT, BatcherKeyT> batchCall;
        while ((batchCall = batchPool.poll()) != null) {
            batchCall.destroy();
        }
        batchCallBuilder.close();
    }

    private void trigger() {
        if (triggering.compareAndSet(false, true)) {
            try {
                if (!callTaskBuffers.isEmpty() && pipelineDepth.get() < capacityEstimator.maxPipelineDepth()) {
                    batchAndEmit();
                }
            } finally {
                triggering.set(false);
                if (!callTaskBuffers.isEmpty() && pipelineDepth.get() < capacityEstimator.maxPipelineDepth()) {
                    trigger();
                }
            }
        }
    }

    private void batchAndEmit() {
        pipelineDepth.incrementAndGet();
        long buildStart = System.nanoTime();
        IBatchCall<CallT, CallResultT, BatcherKeyT> batchCall = borrowBatchCall();

        int batchSize = 0;
        int maxBatchSize = capacityEstimator.maxBatchSize();
        LinkedList<ICallTask<CallT, CallResultT, BatcherKeyT>> batchedTasks = new LinkedList<>();
        ICallTask<CallT, CallResultT, BatcherKeyT> callTask;
        while (batchSize < maxBatchSize && (callTask = callTaskBuffers.poll()) != null) {
            long queueingTime = System.nanoTime() - callTask.ts();
            queueingTimeSummary.record(queueingTime);
            emaQueueingTime.update(queueingTime);
            batchCall.add(callTask);
            batchedTasks.add(callTask);
            batchSize++;
        }
        batchSizeSummary.record(batchSize);
        long execBegin = System.nanoTime();
        batchBuildTimer.record(execBegin - buildStart, TimeUnit.NANOSECONDS);
        try {
            int finalBatchSize = batchSize;
            CompletableFuture<Void> future = batchCall.execute();
            runningBatchCalls.add(future);
            future
                .orTimeout(maxBurstLatency, TimeUnit.NANOSECONDS) // Ensure we don't block indefinitely
                .whenComplete((v, e) -> {
                    runningBatchCalls.remove(future);
                    long execEnd = System.nanoTime();
                    if (e != null) {
                        if (e instanceof TimeoutException) {
                            batchedTasks.forEach(t -> t.resultPromise()
                                .completeExceptionally(new BackPressureException("Batch Call timeout", e)));
                            if (!future.isDone()) {
                                future.cancel(true);
                            }
                        } else {
                            batchedTasks.forEach(t -> t.resultPromise().completeExceptionally(e));
                        }
                        returnBatchCall(batchCall, true);
                    } else {
                        long batchCallLatency = execEnd - execBegin;
                        capacityEstimator.record(finalBatchSize, batchCallLatency);
                        batchExecTimer.record(batchCallLatency, TimeUnit.NANOSECONDS);
                        batchedTasks.forEach(t -> {
                            long callLatency = execEnd - t.ts();
                            batchCallTimer.record(callLatency, TimeUnit.NANOSECONDS);
                        });
                        returnBatchCall(batchCall, false);
                    }
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
