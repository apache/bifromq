/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.basescheduler;

import org.apache.bifromq.basescheduler.exception.AbortException;
import org.apache.bifromq.basescheduler.exception.BatcherUnavailableException;
import org.apache.bifromq.basescheduler.spi.ICallScheduler;
import org.apache.bifromq.basescheduler.spi.ICapacityEstimator;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import java.lang.reflect.ParameterizedType;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;

/**
 * The abstract class for batch call scheduler.
 *
 * @param <CallT> the type of the request to be fulfilled in batch
 * @param <CallResultT> the type of the response expected
 * @param <BatcherKeyT> the type of the key to identify the batch
 */
@Slf4j
public abstract class BatchCallScheduler<CallT, CallResultT, BatcherKeyT>
    implements IBatchCallScheduler<CallT, CallResultT> {
    private static final int BATCHER_EXPIRY_SECONDS = 600;
    private final ICallScheduler<CallT> callScheduler;
    private final ICapacityEstimator capacityEstimator;
    private final LoadingCache<BatcherKeyT, Batcher<CallT, CallResultT, BatcherKeyT>> batchers;
    private final LongAdder runningCalls = new LongAdder();
    private final Gauge runningCallsGauge;
    private final Gauge batcherNumGauge;
    private final Counter callSchedCounter;
    private final Counter callSubmitCounter;

    protected BatchCallScheduler(IBatchCallBuilderFactory<CallT, CallResultT, BatcherKeyT> batchCallFactory,
                                 long maxBurstLatency) {
        String name = getName();
        this.callScheduler = CallSchedulerFactory.INSTANCE.create(name);
        this.capacityEstimator = CapacityEstimatorFactory.INSTANCE.create(name);
        batchers = Caffeine.newBuilder()
            .scheduler(Scheduler.systemScheduler())
            .expireAfterAccess(Duration.ofSeconds(BATCHER_EXPIRY_SECONDS))
            .evictionListener((RemovalListener<BatcherKeyT, Batcher<CallT, CallResultT, BatcherKeyT>>)
                (key, value, removalCause) -> {
                    if (value != null) {
                        value.close();
                    }
                })
            .build(k ->
                new Batcher<>(name, batchCallFactory.newBuilder(name, k), maxBurstLatency, capacityEstimator));
        runningCallsGauge = Gauge.builder("batcher.call.running.gauge", runningCalls::sum)
            .tags("name", name)
            .register(Metrics.globalRegistry);
        batcherNumGauge = Gauge.builder("batcher.num", batchers::estimatedSize)
            .tags("name", name)
            .register(Metrics.globalRegistry);
        callSchedCounter = Counter.builder("batcher.call.sched.count")
            .tags("name", name)
            .register(Metrics.globalRegistry);
        callSubmitCounter = Counter.builder("batcher.call.submit.count")
            .tags("name", name)
            .register(Metrics.globalRegistry);
    }

    protected abstract Optional<BatcherKeyT> find(CallT call);

    @Override
    public CompletableFuture<CallResultT> schedule(CallT request) {
        callSchedCounter.increment();
        runningCalls.increment();
        return callScheduler.submit(request)
            .thenCompose(req -> {
                try {
                    Optional<BatcherKeyT> batcherKey = find(req);
                    if (batcherKey.isPresent()) {
                        Batcher<CallT, CallResultT, BatcherKeyT> batcher = batchers.get(batcherKey.get());
                        callSubmitCounter.increment();
                        return batcher.submit(batcherKey.get(), req);
                    } else {
                        return CompletableFuture.failedFuture(new BatcherUnavailableException("Batcher not found"));
                    }
                } catch (Throwable e) {
                    log.error("Error", e);
                    return CompletableFuture.failedFuture(new AbortException("Failed to submit request", e));
                }
            })
            .whenComplete((v, e) -> runningCalls.decrement());
    }

    @Override
    public void close() {
        CompletableFuture.allOf(
            batchers.asMap().values().stream().map(Batcher::close).toArray(CompletableFuture[]::new)).join();
        batchers.invalidateAll();
        callScheduler.close();
        capacityEstimator.close();
        Metrics.globalRegistry.remove(runningCallsGauge);
        Metrics.globalRegistry.remove(batcherNumGauge);
        Metrics.globalRegistry.remove(callSubmitCounter);
        Metrics.globalRegistry.remove(callSchedCounter);
    }

    private String getName() {
        String typeName = ((ParameterizedType) getClass().getGenericSuperclass())
            .getActualTypeArguments()[0]
            .getTypeName();
        if (typeName.lastIndexOf(".") > 0) {
            typeName = typeName.substring(typeName.lastIndexOf(".") + 1);
        }
        return "scheduler[" + typeName + "]";
    }
}
