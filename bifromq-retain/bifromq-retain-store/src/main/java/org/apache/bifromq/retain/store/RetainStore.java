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

package org.apache.bifromq.retain.store;

import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.base.util.AsyncRunner;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basehookloader.BaseHookLoader;
import org.apache.bifromq.basekv.balance.KVStoreBalanceController;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.server.IBaseKVStoreServer;
import org.apache.bifromq.baserpc.client.IConnectable;
import org.apache.bifromq.retain.store.gc.IRetainStoreGCProcessor;
import org.apache.bifromq.retain.store.gc.RetainStoreGCProcessor;
import org.apache.bifromq.retain.store.spi.IRetainStoreBalancerFactory;

@Slf4j
class RetainStore implements IRetainStore {
    protected final RetainStoreCoProcFactory coProcFactory;
    private final String clusterId;
    private final ExecutorService rpcExecutor;
    private final IBaseKVStoreServer storeServer;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.INIT);
    private final IBaseKVStoreClient storeClient;
    private final KVStoreBalanceController balanceController;
    private final AsyncRunner jobRunner;
    private final ScheduledExecutorService jobScheduler;
    private final boolean jobExecutorOwner;
    private final Duration gcInterval;
    private final List<IRetainStoreBalancerFactory> effectiveBalancerFactories = new LinkedList<>();
    private volatile CompletableFuture<Void> gcJob;
    private IRetainStoreGCProcessor gcProcessor;

    public RetainStore(RetainStoreBuilder builder) {
        this.clusterId = builder.clusterId;
        this.storeClient = builder.retainStoreClient;
        this.gcInterval = builder.gcInterval;
        coProcFactory = new RetainStoreCoProcFactory();
        Map<String, IRetainStoreBalancerFactory> loadedFactories =
            BaseHookLoader.load(IRetainStoreBalancerFactory.class);
        for (String factoryName : builder.balancerFactoryConfig.keySet()) {
            if (!loadedFactories.containsKey(factoryName)) {
                log.warn("RetainStoreBalancerFactory[{}] not found", factoryName);
                continue;
            }
            IRetainStoreBalancerFactory balancer = loadedFactories.get(factoryName);
            balancer.init(builder.balancerFactoryConfig.get(factoryName));
            log.info("RetainStoreBalancerFactory[{}] enabled", factoryName);
            effectiveBalancerFactories.add(balancer);
        }


        balanceController = new KVStoreBalanceController(
            builder.metaService,
            storeClient,
            effectiveBalancerFactories,
            builder.bootstrapDelay,
            builder.zombieProbeDelay,
            builder.balancerRetryDelay,
            builder.bgTaskExecutor);
        jobExecutorOwner = builder.bgTaskExecutor == null;
        if (jobExecutorOwner) {
            String threadName = String.format("retain-store[%s]-job-executor", builder.clusterId);
            jobScheduler = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ScheduledThreadPoolExecutor(1, EnvProvider.INSTANCE.newThreadFactory(threadName)), threadName);
        } else {
            jobScheduler = builder.bgTaskExecutor;
        }
        jobRunner = new AsyncRunner("job.runner", jobScheduler, "type", "retainstore");
        if (builder.workerThreads == 0) {
            rpcExecutor = MoreExecutors.newDirectExecutorService();
        } else {
            rpcExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ThreadPoolExecutor(builder.workerThreads,
                    builder.workerThreads, 0L,
                    TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                    EnvProvider.INSTANCE.newThreadFactory("retain-store-executor")), "retain-store-executor");
        }

        storeServer = IBaseKVStoreServer.builder()
            // attach to rpc server
            .rpcServerBuilder(builder.rpcServerBuilder)
            .metaService(builder.metaService)
            // build basekv store service
            .addService(builder.clusterId)
            .coProcFactory(coProcFactory)
            .storeOptions(builder.storeOptions)
            .agentHost(builder.agentHost)
            .queryExecutor(MoreExecutors.directExecutor())
            .rpcExecutor(rpcExecutor)
            .tickerThreads(builder.tickerThreads)
            .bgTaskExecutor(builder.bgTaskExecutor)
            .attributes(builder.attributes)
            .finish()
            .build();
        start();
    }

    public String id() {
        return storeServer.storeId(clusterId);
    }

    private void start() {
        if (status.compareAndSet(Status.INIT, Status.STARTING)) {
            log.info("Starting retain store");
            storeServer.start();
            balanceController.start(id());
            gcProcessor = new RetainStoreGCProcessor(storeClient, id());
            status.compareAndSet(Status.STARTING, Status.STARTED);
            storeClient
                .connState()
                // observe the first READY state
                .filter(connState -> connState == IConnectable.ConnState.READY)
                .takeUntil(connState -> connState == IConnectable.ConnState.READY)
                .doOnComplete(this::scheduleGC)
                .subscribe();
            log.debug("Retain store started");
        }
    }

    public void close() {
        if (status.compareAndSet(Status.STARTED, Status.STOPPING)) {
            log.info("Stopping RetainStore");
            if (gcJob != null && !gcJob.isDone()) {
                gcJob.cancel(true);
            }
            balanceController.stop();
            storeServer.stop();
            coProcFactory.close();
            effectiveBalancerFactories.forEach(IRetainStoreBalancerFactory::close);
            if (jobExecutorOwner) {
                log.debug("Shutting down job executor");
                MoreExecutors.shutdownAndAwaitTermination(jobScheduler, 5, TimeUnit.SECONDS);
            }
            MoreExecutors.shutdownAndAwaitTermination(rpcExecutor, 5, TimeUnit.SECONDS);
            log.debug("RetainStore shutdown");
            status.compareAndSet(Status.STOPPING, Status.STOPPED);
        }
    }

    private void scheduleGC() {
        jobScheduler.schedule(this::gc, gcInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void gc() {
        jobRunner.add(() -> {
            if (status.get() != Status.STARTED) {
                return;
            }
            long reqId = HLC.INST.getPhysical();
            log.debug("Retain Store GC: id={}", id());
            gcJob = gcProcessor.gc(reqId, null, null, HLC.INST.getPhysical())
                .thenAccept(v -> {
                    log.debug("Retain Store GC succeed: id={}", id());
                })
                .whenComplete((v, e) -> scheduleGC());
        });
    }

    private enum Status {
        INIT, STARTING, STARTED, STOPPING, STOPPED
    }
}
