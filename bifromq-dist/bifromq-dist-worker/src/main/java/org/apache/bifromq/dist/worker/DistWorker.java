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

package org.apache.bifromq.dist.worker;

import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.basehookloader.BaseHookLoader;
import org.apache.bifromq.basekv.balance.KVStoreBalanceController;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.server.IBaseKVStoreServer;
import org.apache.bifromq.baserpc.client.IConnectable;
import org.apache.bifromq.deliverer.BatchDeliveryCallBuilderFactory;
import org.apache.bifromq.deliverer.IMessageDeliverer;
import org.apache.bifromq.deliverer.MessageDeliverer;
import org.apache.bifromq.dist.worker.spi.IDistWorkerBalancerFactory;

@Slf4j
class DistWorker implements IDistWorker {
    private final String clusterId;
    private final ExecutorService rpcExecutor;
    private final IBaseKVStoreClient distWorkerClient;
    private final IBaseKVStoreServer distWorkerServer;
    private final IMessageDeliverer messageDeliverer;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.INIT);
    private final KVStoreBalanceController storeBalanceController;
    private final List<IDistWorkerBalancerFactory> effectiveBalancerFactories = new LinkedList<>();
    private final DistWorkerCoProcFactory coProcFactory;
    private final boolean jobExecutorOwner;
    private final ScheduledExecutorService jobScheduler;
    private final DistWorkerCleaner cleaner;

    public DistWorker(DistWorkerBuilder builder) {
        this.clusterId = builder.clusterId;
        this.messageDeliverer = new MessageDeliverer(
            new BatchDeliveryCallBuilderFactory(builder.distClient, builder.subBrokerManager));
        coProcFactory = new DistWorkerCoProcFactory(
            builder.distClient,
            builder.eventCollector,
            builder.resourceThrottler,
            builder.subBrokerManager,
            this.messageDeliverer,
            builder.settingProvider,
            builder.loadEstimateWindow,
            builder.fanoutParallelism,
            builder.inlineFanoutThreshold);
        Map<String, IDistWorkerBalancerFactory> loadedFactories = BaseHookLoader.load(IDistWorkerBalancerFactory.class);
        for (String factoryName : builder.balancerFactoryConfig.keySet()) {
            if (!loadedFactories.containsKey(factoryName)) {
                log.warn("DistWorkerBalancerFactory[{}] not found", factoryName);
                continue;
            }
            IDistWorkerBalancerFactory balancer = loadedFactories.get(factoryName);
            balancer.init(builder.balancerFactoryConfig.get(factoryName));
            log.info("DistWorkerBalancerFactory[{}] enabled", factoryName);
            effectiveBalancerFactories.add(balancer);
        }

        storeBalanceController = new KVStoreBalanceController(
            builder.metaService,
            builder.distWorkerClient,
            effectiveBalancerFactories,
            builder.bootstrapDelay,
            builder.zombieProbeDelay,
            builder.balancerRetryDelay,
            builder.bgTaskExecutor);
        jobExecutorOwner = builder.bgTaskExecutor == null;
        if (jobExecutorOwner) {
            String threadName = String.format("dist-worker[%s]-job-executor", builder.clusterId);
            jobScheduler = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ScheduledThreadPoolExecutor(1, EnvProvider.INSTANCE.newThreadFactory(threadName)), threadName);
        } else {
            jobScheduler = builder.bgTaskExecutor;
        }

        if (builder.workerThreads == 0) {
            rpcExecutor = MoreExecutors.newDirectExecutorService();
        } else {
            rpcExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ThreadPoolExecutor(builder.workerThreads,
                    builder.workerThreads, 0L,
                    TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                    EnvProvider.INSTANCE.newThreadFactory("dist-worker-executor")), "dist-worker-executor");
        }
        distWorkerClient = builder.distWorkerClient;

        distWorkerServer = IBaseKVStoreServer.builder()
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
        cleaner = new DistWorkerCleaner(distWorkerClient, builder.gcInterval, jobScheduler);
        start();
    }

    public String id() {
        return distWorkerServer.storeId(clusterId);
    }

    private void start() {
        if (status.compareAndSet(Status.INIT, Status.STARTING)) {
            log.info("Starting dist worker");
            distWorkerServer.start();
            String storeId = distWorkerServer.storeId(clusterId);
            storeBalanceController.start(storeId);
            status.compareAndSet(Status.STARTING, Status.STARTED);
            distWorkerClient
                .connState()
                // observe the first READY state
                .filter(connState -> connState == IConnectable.ConnState.READY)
                .takeUntil(connState -> connState == IConnectable.ConnState.READY)
                .doOnComplete(() -> cleaner.start(storeId))
                .subscribe();
            log.debug("Dist worker started");
        }
    }

    public void close() {
        if (status.compareAndSet(Status.STARTED, Status.STOPPING)) {
            log.info("Stopping DistWorker");
            cleaner.stop().join();
            storeBalanceController.stop();
            distWorkerServer.stop();
            log.debug("Stopping CoProcFactory");
            coProcFactory.close();
            effectiveBalancerFactories.forEach(IDistWorkerBalancerFactory::close);
            log.debug("Closing message deliverer");
            messageDeliverer.close();
            MoreExecutors.shutdownAndAwaitTermination(rpcExecutor, 5, TimeUnit.SECONDS);
            if (jobExecutorOwner) {
                log.debug("Shutting down job executor");
                MoreExecutors.shutdownAndAwaitTermination(jobScheduler, 5, TimeUnit.SECONDS);
            }
            log.debug("DistWorker stopped");
            status.compareAndSet(Status.STOPPING, Status.STOPPED);
        }
    }

    private enum Status {
        INIT, STARTING, STARTED, STOPPING, STOPPED
    }
}
