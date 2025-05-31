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

package org.apache.bifromq.dist.worker;

import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import org.apache.bifromq.basekv.store.api.IKVRangeCoProc;
import org.apache.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import org.apache.bifromq.basekv.store.api.IKVRangeSplitHinter;
import org.apache.bifromq.basekv.store.range.hinter.MutationKVLoadBasedSplitHinter;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.deliverer.IMessageDeliverer;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.dist.worker.cache.ISubscriptionCache;
import org.apache.bifromq.dist.worker.cache.SubscriptionCache;
import org.apache.bifromq.dist.worker.hinter.FanoutSplitHinter;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.subbroker.ISubBrokerManager;
import org.apache.bifromq.sysprops.props.DistFanOutParallelism;
import org.apache.bifromq.sysprops.props.DistMatchParallelism;
import org.apache.bifromq.sysprops.props.DistWorkerFanOutSplitThreshold;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DistWorkerCoProcFactory implements IKVRangeCoProcFactory {
    private final IEventCollector eventCollector;
    private final IResourceThrottler resourceThrottler;
    private final IMessageDeliverer deliverer;
    private final ISubscriptionCleaner subscriptionChecker;
    private final ExecutorService matchExecutor;
    private final Duration loadEstWindow;
    private final int fanoutSplitThreshold = DistWorkerFanOutSplitThreshold.INSTANCE.get();

    public DistWorkerCoProcFactory(IDistClient distClient,
                                   IEventCollector eventCollector,
                                   IResourceThrottler resourceThrottler,
                                   ISubBrokerManager subBrokerManager,
                                   IMessageDeliverer messageDeliverer,
                                   Duration loadEstimateWindow) {
        this.eventCollector = eventCollector;
        this.resourceThrottler = resourceThrottler;
        this.loadEstWindow = loadEstimateWindow;
        this.deliverer = messageDeliverer;
        subscriptionChecker = new SubscriptionCleaner(subBrokerManager, distClient);

        matchExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ForkJoinPool(DistMatchParallelism.INSTANCE.get(), new ForkJoinPool.ForkJoinWorkerThreadFactory() {
                final AtomicInteger index = new AtomicInteger(0);

                @Override
                public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                    ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                    worker.setName(String.format("topic-matcher-%d", index.incrementAndGet()));
                    worker.setDaemon(false);
                    return worker;
                }
            }, null, false), "topic-matcher");
    }

    @Override
    public List<IKVRangeSplitHinter> createHinters(String clusterId, String storeId, KVRangeId id,
                                                   Supplier<IKVCloseableReader> readerProvider) {
        return List.of(
            new FanoutSplitHinter(readerProvider, fanoutSplitThreshold,
                "clusterId", clusterId, "storeId", storeId, "rangeId", KVRangeIdUtil.toString(id)),
            new MutationKVLoadBasedSplitHinter(loadEstWindow, Optional::of,
                "clusterId", clusterId, "storeId", storeId, "rangeId", KVRangeIdUtil.toString(id)));
    }

    @Override
    public IKVRangeCoProc createCoProc(String clusterId, String storeId, KVRangeId id,
                                       Supplier<IKVCloseableReader> rangeReaderProvider) {
        ISubscriptionCache routeCache = new SubscriptionCache(id, rangeReaderProvider, matchExecutor);
        ITenantsState tenantsState = new TenantsState(rangeReaderProvider.get(),
            "clusterId", clusterId, "storeId", storeId, "rangeId", KVRangeIdUtil.toString(id));

        IDeliverExecutorGroup deliverExecutorGroup = new DeliverExecutorGroup(
            deliverer, eventCollector, resourceThrottler, DistFanOutParallelism.INSTANCE.get());
        return new DistWorkerCoProc(
            id, rangeReaderProvider, routeCache, tenantsState, deliverExecutorGroup, subscriptionChecker);
    }

    public void close() {
        MoreExecutors.shutdownAndAwaitTermination(matchExecutor, 5, TimeUnit.SECONDS);
    }
}
