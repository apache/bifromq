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

package org.apache.bifromq.retain.server;

import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.deliverer.BatchDeliveryCallBuilderFactory;
import org.apache.bifromq.deliverer.MessageDeliverer;
import org.apache.bifromq.retain.RPCBluePrint;
import org.apache.bifromq.retain.server.scheduler.DeleteCallScheduler;
import org.apache.bifromq.retain.server.scheduler.MatchCallScheduler;
import org.apache.bifromq.retain.server.scheduler.RetainCallScheduler;
import org.apache.bifromq.retain.store.gc.RetainStoreGCProcessor;
import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RetainServer implements IRetainServer {
    private final RetainService retainService;
    private final ExecutorService rpcExecutor;

    RetainServer(RetainServerBuilder builder) {
        this.retainService = new RetainService(
            new RetainStoreGCProcessor(builder.retainStoreClient, null),
            new MessageDeliverer(new BatchDeliveryCallBuilderFactory(builder.distClient, builder.subBrokerManager)),
            new MatchCallScheduler(builder.retainStoreClient, builder.settingProvider),
            new RetainCallScheduler(builder.retainStoreClient),
            new DeleteCallScheduler(builder.retainStoreClient));
        if (builder.workerThreads == 0) {
            rpcExecutor = MoreExecutors.newDirectExecutorService();
        } else {
            rpcExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ThreadPoolExecutor(builder.workerThreads,
                    builder.workerThreads, 0L,
                    TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                    EnvProvider.INSTANCE.newThreadFactory("retain-server-executor")), "retain-server-executor");
        }
        builder.rpcServerBuilder.bindService(retainService.bindService(),
            RPCBluePrint.INSTANCE,
            builder.attributes,
            builder.defaultGroupTags,
            rpcExecutor);
    }

    @Override
    public void close() {
        log.info("Stopping RetainService");
        retainService.close();
        MoreExecutors.shutdownAndAwaitTermination(rpcExecutor, 5, TimeUnit.SECONDS);
        log.debug("RetainService stopped");
    }
}
