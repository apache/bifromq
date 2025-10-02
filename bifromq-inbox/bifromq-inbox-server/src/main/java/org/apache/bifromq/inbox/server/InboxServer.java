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

package org.apache.bifromq.inbox.server;

import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.inbox.RPCBluePrint;
import org.apache.bifromq.inbox.server.scheduler.InboxAttachScheduler;
import org.apache.bifromq.inbox.server.scheduler.InboxCheckSubScheduler;
import org.apache.bifromq.inbox.server.scheduler.InboxCommitScheduler;
import org.apache.bifromq.inbox.server.scheduler.InboxDeleteScheduler;
import org.apache.bifromq.inbox.server.scheduler.InboxDetachScheduler;
import org.apache.bifromq.inbox.server.scheduler.InboxExistScheduler;
import org.apache.bifromq.inbox.server.scheduler.InboxFetchScheduler;
import org.apache.bifromq.inbox.server.scheduler.InboxFetchStateScheduler;
import org.apache.bifromq.inbox.server.scheduler.InboxInsertScheduler;
import org.apache.bifromq.inbox.server.scheduler.InboxSendLWTScheduler;
import org.apache.bifromq.inbox.server.scheduler.InboxSubScheduler;
import org.apache.bifromq.inbox.server.scheduler.InboxUnSubScheduler;

@Slf4j
class InboxServer implements IInboxServer {
    private final InboxService inboxService;
    private final ExecutorService rpcExecutor;

    InboxServer(InboxServerBuilder builder) {
        this.inboxService = InboxService.builder()
            .inboxClient(builder.inboxClient)
            .distClient(builder.distClient)
            .fetchStateScheduler(new InboxFetchStateScheduler(builder.inboxStoreClient))
            .existScheduler(new InboxExistScheduler(builder.inboxStoreClient))
            .sendLWTScheduler(new InboxSendLWTScheduler(builder.inboxStoreClient))
            .checkSubScheduler(new InboxCheckSubScheduler(builder.inboxStoreClient))
            .fetchScheduler(new InboxFetchScheduler(builder.inboxStoreClient))
            .insertScheduler(new InboxInsertScheduler(builder.inboxStoreClient))
            .commitScheduler(new InboxCommitScheduler(builder.inboxStoreClient))
            .attachScheduler(new InboxAttachScheduler(builder.inboxStoreClient))
            .detachScheduler(new InboxDetachScheduler(builder.inboxStoreClient))
            .deleteScheduler(new InboxDeleteScheduler(builder.inboxStoreClient))
            .subScheduler(new InboxSubScheduler(builder.inboxStoreClient))
            .unsubScheduler(new InboxUnSubScheduler(builder.inboxStoreClient))
            .tenantGCRunner(new TenantGCRunner(builder.inboxStoreClient))
            .build();
        if (builder.workerThreads == 0) {
            rpcExecutor = MoreExecutors.newDirectExecutorService();
        } else {
            rpcExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ThreadPoolExecutor(builder.workerThreads,
                    builder.workerThreads, 0L,
                    TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                    EnvProvider.INSTANCE.newThreadFactory("inbox-server-executor")), "inbox-server-executor");
        }
        builder.rpcServerBuilder.bindService(inboxService.bindService(),
            RPCBluePrint.INSTANCE,
            builder.attributes,
            builder.defaultGroupTags,
            rpcExecutor);
        start();
    }

    private void start() {
        log.debug("Starting inbox service");
        inboxService.start();
    }

    @SneakyThrows
    @Override
    public void close() {
        log.info("Stopping InboxService");
        inboxService.stop();
        MoreExecutors.shutdownAndAwaitTermination(rpcExecutor, 5, TimeUnit.SECONDS);
        log.info("InboxService stopped");
    }
}
