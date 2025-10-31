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

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bifromq.baseenv.EnvProvider;

class FetchSignalSender {
    public static ExecutorService INSTANCE = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
        new ForkJoinPool(Math.max(2, EnvProvider.INSTANCE.availableProcessors() / 4),
            new ForkJoinPool.ForkJoinWorkerThreadFactory() {
                final AtomicInteger index = new AtomicInteger(0);

                @Override
                public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                    ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                    worker.setName(String.format("fetch-signaler-%d", index.incrementAndGet()));
                    worker.setDaemon(true);
                    return worker;
                }
            }, null, false), "fetch-signaler");
}
