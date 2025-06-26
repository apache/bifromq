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

package org.apache.bifromq.starter.module;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Names;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.micrometer.core.instrument.binder.netty4.NettyEventExecutorMetrics;
import io.netty.channel.EventLoopGroup;
import jakarta.inject.Singleton;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.baseenv.NettyEnv;
import org.apache.bifromq.starter.config.StandaloneConfig;

public class ExecutorsModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(EventLoopGroup.class).annotatedWith(Names.named("rpcClientEventLoop"))
            .toProvider(RPCClientEventLoopProvider.class)
            .in(Singleton.class);
        bind(ScheduledExecutorService.class).annotatedWith(Names.named("bgTaskScheduler"))
            .toProvider(BackgroundTaskSchedulerProvider.class)
            .in(Singleton.class);
    }

    private static class RPCClientEventLoopProvider implements Provider<EventLoopGroup> {
        private final StandaloneConfig config;

        @Inject
        private RPCClientEventLoopProvider(StandaloneConfig config) {
            this.config = config;
        }

        @Override
        public EventLoopGroup get() {
            EventLoopGroup eventLoopGroup = NettyEnv.createEventLoopGroup(
                config.getRpcConfig().getClientEventLoopThreads(), "rpc-client-worker-elg");
            new NettyEventExecutorMetrics(eventLoopGroup).bindTo(Metrics.globalRegistry);
            return eventLoopGroup;
        }
    }

    private static class BackgroundTaskSchedulerProvider implements Provider<ScheduledExecutorService> {
        private final StandaloneConfig config;

        @Inject
        private BackgroundTaskSchedulerProvider(StandaloneConfig config) {
            this.config = config;
        }

        @Override
        public ScheduledExecutorService get() {
            return ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ScheduledThreadPoolExecutor(config.getBgTaskThreads(),
                    EnvProvider.INSTANCE.newThreadFactory("bg-task-executor")), "bg-task-executor");
        }
    }
}
