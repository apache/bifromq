/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

import org.apache.bifromq.basehookloader.BaseHookLoader;
import org.apache.bifromq.basescheduler.spi.ICallScheduler;
import org.apache.bifromq.basescheduler.spi.ICallSchedulerFactory;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class CallSchedulerFactory implements ICallSchedulerFactory {
    public static final ICallSchedulerFactory INSTANCE = new CallSchedulerFactory();

    private final ICallSchedulerFactory delegate;

    private CallSchedulerFactory() {
        Map<String, ICallSchedulerFactory> factoryMap = BaseHookLoader.load(ICallSchedulerFactory.class);
        if (factoryMap.isEmpty()) {
            delegate = FallbackCallSchedulerFactory.INSTANCE;
        } else {
            delegate = factoryMap.values().iterator().next();
        }
    }

    @Override
    public <ReqT> ICallScheduler<ReqT> create(String name) {
        try {
            ICallScheduler<ReqT> scheduler = delegate.create(name);
            if (scheduler == null) {
                return FallbackCallSchedulerFactory.INSTANCE.create(name);
            }
            return scheduler;
        } catch (Throwable e) {
            log.error("Failed to create pipelineDepthEstimator: scheduler={}", name, e);
            return FallbackCallSchedulerFactory.INSTANCE.create(name);
        }
    }

    private static class FallbackCallSchedulerFactory implements ICallSchedulerFactory {
        private static final ICallSchedulerFactory INSTANCE = new FallbackCallSchedulerFactory();

        @Override
        public <ReqT> ICallScheduler<ReqT> create(String name) {
            return new ICallScheduler<>() {
            };
        }
    }
}
