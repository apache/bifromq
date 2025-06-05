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

package org.apache.bifromq.plugin.resourcethrottler;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class ResourceThrottlerManager implements IResourceThrottler, AutoCloseable {
    private static final Logger pluginLog = LoggerFactory.getLogger("plugin.manager");
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final IResourceThrottler delegate;
    private final Timer callTimer;
    private final Counter callErrorCounter;

    public ResourceThrottlerManager(String resourceThrottlerFQN, PluginManager pluginMgr) {
        Map<String, IResourceThrottler> availResourceThrottlers =
            pluginMgr.getExtensions(IResourceThrottler.class).stream()
                .collect(Collectors.toMap(e -> e.getClass().getName(), e -> e));
        if (availResourceThrottlers.isEmpty()) {
            pluginLog.warn("No resource throttler plugin available, use DEV ONLY one instead");
            delegate = new DevOnlyResourceThrottler();
        } else {
            if (resourceThrottlerFQN == null) {
                pluginLog.warn("Resource throttler type class not specified, use DEV ONLY one instead");
                delegate = new DevOnlyResourceThrottler();
            } else if (!availResourceThrottlers.containsKey(resourceThrottlerFQN)) {
                pluginLog.warn("Resource throttler type '{}' not found, use DEV ONLY one instead",
                    resourceThrottlerFQN);
                delegate = new DevOnlyResourceThrottler();
            } else {
                pluginLog.info("Resource throttler loaded: {}", resourceThrottlerFQN);
                delegate = availResourceThrottlers.get(resourceThrottlerFQN);
            }
        }
        callTimer = Timer.builder("call.exec.timer")
            .tag("method", "ResourceThrottler/hasResource")
            .tag("type", delegate.getClass().getName())
            .register(Metrics.globalRegistry);
        callErrorCounter = Counter.builder("call.exec.fail.count")
            .tag("method", "ResourceThrottler/hasResource")
            .tag("type", delegate.getClass().getName())
            .register(Metrics.globalRegistry);
    }

    @Override
    public boolean hasResource(String tenantId, TenantResourceType type) {
        assert !stopped.get();
        Timer.Sample sample = Timer.start();
        try {
            boolean isEnough = delegate.hasResource(tenantId, type);
            sample.stop(callTimer);
            return isEnough;
        } catch (Throwable e) {
            pluginLog.error("Resource throttler throws exception: type={}", type, e);
            callErrorCounter.increment();
            return true;
        }
    }

    IResourceThrottler getDelegate() {
        return delegate;
    }

    @Override
    public void close() {
        if (stopped.compareAndSet(false, true)) {
            log.debug("Closing resource throttler manager");
            try {
                delegate.close();
            } catch (Throwable e) {
                pluginLog.error("Failed to close resource throttler plugin", e);
            }
            Metrics.globalRegistry.remove(callTimer);
            Metrics.globalRegistry.remove(callErrorCounter);
            log.debug("Resource throttler manager closed");
        }
    }
}
