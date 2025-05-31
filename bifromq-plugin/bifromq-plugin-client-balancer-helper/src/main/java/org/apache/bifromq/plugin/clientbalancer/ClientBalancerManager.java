/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.plugin.clientbalancer;

import org.apache.bifromq.type.ClientInfo;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager for client balancer plugins.
 */
@Slf4j
public class ClientBalancerManager implements IClientBalancer, AutoCloseable {
    private static final Logger pluginLog = LoggerFactory.getLogger("plugin.manager");
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final IClientBalancer delegate;
    private final Timer callTimer;
    private final Counter callErrorCounter;

    public ClientBalancerManager(PluginManager pluginMgr) {
        Map<String, IClientBalancer> loadedClientBalancers =
            pluginMgr.getExtensions(IClientBalancer.class).stream()
                .collect(Collectors.toMap(e -> e.getClass().getName(), e -> e));
        if (loadedClientBalancers.isEmpty()) {
            pluginLog.warn("No client balancer plugin available");
            delegate = new DummyClientBalancer();
        } else {
            if (loadedClientBalancers.size() > 1) {
                pluginLog.warn("Multiple client balancer plugins available, use the first found");
            }
            String clientBalancerFQN = loadedClientBalancers.keySet().iterator().next();
            pluginLog.info("Client balancer loaded: {}", clientBalancerFQN);
            delegate = loadedClientBalancers.get(clientBalancerFQN);
        }
        callTimer = Timer.builder("call.exec.timer")
            .tag("method", "ClientBalancer/needRedirect")
            .tag("type", delegate.getClass().getName())
            .register(Metrics.globalRegistry);
        callErrorCounter = Counter.builder("call.exec.fail.count")
            .tag("method", "ClientBalancer/needRedirect")
            .tag("type", delegate.getClass().getName())
            .register(Metrics.globalRegistry);
    }


    @Override
    public Optional<Redirection> needRedirect(ClientInfo clientInfo) {
        assert !stopped.get();
        Timer.Sample sample = Timer.start();
        try {
            Optional<Redirection> redirection = delegate.needRedirect(clientInfo);
            sample.stop(callTimer);
            return redirection;
        } catch (Throwable e) {
            pluginLog.error("Client balancer plugin error", e);
            callErrorCounter.increment();
            return Optional.empty();
        }
    }

    @Override
    public void close() {
        if (stopped.compareAndSet(false, true)) {
            log.debug("Closing client balancer manager");
            try {
                delegate.close();
            } catch (Throwable e) {
                pluginLog.error("Failed to close client balancer plugin", e);
            }
            Metrics.globalRegistry.remove(callTimer);
            Metrics.globalRegistry.remove(callErrorCounter);
            log.debug("Client balancer manager closed");
        }
    }
}
