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

package org.apache.bifromq.mqtt.service;

import org.apache.bifromq.mqtt.session.IMQTTSession;
import com.google.common.util.concurrent.RateLimiter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LocalSessionRegistry implements ILocalSessionRegistry {
    private final ConcurrentMap<String, IMQTTSession> sessionMap = new ConcurrentHashMap<>();
    private final Gauge connNumGauge;

    public LocalSessionRegistry() {
        connNumGauge = Gauge.builder("mqtt.server.connection.gauge", sessionMap::size)
            .register(Metrics.globalRegistry);
    }

    @Override
    public void add(String sessionId, IMQTTSession session) {
        sessionMap.putIfAbsent(sessionId, session);
    }

    @Override
    public boolean remove(String sessionId, IMQTTSession session) {
        return sessionMap.remove(sessionId, session);
    }

    public IMQTTSession get(String sessionId) {
        return sessionMap.get(sessionId);
    }

    @Override
    public CompletableFuture<Void> disconnectAll(int disconnectRate) {
        RateLimiter limiter = RateLimiter.create(Math.max(1, disconnectRate));
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String sessionId : sessionMap.keySet()) {
            limiter.acquire();
            futures.add(disconnect(sessionId));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }


    private CompletableFuture<Void> disconnect(String sessionId) {
        IMQTTSession session = sessionMap.remove(sessionId);
        if (session != null) {
            return session.disconnect();
        }
        return CompletableFuture.completedFuture(null);
    }
}
