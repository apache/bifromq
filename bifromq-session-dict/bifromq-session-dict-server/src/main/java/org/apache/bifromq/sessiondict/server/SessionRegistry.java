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

package org.apache.bifromq.sessiondict.server;

import static org.apache.bifromq.metrics.ITenantMeter.gauging;
import static org.apache.bifromq.metrics.ITenantMeter.stopGauging;
import static org.apache.bifromq.metrics.TenantMetric.MqttConnectionGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttLivePersistentSessionGauge;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE_P_VALUE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE_T_VALUE;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Maps;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bifromq.sessiondict.rpc.proto.ServerRedirection;
import org.apache.bifromq.type.ClientInfo;

class SessionRegistry implements ISessionRegistry {
    private static final ServerRedirection NO_MOVE =
        ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build();
    static Comparator<MqttClientKey> ClientKeyComparator = (key1, key2) -> Comparator
        .comparing(MqttClientKey::userId)
        .thenComparing(MqttClientKey::clientId)
        .compare(key1, key2);
    private static final NavigableMap<MqttClientKey, ClientInfo> EMPTY_MAP = new TreeMap<>(ClientKeyComparator);
    private final Map<String, NavigableMap<MqttClientKey, ClientInfo>> tenantSessions = Maps.newConcurrentMap();
    private final Map<String, SessionCounter> sessionCounters = Maps.newConcurrentMap();
    private final Map<ClientInfo, ISessionRegister> clientRegisterMap = Maps.newConcurrentMap();

    @Override
    public void add(ClientInfo sessionOwner, ISessionRegister register) {
        String tenantId = sessionOwner.getTenantId();
        MqttClientKey clientKey = MqttClientKey.from(sessionOwner);
        tenantSessions.compute(tenantId, (k, v) -> {
            if (v == null) {
                v = new ConcurrentSkipListMap<>(ClientKeyComparator);
                SessionCounter sessionCounter = new SessionCounter();
                sessionCounters.put(tenantId, sessionCounter);
                gauging(tenantId, MqttConnectionGauge, sessionCounter.total::get);
                gauging(tenantId, MqttLivePersistentSessionGauge, sessionCounter.persistent::get);
            }
            ClientInfo prevSessionOwner = v.put(clientKey, sessionOwner);
            if (prevSessionOwner != null) {
                if (!prevSessionOwner.equals(sessionOwner)) {
                    ISessionRegister prevSessionRegister = clientRegisterMap.remove(prevSessionOwner);
                    clientRegisterMap.put(sessionOwner, register);
                    // kick previous session owner
                    assert prevSessionRegister != null;
                    prevSessionRegister.kick(tenantId, prevSessionOwner, sessionOwner, NO_MOVE);
                    if (isPersistent(sessionOwner) && !isPersistent(prevSessionOwner)) {
                        // kicked by a persistent session
                        SessionCounter sessionCounter = sessionCounters.get(tenantId);
                        sessionCounter.persistent.incrementAndGet();
                    } else if (!isPersistent(sessionOwner) && isPersistent(prevSessionOwner)) {
                        SessionCounter sessionCounter = sessionCounters.get(tenantId);
                        sessionCounter.persistent.decrementAndGet();
                    }
                } else {
                    ISessionRegister prevSessionRegister = clientRegisterMap.put(sessionOwner, register);
                    if (prevSessionRegister != null && prevSessionRegister != register) {
                        prevSessionRegister.kick(tenantId, prevSessionOwner, sessionOwner, NO_MOVE);
                    }
                }
                // ignore duplicated add
            } else {
                // new session
                clientRegisterMap.put(sessionOwner, register);
                SessionCounter sessionCounter = sessionCounters.get(tenantId);
                sessionCounter.total.incrementAndGet();
                if (isPersistent(sessionOwner)) {
                    sessionCounter.persistent.incrementAndGet();
                }
            }
            return v;
        });
    }

    @Override
    public void remove(ClientInfo sessionOwner, ISessionRegister register) {
        String tenantId = sessionOwner.getTenantId();
        MqttClientKey clientKey = MqttClientKey.from(sessionOwner);
        tenantSessions.computeIfPresent(tenantId, (k, v) -> {
            boolean s1 = v.remove(clientKey, sessionOwner);
            boolean s2 = clientRegisterMap.remove(sessionOwner, register);
            if (s1) {
                SessionCounter sessionCounter = sessionCounters.get(tenantId);
                sessionCounter.total.decrementAndGet();
                if (isPersistent(sessionOwner)) {
                    sessionCounter.persistent.decrementAndGet();
                }
            }
            if (v.isEmpty()) {
                sessionCounters.remove(k);
                stopGauging(tenantId, MqttConnectionGauge);
                stopGauging(tenantId, MqttLivePersistentSessionGauge);
                return null;
            }
            return v;
        });
    }

    @Override
    public Optional<ClientInfo> get(String tenantId, String userId, String mqttClientId) {
        return Optional.ofNullable(
            tenantSessions.getOrDefault(tenantId, EMPTY_MAP).get(new MqttClientKey(userId, mqttClientId)));
    }

    @Override
    public Optional<SessionRegistration> findRegistration(String tenantId, String userId, String mqttClientId) {
        Optional<ClientInfo> sessionOwner = get(tenantId, userId, mqttClientId);
        if (sessionOwner.isPresent()) {
            ISessionRegister sessionRegister = clientRegisterMap.get(sessionOwner.get());
            if (sessionRegister != null) {
                return Optional.of(
                    new SessionRegistration(sessionOwner.get(), clientRegisterMap.get(sessionOwner.get())));
            }
        }
        return Optional.empty();
    }

    @Override
    public Iterable<SessionRegistration> findRegistrations(String tenantId, String userId) {
        return () -> new AbstractIterator<>() {
            private final MqttClientKey fromKey = new MqttClientKey(userId, "");
            private final MqttClientKey toKey = new MqttClientKey(userId + "\0", "");
            private final Iterator<ClientInfo> sessionOwners = tenantSessions
                .getOrDefault(tenantId, EMPTY_MAP)
                .subMap(fromKey, true, toKey, false)
                .values()
                .iterator();

            @Override
            protected SessionRegistration computeNext() {
                if (sessionOwners.hasNext()) {
                    ClientInfo sessionOwner = sessionOwners.next();
                    ISessionRegister sessionRegister = clientRegisterMap.get(sessionOwner);
                    if (sessionRegister != null) {
                        return new SessionRegistration(sessionOwner, sessionRegister);
                    }
                    return computeNext();
                } else {
                    return endOfData();
                }
            }
        };
    }

    @Override
    public Iterable<SessionRegistration> findRegistrations(String tenantId) {
        return () -> new AbstractIterator<>() {
            private final Iterator<ClientInfo> sessionOwners = tenantSessions
                .getOrDefault(tenantId, EMPTY_MAP)
                .values()
                .iterator();

            @Override
            protected SessionRegistration computeNext() {
                if (sessionOwners.hasNext()) {
                    ClientInfo sessionOwner = sessionOwners.next();
                    ISessionRegister sessionRegister = clientRegisterMap.get(sessionOwner);
                    if (sessionRegister != null) {
                        return new SessionRegistration(sessionOwner, sessionRegister);
                    }
                    return computeNext();
                } else {
                    return endOfData();
                }
            }
        };
    }

    @Override
    public void close() {
        for (String tenantId : tenantSessions.keySet()) {
            stopGauging(tenantId, MqttConnectionGauge);
            stopGauging(tenantId, MqttLivePersistentSessionGauge);
        }
    }

    private boolean isPersistent(ClientInfo sessionOwner) {
        return sessionOwner.getMetadataOrDefault(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_T_VALUE)
            .equals(MQTT_CLIENT_SESSION_TYPE_P_VALUE);
    }

    private static class SessionCounter {
        private final AtomicInteger total = new AtomicInteger();
        private final AtomicInteger persistent = new AtomicInteger();
    }
}
