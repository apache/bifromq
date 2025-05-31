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

import static org.apache.bifromq.metrics.ITenantMeter.stopGauging;
import static org.apache.bifromq.metrics.TenantMetric.MqttConnectionGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttLivePersistentSessionGauge;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE_P_VALUE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE_T_VALUE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.metrics.TenantMetric;
import org.apache.bifromq.sessiondict.rpc.proto.ServerRedirection;
import org.apache.bifromq.type.ClientInfo;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SessionRegistryTest {
    private SimpleMeterRegistry meterRegistry;
    private SessionRegistry sessionRegistry;
    private final String tenantId1 = "tenant1";
    private final String tenantId2 = "tenant2";

    @BeforeMethod
    public void setUp() {
        sessionRegistry = new SessionRegistry();
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
    }

    @AfterMethod
    @SneakyThrows
    public void tearDown() {
        Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
        Metrics.globalRegistry.remove(meterRegistry);
        stopGauging(tenantId1, MqttConnectionGauge);
        stopGauging(tenantId1, MqttLivePersistentSessionGauge);
        stopGauging(tenantId2, MqttLivePersistentSessionGauge);
        stopGauging(tenantId2, MqttLivePersistentSessionGauge);
    }

    @Test
    public void testAddAndGetSession() {
        ISessionRegister registerMock = Mockito.mock(ISessionRegister.class);

        ClientInfo sessionOwner = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "user1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "client1")
            .build();

        sessionRegistry.add(sessionOwner, registerMock);

        Optional<ClientInfo> retrieved = sessionRegistry.get("tenant1", "user1", "client1");
        assertTrue(retrieved.isPresent());
        assertEquals(sessionOwner, retrieved.get());
        assertGaugeValue(sessionOwner.getTenantId(), MqttConnectionGauge, 1.0);
        assertGaugeValue(sessionOwner.getTenantId(), MqttLivePersistentSessionGauge, 0.0);
    }

    @Test
    public void testAddMultipleSessions() {
        ISessionRegister registerMock1 = Mockito.mock(ISessionRegister.class);
        ISessionRegister registerMock2 = Mockito.mock(ISessionRegister.class);

        ClientInfo sessionOwner1 = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "user1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "client1")
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_T_VALUE)
            .build();

        ClientInfo sessionOwner2 = ClientInfo.newBuilder()
            .setTenantId(tenantId2)
            .putMetadata(MQTT_USER_ID_KEY, "user2")
            .putMetadata(MQTT_CLIENT_ID_KEY, "client2")
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_P_VALUE)
            .build();

        sessionRegistry.add(sessionOwner1, registerMock1);
        sessionRegistry.add(sessionOwner2, registerMock2);

        Optional<ClientInfo> retrieved1 = sessionRegistry.get(tenantId1, "user1", "client1");
        Optional<ClientInfo> retrieved2 = sessionRegistry.get(tenantId2, "user2", "client2");

        assertTrue(retrieved1.isPresent());
        assertEquals(sessionOwner1, retrieved1.get());
        assertTrue(retrieved2.isPresent());
        assertEquals(sessionOwner2, retrieved2.get());
        assertGaugeValue(sessionOwner1.getTenantId(), MqttConnectionGauge, 1.0);
        assertGaugeValue(sessionOwner1.getTenantId(), MqttLivePersistentSessionGauge, 0.0);

        assertGaugeValue(sessionOwner2.getTenantId(), MqttConnectionGauge, 1.0);
        assertGaugeValue(sessionOwner2.getTenantId(), MqttLivePersistentSessionGauge, 1.0);
    }

    @Test
    public void testAddDuplicatePersistentSession() {
        ISessionRegister registerMock1 = Mockito.mock(ISessionRegister.class);
        ISessionRegister registerMock2 = Mockito.mock(ISessionRegister.class);

        ClientInfo sessionOwner = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "userId")
            .putMetadata(MQTT_CLIENT_ID_KEY, "clientId")
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_P_VALUE)
            .build();
        SessionRegistry sessionRegistry = new SessionRegistry();

        sessionRegistry.add(sessionOwner, registerMock1);
        assertGaugeValue(tenantId1, MqttLivePersistentSessionGauge, 1.0);

        sessionRegistry.add(sessionOwner, registerMock2);
        assertGaugeValue(tenantId1, MqttLivePersistentSessionGauge,
            1.0); // Should not increment because it's a duplicate key
    }


    @Test
    public void testAddSameClientKeyKicksPreviousSession() {
        ISessionRegister registerMock1 = Mockito.mock(ISessionRegister.class);
        ISessionRegister registerMock2 = Mockito.mock(ISessionRegister.class);

        ClientInfo sessionOwner1 = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "user1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "client1")
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_T_VALUE)
            .putMetadata(MQTT_CHANNEL_ID_KEY, "channel1")
            .build();

        ClientInfo sessionOwner2 = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "user1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "client1")
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_P_VALUE)
            .putMetadata(MQTT_CHANNEL_ID_KEY, "channel2")
            .build();

        sessionRegistry.add(sessionOwner1, registerMock1);

        sessionRegistry.add(sessionOwner2, registerMock2);

        Mockito.verify(registerMock1).kick(eq(tenantId1), eq(sessionOwner1),
            eq(sessionOwner2), eq(ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build()));

        Optional<ClientInfo> retrieved = sessionRegistry.get(tenantId1, "user1", "client1");
        assertTrue(retrieved.isPresent());
        assertEquals(sessionOwner2, retrieved.get());

        assertGaugeValue(sessionOwner2.getTenantId(), MqttConnectionGauge, 1.0);
        assertGaugeValue(sessionOwner2.getTenantId(), MqttLivePersistentSessionGauge, 1.0);
    }

    @Test
    public void testTransientKicksPersistent() {
        ISessionRegister registerMock1 = Mockito.mock(ISessionRegister.class);
        ISessionRegister registerMock2 = Mockito.mock(ISessionRegister.class);
        ClientInfo pSession = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "user1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "client1")
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_P_VALUE)
            .putMetadata(MQTT_CHANNEL_ID_KEY, "channel1")
            .build();
        ClientInfo tSession = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "user1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "client1")
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_T_VALUE)
            .putMetadata(MQTT_CHANNEL_ID_KEY, "channel2")
            .build();
        sessionRegistry.add(pSession, registerMock1);
        sessionRegistry.add(tSession, registerMock2);
        Mockito.verify(registerMock1).kick(eq(tenantId1), eq(pSession),
            eq(tSession), eq(ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build()));

        Optional<ClientInfo> retrieved = sessionRegistry.get(tenantId1, "user1", "client1");
        assertTrue(retrieved.isPresent());
        assertEquals(tSession, retrieved.get());

        assertGaugeValue(tenantId1, MqttConnectionGauge, 1.0);
        assertGaugeValue(tenantId1, MqttLivePersistentSessionGauge, 0.0);
    }

    @Test
    public void testRemoveSession() {
        ISessionRegister registerMock = Mockito.mock(ISessionRegister.class);

        ClientInfo sessionOwner = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "user1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "client1")
            .build();

        sessionRegistry.add(sessionOwner, registerMock);

        sessionRegistry.remove(sessionOwner, registerMock);

        Optional<ClientInfo> retrieved = sessionRegistry.get(tenantId1, "user1", "client1");
        assertFalse(retrieved.isPresent());
    }

    @Test
    public void testRemoveNonExistingSession() {
        ISessionRegister registerMock = Mockito.mock(ISessionRegister.class);

        ClientInfo sessionOwner = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "user1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "client1")
            .build();

        sessionRegistry.remove(sessionOwner, registerMock);

        Optional<ClientInfo> retrieved = sessionRegistry.get("tenant1", "user1", "client1");
        assertFalse(retrieved.isPresent());
    }

    @Test
    public void testFindRegistration() {
        ISessionRegister registerMock = Mockito.mock(ISessionRegister.class);

        ClientInfo sessionOwner = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "user1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "client1")
            .build();

        sessionRegistry.add(sessionOwner, registerMock);

        Optional<ISessionRegistry.SessionRegistration> registration =
            sessionRegistry.findRegistration("tenant1", "user1", "client1");

        assertTrue(registration.isPresent());
        assertEquals(sessionOwner, registration.get().sessionOwner());
        assertEquals(registerMock, registration.get().register());
    }

    @Test
    public void testFindRegistrationsForUser() {
        ISessionRegister registerMock1 = Mockito.mock(ISessionRegister.class);
        ISessionRegister registerMock2 = Mockito.mock(ISessionRegister.class);

        ClientInfo sessionOwner1 = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "user1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "client1")
            .build();

        ClientInfo sessionOwner2 = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "user1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "client2")
            .build();

        // 添加会话
        sessionRegistry.add(sessionOwner1, registerMock1);
        sessionRegistry.add(sessionOwner2, registerMock2);

        Iterable<ISessionRegistry.SessionRegistration> registrations =
            sessionRegistry.findRegistrations(tenantId1, "user1");

        List<ISessionRegistry.SessionRegistration> registrationList = new ArrayList<>();
        registrations.forEach(registrationList::add);

        assertEquals(2, registrationList.size());
        for (ISessionRegistry.SessionRegistration reg : registrationList) {
            assertTrue((reg.sessionOwner().equals(sessionOwner1) && reg.register().equals(registerMock1)) ||
                (reg.sessionOwner().equals(sessionOwner2) && reg.register().equals(registerMock2)));
        }
    }

    @Test
    public void testFindRegistrationsForTenant() {
        ISessionRegister registerMock1 = Mockito.mock(ISessionRegister.class);
        ISessionRegister registerMock2 = Mockito.mock(ISessionRegister.class);

        ClientInfo sessionOwner1 = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "user1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "client1")
            .build();

        ClientInfo sessionOwner2 = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "user2")
            .putMetadata(MQTT_CLIENT_ID_KEY, "client2")
            .build();

        sessionRegistry.add(sessionOwner1, registerMock1);
        sessionRegistry.add(sessionOwner2, registerMock2);

        Iterable<ISessionRegistry.SessionRegistration> registrations = sessionRegistry.findRegistrations("tenant1");

        List<ISessionRegistry.SessionRegistration> registrationList = new ArrayList<>();
        registrations.forEach(registrationList::add);

        assertEquals(2, registrationList.size());
        for (ISessionRegistry.SessionRegistration reg : registrationList) {
            assertTrue((reg.sessionOwner().equals(sessionOwner1) && reg.register().equals(registerMock1)) ||
                (reg.sessionOwner().equals(sessionOwner2) && reg.register().equals(registerMock2)));
        }
    }

    @Test
    public void testTenantMetric() {
        ISessionRegister register1 = Mockito.mock(ISessionRegister.class);
        ISessionRegister register2 = Mockito.mock(ISessionRegister.class);
        ClientInfo sessionOwner1 = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "userId")
            .putMetadata(MQTT_CLIENT_ID_KEY, "clientId")
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_T_VALUE)
            .build();
        ClientInfo sessionOwner2 = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "userId")
            .putMetadata(MQTT_CLIENT_ID_KEY, "clientId1")
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_P_VALUE)
            .build();
        SessionRegistry sessionRegistry = new SessionRegistry();

        assertNoGauge(tenantId1, MqttConnectionGauge);
        assertNoGauge(tenantId1, MqttLivePersistentSessionGauge);
        sessionRegistry.add(sessionOwner1, register1);
        sessionRegistry.add(sessionOwner2, register2);
        assertHasGauge(tenantId1, MqttConnectionGauge);
        assertGaugeValue(tenantId1, MqttConnectionGauge, 2.0);
        assertHasGauge(tenantId1, MqttLivePersistentSessionGauge);
        assertGaugeValue(tenantId1, MqttLivePersistentSessionGauge, 1.0);
        sessionRegistry.remove(sessionOwner1, register1);
        assertGaugeValue(tenantId1, MqttConnectionGauge, 1.0);
        assertGaugeValue(tenantId1, MqttLivePersistentSessionGauge, 1.0);
        sessionRegistry.remove(sessionOwner2, register2);
        assertNoGauge(tenantId1, MqttConnectionGauge);
        assertNoGauge(tenantId1, MqttLivePersistentSessionGauge);
    }

    private void assertNoGauge(String tenantId, TenantMetric tenantMetric) {
        Optional<Meter> gauge = getGauge(tenantId, tenantMetric);
        assertTrue(gauge.isEmpty());
    }

    private void assertHasGauge(String tenantId, TenantMetric tenantMetric) {
        Optional<Meter> gauge = getGauge(tenantId, tenantMetric);
        assertFalse(gauge.isEmpty());
    }

    private void assertGaugeValue(String tenantId, TenantMetric tenantMetric, double value) {
        Optional<Meter> meter = getGauge(tenantId, tenantMetric);
        assertTrue(meter.isPresent());
        assertEquals(((Gauge) meter.get()).value(), value);
    }

    private Optional<Meter> getGauge(String tenantId, TenantMetric tenantMetric) {
        return meterRegistry.getMeters().stream()
            .filter(m -> m.getId().getName().equals(tenantMetric.metricName)
                && tenantId.equals(m.getId().getTag(ITenantMeter.TAG_TENANT_ID))).findFirst();
    }
}