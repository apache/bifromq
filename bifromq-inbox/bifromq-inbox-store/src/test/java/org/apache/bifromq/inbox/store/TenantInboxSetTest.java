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

package org.apache.bifromq.inbox.store;

import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentSessionNumGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentSessionSpaceGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentSubCountGauge;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.inbox.storage.proto.InboxMetadata;
import org.apache.bifromq.inbox.storage.proto.TopicFilterOption;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.session.MQTTSessionStart;
import org.apache.bifromq.plugin.eventcollector.session.MQTTSessionStop;
import org.apache.bifromq.type.ClientInfo;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class TenantInboxSetTest extends MeterTest {
    @Mock
    private IEventCollector eventCollector;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        super.setup();
        closeable = MockitoAnnotations.openMocks(this);
    }

    @SneakyThrows
    @AfterMethod
    public void tearDown() {
        closeable.close();
        super.tearDown();
    }

    @Test
    public void testSetupMeterAfterCreate() {
        String tenantId = "tenantId-" + System.nanoTime();
        assertNoGauge(tenantId, MqttPersistentSubCountGauge);
        assertNoGauge(tenantId, MqttPersistentSessionNumGauge);
        assertNoGauge(tenantId, MqttPersistentSessionSpaceGauge);
        // mock Supplier<Number> using Mockito
        Supplier<Number> usedSpaceGetter = () -> 1;
        TenantInboxSet inboxSet = new TenantInboxSet(eventCollector, tenantId, usedSpaceGetter);

        assertGaugeValue(tenantId, MqttPersistentSubCountGauge, 0);
        assertGaugeValue(tenantId, MqttPersistentSessionNumGauge, 0);
        assertGaugeValue(tenantId, MqttPersistentSessionSpaceGauge, 1);
        inboxSet.destroy();
    }

    @Test
    public void testMeterClearedAfterDestroy() {
        String tenantId = "tenantId-" + System.nanoTime();
        Supplier<Number> usedSpaceGetter = () -> 1;
        TenantInboxSet inboxSet = new TenantInboxSet(eventCollector, tenantId, usedSpaceGetter, "tag1", "value1");
        inboxSet.destroy();
        assertNoGauge(tenantId, MqttPersistentSubCountGauge);
        assertNoGauge(tenantId, MqttPersistentSessionNumGauge);
        assertNoGauge(tenantId, MqttPersistentSessionSpaceGauge);
    }

    @Test
    public void testUpsert() {
        String tenantId = "tenantId-" + System.nanoTime();
        Supplier<Number> usedSpaceGetter = Mockito.mock(Supplier.class);
        when(usedSpaceGetter.get()).thenReturn(1);
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId("tenant1").build();
        InboxMetadata inboxMetadata = InboxMetadata.newBuilder()
                .setInboxId("testInboxId")
                .setIncarnation(1)
                .setClient(clientInfo)
                .build();
        TenantInboxSet inboxSet = new TenantInboxSet(eventCollector, tenantId, usedSpaceGetter);
        inboxSet.upsert(inboxMetadata);
        assertGaugeValue(tenantId, MqttPersistentSubCountGauge, 0);
        assertGaugeValue(tenantId, MqttPersistentSessionNumGauge, 1);
        assertGaugeValue(tenantId, MqttPersistentSessionSpaceGauge, 1);

        verify(eventCollector).report(argThat(e -> (e instanceof MQTTSessionStart) &&
                ((MQTTSessionStart) e).sessionId().equals("testInboxId") &&
                ((MQTTSessionStart) e).clientInfo().equals(clientInfo)));

        inboxMetadata = InboxMetadata.newBuilder()
                .setInboxId("testInboxId")
                .setIncarnation(1)
                .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
                .build();

        reset(eventCollector);
        inboxSet.upsert(inboxMetadata);
        verify(eventCollector, never()).report(any());
        assertGaugeValue(tenantId, MqttPersistentSubCountGauge, 1);
        assertGaugeValue(tenantId, MqttPersistentSessionNumGauge, 1);
        assertGaugeValue(tenantId, MqttPersistentSessionSpaceGauge, 1);

        when(usedSpaceGetter.get()).thenReturn(2);
        inboxMetadata = InboxMetadata.newBuilder()
                .setInboxId("testInboxId1")
                .setIncarnation(1)
                .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
                .build();

        reset(eventCollector);
        inboxSet.upsert(inboxMetadata);
        verify(eventCollector).report(argThat(e -> (e instanceof MQTTSessionStart) &&
                ((MQTTSessionStart) e).sessionId().equals("testInboxId1")));
        assertGaugeValue(tenantId, MqttPersistentSubCountGauge, 2);
        assertGaugeValue(tenantId, MqttPersistentSessionNumGauge, 2);
        assertGaugeValue(tenantId, MqttPersistentSessionSpaceGauge, 2);

    }

    @Test
    public void testRemove() {
        String tenantId = "tenantId-" + System.nanoTime();
        Supplier<Number> usedSpaceGetter = Mockito.mock(Supplier.class);
        when(usedSpaceGetter.get()).thenReturn(1);
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId("tenant1").build();
        final InboxMetadata inboxMetadata = InboxMetadata.newBuilder()
                .setInboxId("testInboxId")
                .setIncarnation(1)
                .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
                .setClient(clientInfo)
                .build();
        final InboxMetadata inboxMetadata1 = InboxMetadata.newBuilder()
                .setInboxId("testInboxId1")
                .setIncarnation(1)
                .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
                .setClient(clientInfo)
                .build();
        TenantInboxSet inboxSet = new TenantInboxSet(eventCollector, tenantId, usedSpaceGetter);
        inboxSet.upsert(inboxMetadata);
        inboxSet.upsert(inboxMetadata1);
        assertGaugeValue(tenantId, MqttPersistentSubCountGauge, 2);
        assertGaugeValue(tenantId, MqttPersistentSessionNumGauge, 2);

        reset(eventCollector);
        inboxSet.remove(inboxMetadata.getInboxId(), inboxMetadata.getIncarnation());
        verify(eventCollector).report(argThat(e -> (e instanceof MQTTSessionStop) &&
                ((MQTTSessionStop) e).sessionId().equals(inboxMetadata.getInboxId()) &&
                ((MQTTSessionStop) e).clientInfo().equals(clientInfo)));
        assertGaugeValue(tenantId, MqttPersistentSubCountGauge, 1);
        assertGaugeValue(tenantId, MqttPersistentSessionNumGauge, 1);

        assertFalse(inboxSet.isEmpty());
        reset(eventCollector);
        inboxSet.remove(inboxMetadata1.getInboxId(), inboxMetadata1.getIncarnation());

        verify(eventCollector).report(argThat(e -> (e instanceof MQTTSessionStop) &&
                ((MQTTSessionStop) e).sessionId().equals(inboxMetadata1.getInboxId()) &&
                ((MQTTSessionStop) e).clientInfo().equals(clientInfo)));

        assertGaugeValue(tenantId, MqttPersistentSubCountGauge, 0);
        assertGaugeValue(tenantId, MqttPersistentSessionNumGauge, 0);
        assertTrue(inboxSet.isEmpty());
    }

    @Test
    public void testGetInboxMetadata() {
        String tenantId = "tenantId-" + System.nanoTime();
        Supplier<Number> usedSpaceGetter = Mockito.mock(Supplier.class);
        when(usedSpaceGetter.get()).thenReturn(1);
        InboxMetadata inboxMetadata = InboxMetadata.newBuilder()
                .setInboxId("testInboxId")
                .setIncarnation(1)
                .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
                .build();
        TenantInboxSet inboxSet = new TenantInboxSet(eventCollector, tenantId, usedSpaceGetter);
        inboxSet.upsert(inboxMetadata);
        assertTrue(inboxSet.get(inboxMetadata.getInboxId(), inboxMetadata.getIncarnation()).isPresent());
        assertEquals(inboxSet.get(inboxMetadata.getInboxId(), inboxMetadata.getIncarnation()).get(), inboxMetadata);
        inboxSet.remove(inboxMetadata.getInboxId(), inboxMetadata.getIncarnation());
        assertFalse(inboxSet.get(inboxMetadata.getInboxId(), inboxMetadata.getIncarnation()).isPresent());
    }

    @Test
    public void testGetAll() {
        String tenantId = "tenantId-" + System.nanoTime();
        Supplier<Number> usedSpaceGetter = Mockito.mock(Supplier.class);
        when(usedSpaceGetter.get()).thenReturn(1);
        InboxMetadata inboxMetadata = InboxMetadata.newBuilder()
                .setInboxId("testInboxId")
                .setIncarnation(1)
                .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
                .build();

        InboxMetadata inboxMetadata1 = InboxMetadata.newBuilder()
                .setInboxId("testInboxId")
                .setIncarnation(2)
                .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
                .build();
        InboxMetadata inboxMetadata2 = InboxMetadata.newBuilder()
                .setInboxId("testInboxId1")
                .setIncarnation(1)
                .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
                .build();
        TenantInboxSet inboxSet = new TenantInboxSet(eventCollector, tenantId, usedSpaceGetter);
        inboxSet.upsert(inboxMetadata);
        inboxSet.upsert(inboxMetadata1);
        inboxSet.upsert(inboxMetadata2);
        assertEquals(inboxSet.getAll().size(), 2);
        assertEquals(inboxSet.getAll(inboxMetadata.getInboxId()).size(), 2);
        assertEquals(inboxSet.getAll(inboxMetadata2.getInboxId()).size(), 1);
        inboxSet.destroy();
    }

    @Test
    public void testRemoveAll() {
        String tenantId = "tenantId-" + System.nanoTime();
        Supplier<Number> usedSpaceGetter = Mockito.mock(Supplier.class);
        when(usedSpaceGetter.get()).thenReturn(1);
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId("tenant1").build();
        final InboxMetadata inboxMetadata = InboxMetadata.newBuilder()
                .setInboxId("testInboxId")
                .setIncarnation(1)
                .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
                .setClient(clientInfo)
                .build();
        final InboxMetadata inboxMetadata1 = InboxMetadata.newBuilder()
                .setInboxId("testInboxId1")
                .setIncarnation(1)
                .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
                .setClient(clientInfo)
                .build();
        final InboxMetadata inboxMetadata11 = InboxMetadata.newBuilder()
            .setInboxId("testInboxId1")
            .setIncarnation(2)
            .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
            .setClient(clientInfo)
            .build();
        TenantInboxSet inboxSet = new TenantInboxSet(eventCollector, tenantId, usedSpaceGetter);
        inboxSet.upsert(inboxMetadata);
        inboxSet.upsert(inboxMetadata1);
        inboxSet.upsert(inboxMetadata11);

        reset(eventCollector);
        inboxSet.removeAll();
        verify(eventCollector, times(2)).report(any(MQTTSessionStop.class));
        assertGaugeValue(tenantId, MqttPersistentSubCountGauge, 0);
        assertGaugeValue(tenantId, MqttPersistentSessionNumGauge, 0);
        assertTrue(inboxSet.isEmpty());
    }
}
