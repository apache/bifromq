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

package org.apache.bifromq.inbox.store;

import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.inboxInstanceStartKey;
import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentSessionNumGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentSessionSpaceGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentSubCountGauge;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.inbox.storage.proto.InboxMetadata;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.type.TopicFilterOption;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TenantsStatsTest {
    private SimpleMeterRegistry meterRegistry;

    private static InboxMetadata meta(String inboxId, long incarnation, int topicFiltersCount) {
        InboxMetadata.Builder b = InboxMetadata.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMod(0)
            .setExpirySeconds(60)
            .setLimit(100);
        for (int i = 0; i < topicFiltersCount; i++) {
            b.putTopicFilters("tf" + i, TopicFilterOption.getDefaultInstance());
        }
        return b.build();
    }

    @BeforeMethod
    public void setup() {
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
    }

    @AfterMethod
    public void tearDown() {
        Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
        Metrics.globalRegistry.remove(meterRegistry);
    }

    @Test
    public void testAddAndRemoveSessionAndSubCounts() throws Exception {
        IKVCloseableReader reader = Mockito.mock(IKVCloseableReader.class);
        Mockito.when(reader.boundary()).thenReturn(Boundary.getDefaultInstance());
        Mockito.when(reader.iterator()).thenReturn(new EmptyIterator());
        Mockito.doNothing().when(reader).close();
        TenantsStats stats = new TenantsStats(() -> reader, "clusterId", "c1", "storeId", "s1", "rangeId", "r1");

        String tenant = "tenantA-" + System.nanoTime();
        // async add session & sub
        stats.addSessionCount(tenant, 1);
        // register gauges by promoting to leader
        stats.toggleMetering(true);
        await().atMost(2, TimeUnit.SECONDS)
            .until(() -> findGaugeValue(MqttPersistentSessionNumGauge.metricName, tenant) == 1.0);

        stats.addSubCount(tenant, 3);
        await().atMost(2, TimeUnit.SECONDS)
            .until(() -> findGaugeValue(MqttPersistentSubCountGauge.metricName, tenant) == 3.0);

        // invoke private negative update directly to bypass public assert
        Method decSess = TenantsStats.class.getDeclaredMethod("doAddSessionCount", String.class, int.class);
        decSess.setAccessible(true);
        decSess.invoke(stats, tenant, -1);

        await().atMost(2, TimeUnit.SECONDS)
            .until(() -> findGauge(MqttPersistentSessionNumGauge.metricName, tenant) == null);
        // sub gauge removed together with session removal (destroy)
        assertNull(findGauge(MqttPersistentSubCountGauge.metricName, tenant));
    }

    @Test
    public void testResetScansAndAggregates() {
        // build entries: tenantA(inboxX:3, inboxY:2), tenantB(inboxZ:4)
        String tenantA = "tenantA-" + System.nanoTime();
        String tenantB = "tenantB-" + System.nanoTime();
        List<Entry> entries = new ArrayList<>();
        entries.add(new Entry(
            inboxInstanceStartKey(tenantA, "inboxX", 10),
            meta("inboxX", 10, 3).toByteString()));
        entries.add(new Entry(
            inboxInstanceStartKey(tenantA, "inboxY", 1),
            meta("inboxY", 1, 2).toByteString()));
        entries.add(new Entry(
            inboxInstanceStartKey(tenantB, "inboxZ", 7),
            meta("inboxZ", 7, 4).toByteString()));

        // sort by key lexicographically as store would
        entries.sort(Comparator.comparing(e -> e.key, ByteString.unsignedLexicographicalComparator()));

        IKVCloseableReader reader = new FakeReader(entries);
        TenantsStats stats = new TenantsStats(() -> reader, "clusterId", "c1", "storeId", "s1", "rangeId", "r1");

        stats.reset(Boundary.getDefaultInstance());

        // wait until space gauges appear indicating tenants are loaded
        await().pollDelay(Duration.ofMillis(10)).atMost(2, TimeUnit.SECONDS).until(() ->
            findGauge(MqttPersistentSessionSpaceGauge.metricName, tenantA) != null
                && findGauge(MqttPersistentSessionSpaceGauge.metricName, tenantB) != null);

        // now promote to leader to register session/sub gauges and verify values
        stats.toggleMetering(true);
        await().atMost(2, TimeUnit.SECONDS).until(() ->
            findGaugeValue(MqttPersistentSessionNumGauge.metricName, tenantA) == 2.0
                && findGaugeValue(MqttPersistentSubCountGauge.metricName, tenantA) == 5.0
                && findGaugeValue(MqttPersistentSessionNumGauge.metricName, tenantB) == 1.0
                && findGaugeValue(MqttPersistentSubCountGauge.metricName, tenantB) == 4.0);
    }

    @Test
    public void testCloseUnregistersAllGauges() throws Exception {
        IKVCloseableReader reader = Mockito.mock(IKVCloseableReader.class);
        Mockito.when(reader.boundary()).thenReturn(Boundary.getDefaultInstance());
        Mockito.when(reader.iterator()).thenReturn(new EmptyIterator());
        Mockito.doNothing().when(reader).close();

        TenantsStats stats = new TenantsStats(() -> reader, "clusterId", "c1", "storeId", "s1", "rangeId", "r1");

        String tenant1 = "tenantCloseA-" + System.nanoTime();
        String tenant2 = "tenantCloseB-" + System.nanoTime();

        // create gauges via async updates
        stats.addSessionCount(tenant1, 1);
        stats.addSubCount(tenant1, 2);
        stats.addSessionCount(tenant2, 3);
        stats.addSubCount(tenant2, 4);

        // register gauges by promoting to leader
        stats.toggleMetering(true);
        await().atMost(2, TimeUnit.SECONDS)
            .until(() -> findGauge(MqttPersistentSessionNumGauge.metricName, tenant1) != null
                && findGauge(MqttPersistentSubCountGauge.metricName, tenant1) != null
                && findGauge(MqttPersistentSessionNumGauge.metricName, tenant2) != null
                && findGauge(MqttPersistentSubCountGauge.metricName, tenant2) != null);

        // close should destroy all tenant stats and unregister gauges synchronously
        stats.close();

        assertNull(findGauge(MqttPersistentSessionNumGauge.metricName, tenant1));
        assertNull(findGauge(MqttPersistentSubCountGauge.metricName, tenant1));
        assertNull(findGauge(MqttPersistentSessionSpaceGauge.metricName, tenant1));

        assertNull(findGauge(MqttPersistentSessionNumGauge.metricName, tenant2));
        assertNull(findGauge(MqttPersistentSubCountGauge.metricName, tenant2));
        assertNull(findGauge(MqttPersistentSessionSpaceGauge.metricName, tenant2));
    }

    @Test
    public void testToggleBroadcastToMultipleTenants() {
        IKVCloseableReader reader = Mockito.mock(IKVCloseableReader.class);
        Mockito.when(reader.boundary()).thenReturn(Boundary.getDefaultInstance());
        Mockito.when(reader.iterator()).thenReturn(new EmptyIterator());
        TenantsStats stats = new TenantsStats(() -> reader, "clusterId", "c1", "storeId", "s1", "rangeId", "r1");

        String tenantA = "toggleA-" + System.nanoTime();
        String tenantB = "toggleB-" + System.nanoTime();

        stats.addSessionCount(tenantA, 2);
        stats.addSubCount(tenantA, 3);
        stats.addSessionCount(tenantB, 5);
        stats.addSubCount(tenantB, 7);

        // before promotion, gauges for session/sub should be absent
        await().atMost(2, TimeUnit.SECONDS).until(() ->
            findGauge(MqttPersistentSessionNumGauge.metricName, tenantA) == null
                && findGauge(MqttPersistentSubCountGauge.metricName, tenantA) == null
                && findGauge(MqttPersistentSessionNumGauge.metricName, tenantB) == null
                && findGauge(MqttPersistentSubCountGauge.metricName, tenantB) == null);

        stats.toggleMetering(true);

        await().atMost(2, TimeUnit.SECONDS).until(() ->
            findGaugeValue(MqttPersistentSessionNumGauge.metricName, tenantA) == 2.0
                && findGaugeValue(MqttPersistentSubCountGauge.metricName, tenantA) == 3.0
                && findGaugeValue(MqttPersistentSessionNumGauge.metricName, tenantB) == 5.0
                && findGaugeValue(MqttPersistentSubCountGauge.metricName, tenantB) == 7.0);

        stats.toggleMetering(false);

        await().atMost(2, TimeUnit.SECONDS).until(() ->
            findGauge(MqttPersistentSessionNumGauge.metricName, tenantA) == null
                && findGauge(MqttPersistentSubCountGauge.metricName, tenantA) == null
                && findGauge(MqttPersistentSessionNumGauge.metricName, tenantB) == null
                && findGauge(MqttPersistentSubCountGauge.metricName, tenantB) == null);

        // space gauges should still be present due to creation per-tenant
        assertNotNull(findGauge(MqttPersistentSessionSpaceGauge.metricName, tenantA));
        assertNotNull(findGauge(MqttPersistentSessionSpaceGauge.metricName, tenantB));
    }

    @Test
    public void testDemotionKeepsSpaceGauge() {
        IKVCloseableReader reader = Mockito.mock(IKVCloseableReader.class);
        Mockito.when(reader.boundary()).thenReturn(Boundary.getDefaultInstance());
        Mockito.when(reader.iterator()).thenReturn(new EmptyIterator());
        TenantsStats stats = new TenantsStats(() -> reader, "clusterId", "c1", "storeId", "s1", "rangeId", "r1");

        String tenant = "toggleSpace-" + System.nanoTime();
        stats.addSessionCount(tenant, 1);
        stats.addSubCount(tenant, 1);

        stats.toggleMetering(true);
        await().atMost(2, TimeUnit.SECONDS)
            .until(() -> findGauge(MqttPersistentSessionNumGauge.metricName, tenant) != null
                && findGauge(MqttPersistentSubCountGauge.metricName, tenant) != null);

        stats.toggleMetering(false);
        await().atMost(2, TimeUnit.SECONDS)
            .until(() -> findGauge(MqttPersistentSessionNumGauge.metricName, tenant) == null
                && findGauge(MqttPersistentSubCountGauge.metricName, tenant) == null);

        // space gauge should remain
        assertNotNull(findGauge(MqttPersistentSessionSpaceGauge.metricName, tenant));
    }

    private Double findGaugeValue(String metricName, String tenantId) {
        Meter m = findGauge(metricName, tenantId);
        assertNotNull(m);
        return meterRegistry.find(metricName).tag(ITenantMeter.TAG_TENANT_ID, tenantId).gauge().value();
    }

    private Meter findGauge(String metricName, String tenantId) {
        return meterRegistry.getMeters().stream()
            .filter(m -> m.getId().getName().equals(metricName)
                && m.getId().getType() == Meter.Type.GAUGE
                && tenantId.equals(m.getId().getTag(ITenantMeter.TAG_TENANT_ID)))
            .findFirst().orElse(null);
    }

    private static class Entry {
        final ByteString key;
        final ByteString value;

        Entry(ByteString key, ByteString value) {
            this.key = key;
            this.value = value;
        }
    }

    private static class FakeIterator implements IKVIterator {
        private final List<Entry> entries;
        private final Comparator<ByteString> cmp = ByteString.unsignedLexicographicalComparator();
        private int idx = -1;

        FakeIterator(List<Entry> entries) {
            this.entries = entries;
        }

        @Override
        public ByteString key() {
            return entries.get(idx).key;
        }

        @Override
        public ByteString value() {
            return entries.get(idx).value;
        }

        @Override
        public boolean isValid() {
            return idx >= 0 && idx < entries.size();
        }

        @Override
        public void next() {
            idx++;
        }

        @Override
        public void prev() {
            idx--;
        }

        @Override
        public void seekToFirst() {
            idx = 0;
        }

        @Override
        public void seekToLast() {
            idx = entries.isEmpty() ? 0 : entries.size() - 1;
        }

        @Override
        public void seek(ByteString key) {
            int i = 0;
            for (; i < entries.size(); i++) {
                if (cmp.compare(entries.get(i).key, key) >= 0) {
                    break;
                }
            }
            idx = i;
        }

        @Override
        public void seekForPrev(ByteString key) {
            int i = entries.size() - 1;
            for (; i >= 0; i--) {
                if (cmp.compare(entries.get(i).key, key) <= 0) {
                    break;
                }
            }
            idx = i;
        }
    }

    private static class EmptyIterator extends FakeIterator {
        EmptyIterator() {
            super(List.of());
        }
    }

    private static class FakeReader implements IKVCloseableReader {
        private final List<Entry> entries;

        FakeReader(List<Entry> entries) {
            this.entries = entries;
        }

        @Override
        public Boundary boundary() {
            return Boundary.getDefaultInstance();
        }

        @Override
        public long size(Boundary boundary) {
            return 0;
        }

        @Override
        public boolean exist(ByteString key) {
            return false;
        }

        @Override
        public Optional<ByteString> get(ByteString key) {
            return Optional.empty();
        }

        @Override
        public IKVIterator iterator() {
            return new FakeIterator(entries);
        }

        @Override
        public void refresh() {}

        @Override
        public void close() {}
    }
}
