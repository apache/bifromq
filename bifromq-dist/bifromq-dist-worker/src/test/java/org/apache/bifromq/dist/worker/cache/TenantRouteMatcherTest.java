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

package org.apache.bifromq.dist.worker.cache;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toGroupRouteKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toNormalRouteKey;
import static org.apache.bifromq.dist.worker.schema.cache.Matchings.normalMatching;
import static org.apache.bifromq.dist.worker.schema.cache.Matchings.receiverUrl;
import static org.apache.bifromq.dist.worker.schema.cache.Matchings.unorderedGroupMatching;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVReader;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.dist.rpc.proto.RouteGroup;
import org.apache.bifromq.dist.worker.schema.cache.GroupMatching;
import org.apache.bifromq.dist.worker.schema.cache.Matching;
import org.apache.bifromq.dist.worker.schema.cache.NormalMatching;
import org.apache.bifromq.plugin.eventcollector.Event;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.distservice.GroupFanoutThrottled;
import org.apache.bifromq.plugin.eventcollector.distservice.PersistentFanoutThrottled;
import org.apache.bifromq.util.BSUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TenantRouteMatcherTest {
    private static final String TENANT_ID = "tenantA";
    private static final String OTHER_TENANT = "tenantB";

    private SimpleMeterRegistry meterRegistry;
    private RecordingEventCollector eventCollector;
    private Timer matchTimer;

    private static NavigableMap<ByteString, ByteString> newTreeMap() {
        return new TreeMap<>(ByteString.unsignedLexicographicalComparator());
    }

    @BeforeMethod
    public void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        eventCollector = new RecordingEventCollector();
        matchTimer = meterRegistry.timer("tenantRouteMatch");
    }

    @AfterMethod
    public void tearDown() {
        meterRegistry.close();
    }

    @Test
    public void matchAllReturnsEmptyWhenNoTenantDataPresent() {
        NavigableMap<ByteString, ByteString> kvData = newTreeMap();
        NormalMatching otherTenantRoute = normalMatching(OTHER_TENANT, "sensors/+/temp", 1, "receiverX", "delivererX",
            1);
        kvData.put(toNormalRouteKey(OTHER_TENANT, otherTenantRoute.matcher, otherTenantRoute.receiverUrl()),
            BSUtil.toByteString(otherTenantRoute.incarnation()));

        Set<String> topics = Set.of("sensors/device1/temp", "sensors/device1/humidity");
        TenantRouteMatcher matcher =
            new TenantRouteMatcher(TENANT_ID, () -> new TreeMapKVReader(kvData), eventCollector, matchTimer);

        Map<String, IMatchedRoutes> matchedRoutes = matcher.matchAll(topics, 10, 10);

        assertEquals(matchedRoutes.keySet(), topics);
        matchedRoutes.values().forEach(routes -> {
            assertTrue(routes.routes().isEmpty());
            assertEquals(routes.persistentFanout(), 0);
            assertEquals(routes.groupFanout(), 0);
        });
        assertTrue(eventCollector.events().isEmpty());
    }

    @Test
    public void matchAllAcrossMultipleTopics() {
        NavigableMap<ByteString, ByteString> kvData = newTreeMap();

        NormalMatching tempRoute = normalMatching(TENANT_ID, "sensors/+/temp", 1, "receiverA", "delivererA", 1);
        kvData.put(toNormalRouteKey(TENANT_ID, tempRoute.matcher, tempRoute.receiverUrl()),
            BSUtil.toByteString(tempRoute.incarnation()));

        NormalMatching humidityRoute =
            normalMatching(TENANT_ID, "sensors/+/humidity", 1, "receiverB", "delivererB", 2);
        kvData.put(
            toNormalRouteKey(TENANT_ID, humidityRoute.matcher, humidityRoute.receiverUrl()),
            BSUtil.toByteString(humidityRoute.incarnation()));

        TenantRouteMatcher matcher =
            new TenantRouteMatcher(TENANT_ID, () -> new TreeMapKVReader(kvData), eventCollector, matchTimer);

        Set<String> topics = Set.of(
            "sensors/device1/temp",
            "sensors/device1/humidity",
            "sensors/device2/temp");

        Map<String, IMatchedRoutes> matchedRoutes = matcher.matchAll(topics, 10, 10);

        assertEquals(matchedRoutes.keySet(), topics);
        assertTrue(matchedRoutes.get("sensors/device1/temp").routes().contains(tempRoute));
        assertTrue(matchedRoutes.get("sensors/device2/temp").routes().contains(tempRoute));
        assertTrue(matchedRoutes.get("sensors/device1/humidity").routes().contains(humidityRoute));

        assertEquals(matchedRoutes.get("sensors/device1/temp").persistentFanout(), 1);
        assertEquals(matchedRoutes.get("sensors/device2/temp").persistentFanout(), 1);
        assertEquals(matchedRoutes.get("sensors/device1/humidity").persistentFanout(), 1);
        matchedRoutes.values().forEach(routes -> assertEquals(routes.groupFanout(), 0));
        assertTrue(eventCollector.events().isEmpty());
    }

    @Test
    public void reuseCachedFilterMatchesForRepeatedSubscriptions() {
        NavigableMap<ByteString, ByteString> kvData = newTreeMap();

        NormalMatching firstRoute = normalMatching(TENANT_ID, "devices/+/status", 1, "receiverA", "delivererA", 1);
        NormalMatching secondRoute = normalMatching(TENANT_ID, "devices/+/status", 2, "receiverB", "delivererB", 1);

        kvData.put(toNormalRouteKey(TENANT_ID, firstRoute.matcher, firstRoute.receiverUrl()),
            BSUtil.toByteString(firstRoute.incarnation()));
        kvData.put(toNormalRouteKey(TENANT_ID, secondRoute.matcher, secondRoute.receiverUrl()),
            BSUtil.toByteString(secondRoute.incarnation()));

        TenantRouteMatcher matcher =
            new TenantRouteMatcher(TENANT_ID, () -> new TreeMapKVReader(kvData), eventCollector, matchTimer);

        Set<String> topics = Set.of("devices/a/status", "devices/b/status");

        Map<String, IMatchedRoutes> matchedRoutes = matcher.matchAll(topics, 5, 5);

        topics.forEach(topic -> {
            IMatchedRoutes routes = matchedRoutes.get(topic);
            assertTrue(routes.routes().contains(firstRoute));
            assertTrue(routes.routes().contains(secondRoute));
            assertEquals(routes.persistentFanout(), 1); // only first route counts as persistent (subBrokerId == 1)
            assertEquals(routes.groupFanout(), 0);
        });
        assertTrue(eventCollector.events().isEmpty());
    }

    @Test
    public void matchAllWithSharedSubscription() {
        NavigableMap<ByteString, ByteString> kvData = newTreeMap();

        GroupMatching groupMatching = unorderedGroupMatching(
            TENANT_ID,
            "alerts/+/+/temperature",
            "groupAlpha",
            Map.of(receiverUrl(1, "receiverA", "delivererA"), 10L,
                receiverUrl(2, "receiverB", "delivererB"), 11L));

        kvData.put(toGroupRouteKey(TENANT_ID, groupMatching.matcher),
            RouteGroup.newBuilder().putAllMembers(groupMatching.receivers()).build().toByteString());

        TenantRouteMatcher matcher =
            new TenantRouteMatcher(TENANT_ID, () -> new TreeMapKVReader(kvData), eventCollector, matchTimer);

        Set<String> topics = Set.of("alerts/site1/device1/temperature", "alerts/site1/device2/temperature");

        Map<String, IMatchedRoutes> matchedRoutes = matcher.matchAll(topics, 10, 10);

        topics.forEach(topic -> {
            IMatchedRoutes routes = matchedRoutes.get(topic);
            assertTrue(routes.routes().contains(groupMatching));
            assertEquals(routes.persistentFanout(), 0);
            assertEquals(routes.groupFanout(), 1);
        });
        assertTrue(eventCollector.events().isEmpty());
    }

    @Test
    public void skipNonMatchingRoutesAndFallbackToSeek() {
        NavigableMap<ByteString, ByteString> kvData = newTreeMap();
        for (int i = 0; i < 21; i++) {
            NormalMatching noise = normalMatching(TENANT_ID, "invalid/" + i, 1, "noise" + i, "deliverer" + i, i);
            kvData.put(toNormalRouteKey(TENANT_ID, noise.matcher, noise.receiverUrl()),
                BSUtil.toByteString(noise.incarnation()));
        }

        NormalMatching valid = normalMatching(TENANT_ID, "metrics/+/cpu", 1, "receiverA", "delivererA", 1);
        kvData.put(toNormalRouteKey(TENANT_ID, valid.matcher, valid.receiverUrl()),
            BSUtil.toByteString(valid.incarnation()));

        TreeMapKVReader kvReader = new TreeMapKVReader(kvData);
        TenantRouteMatcher matcher = new TenantRouteMatcher(TENANT_ID, () -> kvReader, eventCollector, matchTimer);

        Set<String> topics = Set.of("metrics/server1/cpu");

        Map<String, IMatchedRoutes> matchedRoutes = matcher.matchAll(topics, 10, 10);

        IMatchedRoutes result = matchedRoutes.get("metrics/server1/cpu");
        assertTrue(result.routes().contains(valid));
        assertEquals(result.persistentFanout(), 1);
        assertEquals(result.groupFanout(), 0);

        TreeMapKVIterator iterator = kvReader.lastIterator();
        assertNotNull(iterator);
        assertTrue(iterator.getSeekCount() >= 2); // initial seek + fallback seek
        assertTrue(iterator.getNextCount() >= 21); // probed through noise entries
        assertTrue(eventCollector.events().isEmpty());
    }

    @Test
    public void isolateRoutesByTenant() {
        NavigableMap<ByteString, ByteString> kvData = newTreeMap();

        NormalMatching tenantRoute = normalMatching(TENANT_ID, "devices/+/signal", 1, "receiverA", "delivererA", 1);
        kvData.put(toNormalRouteKey(TENANT_ID, tenantRoute.matcher, tenantRoute.receiverUrl()),
            BSUtil.toByteString(tenantRoute.incarnation()));

        NormalMatching otherTenantRoute =
            normalMatching(OTHER_TENANT, "devices/+/signal", 1, "receiverB", "delivererB", 1);
        kvData.put(toNormalRouteKey(OTHER_TENANT, otherTenantRoute.matcher, otherTenantRoute.receiverUrl()),
            BSUtil.toByteString(otherTenantRoute.incarnation()));

        Supplier<IKVReader> readerSupplier = () -> new TreeMapKVReader(kvData);

        TenantRouteMatcher matcherTenant =
            new TenantRouteMatcher(TENANT_ID, readerSupplier, eventCollector, matchTimer);
        TenantRouteMatcher matcherOther =
            new TenantRouteMatcher(OTHER_TENANT, readerSupplier, eventCollector, matchTimer);

        Set<String> topics = Set.of("devices/a/signal");

        Map<String, IMatchedRoutes> tenantMatches = matcherTenant.matchAll(topics, 10, 10);
        assertTrue(tenantMatches.get("devices/a/signal").routes().contains(tenantRoute));
        assertFalse(tenantMatches.get("devices/a/signal").routes().contains(otherTenantRoute));

        Map<String, IMatchedRoutes> otherMatches = matcherOther.matchAll(topics, 10, 10);
        assertTrue(otherMatches.get("devices/a/signal").routes().contains(otherTenantRoute));
        assertFalse(otherMatches.get("devices/a/signal").routes().contains(tenantRoute));

        assertTrue(eventCollector.events().isEmpty());
    }

    @Test
    public void triggerPersistentFanoutThrottling() {
        NavigableMap<ByteString, ByteString> kvData = newTreeMap();

        NormalMatching first = normalMatching(TENANT_ID, "alarms/+/critical", 1, "receiverA", "delivererA", 1);
        NormalMatching second = normalMatching(TENANT_ID, "alarms/+/critical", 1, "receiverB", "delivererB", 2);

        kvData.put(toNormalRouteKey(TENANT_ID, first.matcher, first.receiverUrl()),
            BSUtil.toByteString(first.incarnation()));
        kvData.put(toNormalRouteKey(TENANT_ID, second.matcher, second.receiverUrl()),
            BSUtil.toByteString(second.incarnation()));

        TenantRouteMatcher matcher =
            new TenantRouteMatcher(TENANT_ID, () -> new TreeMapKVReader(kvData), eventCollector, matchTimer);

        Map<String, IMatchedRoutes> matchedRoutes =
            matcher.matchAll(Set.of("alarms/device1/critical"), 1, 10);

        IMatchedRoutes routes = matchedRoutes.get("alarms/device1/critical");
        assertEquals(routes.persistentFanout(), 1);
        assertEquals(routes.groupFanout(), 0);
        assertEquals(routes.routes().size(), 1);

        List<PersistentFanoutThrottled> events = eventCollector.eventsOfType(PersistentFanoutThrottled.class);
        assertEquals(events.size(), 1);
        PersistentFanoutThrottled event = events.get(0);
        assertEquals(event.tenantId(), TENANT_ID);
        assertEquals(event.topic(), "alarms/device1/critical");
        assertEquals(event.mqttTopicFilter(), second.mqttTopicFilter());
        assertEquals(event.maxCount(), 1);
    }

    @Test
    public void triggerGroupFanoutThrottling() {
        NavigableMap<ByteString, ByteString> kvData = newTreeMap();

        GroupMatching first = unorderedGroupMatching(
            TENANT_ID,
            "jobs/+/progress",
            "groupA",
            Map.of(receiverUrl(1, "receiverA", "delivererA"), 1L));
        GroupMatching second = unorderedGroupMatching(
            TENANT_ID,
            "jobs/+/progress",
            "groupB",
            Map.of(receiverUrl(1, "receiverB", "delivererB"), 1L));

        kvData.put(toGroupRouteKey(TENANT_ID, first.matcher),
            RouteGroup.newBuilder().putAllMembers(first.receivers()).build().toByteString());
        kvData.put(toGroupRouteKey(TENANT_ID, second.matcher),
            RouteGroup.newBuilder().putAllMembers(second.receivers()).build().toByteString());

        TenantRouteMatcher matcher =
            new TenantRouteMatcher(TENANT_ID, () -> new TreeMapKVReader(kvData), eventCollector, matchTimer);

        Map<String, IMatchedRoutes> matchedRoutes =
            matcher.matchAll(Set.of("jobs/job1/progress"), 10, 1);

        IMatchedRoutes routes = matchedRoutes.get("jobs/job1/progress");
        assertEquals(routes.groupFanout(), 1);
        assertEquals(routes.routes().stream().filter(m -> m.type() == Matching.Type.Group).count(), 1);

        List<GroupFanoutThrottled> events = eventCollector.eventsOfType(GroupFanoutThrottled.class);
        assertEquals(events.size(), 1);
        GroupFanoutThrottled event = events.get(0);
        assertEquals(event.tenantId(), TENANT_ID);
        assertEquals(event.topic(), "jobs/job1/progress");
        // second comes before first in lexicographical order by bucketing key
        assertEquals(event.mqttTopicFilter(), first.mqttTopicFilter());
        assertEquals(event.maxCount(), 1);
    }

    private static final class TreeMapKVReader implements IKVReader {
        private final NavigableMap<ByteString, ByteString> data;
        private TreeMapKVIterator lastIterator;

        private TreeMapKVReader(NavigableMap<ByteString, ByteString> data) {
            this.data = data;
        }

        @Override
        public Boundary boundary() {
            if (data.isEmpty()) {
                return FULL_BOUNDARY;
            }
            ByteString start = data.firstKey();
            ByteString end = upperBound(data.lastKey());
            return toBoundary(start, end);
        }

        @Override
        public long size(Boundary boundary) {
            if (data.isEmpty()) {
                return 0;
            }
            ByteString start = BoundaryUtil.startKey(boundary);
            ByteString end = BoundaryUtil.endKey(boundary);
            NavigableMap<ByteString, ByteString> sub = data;
            if (start != null) {
                sub = sub.tailMap(start, true);
            }
            if (end != null) {
                sub = sub.headMap(end, false);
            }
            return sub.size();
        }

        @Override
        public boolean exist(ByteString key) {
            return data.containsKey(key);
        }

        @Override
        public java.util.Optional<ByteString> get(ByteString key) {
            return java.util.Optional.ofNullable(data.get(key));
        }

        @Override
        public IKVIterator iterator() {
            lastIterator = new TreeMapKVIterator(data);
            return lastIterator;
        }

        @Override
        public void refresh() {
            // no-op for in-memory stub
        }

        TreeMapKVIterator lastIterator() {
            return lastIterator;
        }
    }

    private static final class TreeMapKVIterator implements IKVIterator {
        private final NavigableMap<ByteString, ByteString> data;
        private Map.Entry<ByteString, ByteString> current;
        private int seekCount;
        private int nextCount;

        private TreeMapKVIterator(NavigableMap<ByteString, ByteString> data) {
            this.data = data;
        }

        @Override
        public ByteString key() {
            if (current == null) {
                throw new IllegalStateException("Iterator is not valid");
            }
            return current.getKey();
        }

        @Override
        public ByteString value() {
            if (current == null) {
                throw new IllegalStateException("Iterator is not valid");
            }
            return current.getValue();
        }

        @Override
        public boolean isValid() {
            return current != null;
        }

        @Override
        public void next() {
            if (current == null) {
                throw new IllegalStateException("Iterator is not valid");
            }
            current = data.higherEntry(current.getKey());
            nextCount++;
        }

        @Override
        public void prev() {
            if (current == null) {
                throw new IllegalStateException("Iterator is not valid");
            }
            current = data.lowerEntry(current.getKey());
            nextCount++;
        }

        @Override
        public void seekToFirst() {
            current = data.firstEntry();
            seekCount++;
        }

        @Override
        public void seekToLast() {
            current = data.lastEntry();
            seekCount++;
        }

        @Override
        public void seek(ByteString key) {
            current = key == null ? data.firstEntry() : data.ceilingEntry(key);
            seekCount++;
        }

        @Override
        public void seekForPrev(ByteString key) {
            current = key == null ? data.lastEntry() : data.floorEntry(key);
            seekCount++;
        }

        int getSeekCount() {
            return seekCount;
        }

        int getNextCount() {
            return nextCount;
        }
    }

    private static final class RecordingEventCollector implements IEventCollector {
        private final List<Event<?>> events = new CopyOnWriteArrayList<>();

        @Override
        public void report(Event<?> event) {
            events.add((Event<?>) event.clone());
        }

        List<Event<?>> events() {
            return new ArrayList<>(events);
        }

        <T extends Event<?>> List<T> eventsOfType(Class<T> type) {
            List<T> filtered = new ArrayList<>();
            for (Event<?> event : events) {
                if (type.isInstance(event)) {
                    filtered.add(type.cast(event));
                }
            }
            return filtered;
        }
    }
}
