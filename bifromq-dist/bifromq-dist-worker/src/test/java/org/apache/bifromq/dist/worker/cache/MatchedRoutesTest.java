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

import static org.apache.bifromq.dist.worker.schema.cache.Matchings.normalMatching;
import static org.apache.bifromq.dist.worker.schema.cache.Matchings.receiverUrl;
import static org.apache.bifromq.dist.worker.schema.cache.Matchings.unorderedGroupMatching;
import static org.apache.bifromq.plugin.eventcollector.EventType.GROUP_FANOUT_THROTTLED;
import static org.apache.bifromq.plugin.eventcollector.EventType.PERSISTENT_FANOUT_THROTTLED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Map;
import org.apache.bifromq.dist.worker.schema.cache.GroupMatching;
import org.apache.bifromq.dist.worker.schema.cache.Matching;
import org.apache.bifromq.dist.worker.schema.cache.NormalMatching;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.distservice.GroupFanoutThrottled;
import org.apache.bifromq.plugin.eventcollector.distservice.PersistentFanoutThrottled;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MatchedRoutesTest {
    private static final String TENANT_ID = "tenantA";
    private static final String TOPIC = "sensor/temperature";
    private static final String GROUP_FILTER = "sensor/+";

    private IEventCollector eventCollector;

    @BeforeMethod
    public void setUp() {
        eventCollector = mock(IEventCollector.class);
    }

    @Test
    public void addPersistentNormalMatchingWithinLimit() {
        MatchedRoutes routes = new MatchedRoutes(TENANT_ID, TOPIC, eventCollector, 2, 2);
        NormalMatching persistent = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);

        IMatchedRoutes.AddResult result = routes.addNormalMatching(persistent);

        assertEquals(result, IMatchedRoutes.AddResult.Added);
        assertEquals(routes.persistentFanout(), 1);
        assertEquals(routes.maxPersistentFanout(), 2);
        assertTrue(routes.routes().contains(persistent));
        verify(eventCollector, never()).report(any());
    }

    @Test
    public void addPersistentNormalMatchingExceedLimit() {
        MatchedRoutes routes = new MatchedRoutes(TENANT_ID, TOPIC, eventCollector, 1, 2);
        NormalMatching first = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        NormalMatching second = normalMatching(TENANT_ID, TOPIC, 1, "receiverB", "delivererB", 2);

        assertEquals(routes.addNormalMatching(first), IMatchedRoutes.AddResult.Added);
        IMatchedRoutes.AddResult result = routes.addNormalMatching(second);

        assertEquals(result, IMatchedRoutes.AddResult.ExceedFanoutLimit);
        assertEquals(routes.persistentFanout(), 1);
        assertFalse(routes.routes().contains(second));
        verify(eventCollector, times(1)).report(argThat(e -> {
            if (e.type() != PERSISTENT_FANOUT_THROTTLED) {
                return false;
            }
            PersistentFanoutThrottled event = (PersistentFanoutThrottled) e;
            return event.tenantId().equals(TENANT_ID)
                && event.topic().equals(TOPIC)
                && event.mqttTopicFilter().equals(second.mqttTopicFilter())
                && event.maxCount() == 1;
        }));
    }

    @Test
    public void addNonPersistentNormalMatchingDoesNotAffectPersistentFanout() {
        MatchedRoutes routes = new MatchedRoutes(TENANT_ID, TOPIC, eventCollector, 1, 2);
        NormalMatching nonPersistent = normalMatching(TENANT_ID, TOPIC, 2, "receiverC", "delivererC", 1);

        IMatchedRoutes.AddResult result = routes.addNormalMatching(nonPersistent);

        assertEquals(result, IMatchedRoutes.AddResult.Added);
        assertEquals(routes.persistentFanout(), 0);
        assertTrue(routes.routes().contains(nonPersistent));
    }

    @Test
    public void addDuplicateNormalMatchingReturnsExists() {
        MatchedRoutes routes = new MatchedRoutes(TENANT_ID, TOPIC, eventCollector, 2, 2);
        NormalMatching matching = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);

        assertEquals(routes.addNormalMatching(matching), IMatchedRoutes.AddResult.Added);
        IMatchedRoutes.AddResult second = routes.addNormalMatching(matching);

        assertEquals(second, IMatchedRoutes.AddResult.Exists);
        assertEquals(routes.persistentFanout(), 1);
        assertEquals(routes.routes().stream()
            .filter(m -> m.type() == Matching.Type.Normal && ((NormalMatching) m).subBrokerId() == 1)
            .count(), 1);
    }

    @Test
    public void removePersistentNormalMatching() {
        MatchedRoutes routes = new MatchedRoutes(TENANT_ID, TOPIC, eventCollector, 2, 2);
        NormalMatching persistent = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        routes.addNormalMatching(persistent);

        routes.removeNormalMatching(persistent);

        assertEquals(routes.persistentFanout(), 0);
        assertFalse(routes.routes().contains(persistent));

        // second removal should be a no-op
        routes.removeNormalMatching(persistent);
        assertEquals(routes.persistentFanout(), 0);
    }

    @Test
    public void removeNonPersistentNormalMatching() {
        MatchedRoutes routes = new MatchedRoutes(TENANT_ID, TOPIC, eventCollector, 2, 2);
        NormalMatching nonPersistent = normalMatching(TENANT_ID, TOPIC, 2, "receiverB", "delivererB", 1);
        routes.addNormalMatching(nonPersistent);

        routes.removeNormalMatching(nonPersistent);

        assertEquals(routes.persistentFanout(), 0);
        assertFalse(routes.routes().contains(nonPersistent));
    }

    @Test
    public void putGroupMatchingWithinLimit() {
        MatchedRoutes routes = new MatchedRoutes(TENANT_ID, TOPIC, eventCollector, 2, 2);
        GroupMatching group = unorderedGroupMatching(TENANT_ID, GROUP_FILTER, "groupA",
            Map.of(receiverUrl(1, "receiverA", "delivererA"), 1L));

        IMatchedRoutes.AddResult result = routes.putGroupMatching(group);

        assertEquals(result, IMatchedRoutes.AddResult.Added);
        assertEquals(routes.groupFanout(), 1);
        assertTrue(routes.routes().contains(group));
        verify(eventCollector, never()).report(any());
    }

    @Test
    public void putGroupMatchingExceedLimit() {
        MatchedRoutes routes = new MatchedRoutes(TENANT_ID, TOPIC, eventCollector, 2, 1);
        GroupMatching first = unorderedGroupMatching(TENANT_ID, GROUP_FILTER, "groupA",
            Map.of(receiverUrl(1, "receiverA", "delivererA"), 1L));
        GroupMatching second = unorderedGroupMatching(TENANT_ID, GROUP_FILTER, "groupB",
            Map.of(receiverUrl(1, "receiverB", "delivererB"), 1L));

        assertEquals(routes.putGroupMatching(first), IMatchedRoutes.AddResult.Added);
        IMatchedRoutes.AddResult result = routes.putGroupMatching(second);

        assertEquals(result, IMatchedRoutes.AddResult.ExceedFanoutLimit);
        assertEquals(routes.groupFanout(), 1);
        assertFalse(routes.routes().contains(second));
        verify(eventCollector, times(1)).report(argThat(e -> {
            if (e.type() != GROUP_FANOUT_THROTTLED) {
                return false;
            }
            GroupFanoutThrottled event = (GroupFanoutThrottled) e;
            return event.tenantId().equals(TENANT_ID)
                && event.topic().equals(TOPIC)
                && event.mqttTopicFilter().equals(second.mqttTopicFilter())
                && event.maxCount() == 1;
        }));
    }

    @Test
    public void putGroupMatchingReplacesExistingGroup() {
        MatchedRoutes routes = new MatchedRoutes(TENANT_ID, TOPIC, eventCollector, 2, 3);
        GroupMatching original = unorderedGroupMatching(TENANT_ID, GROUP_FILTER, "groupA",
            Map.of(receiverUrl(1, "receiverA", "delivererA"), 1L));
        GroupMatching replacement = unorderedGroupMatching(TENANT_ID, GROUP_FILTER, "groupA",
            Map.of(receiverUrl(1, "receiverB", "delivererB"), 2L));

        assertEquals(routes.putGroupMatching(original), IMatchedRoutes.AddResult.Added);
        IMatchedRoutes.AddResult result = routes.putGroupMatching(replacement);

        assertEquals(result, IMatchedRoutes.AddResult.Exists);
        assertEquals(routes.groupFanout(), 1);
        assertTrue(routes.routes().contains(replacement));
        assertFalse(routes.routes().contains(original));
    }

    @Test
    public void removeGroupMatching() {
        MatchedRoutes routes = new MatchedRoutes(TENANT_ID, TOPIC, eventCollector, 2, 2);
        GroupMatching group = unorderedGroupMatching(TENANT_ID, GROUP_FILTER, "groupA", Map.of());
        routes.putGroupMatching(group);

        routes.removeGroupMatching(group);

        assertEquals(routes.groupFanout(), 0);
        assertFalse(routes.routes().contains(group));

        routes.removeGroupMatching(group);
        assertEquals(routes.groupFanout(), 0);
    }

    @Test
    public void adjustReturnsReloadNeededWhenPersistentLimitIncreases() {
        MatchedRoutes routes = new MatchedRoutes(TENANT_ID, TOPIC, eventCollector, 1, 2);
        routes.addNormalMatching(normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1));

        IMatchedRoutes.AdjustResult result = routes.adjust(2, routes.maxGroupFanout());

        assertEquals(result, IMatchedRoutes.AdjustResult.ReloadNeeded);
    }

    @Test
    public void adjustReturnsReloadNeededWhenGroupLimitIncreases() {
        MatchedRoutes routes = new MatchedRoutes(TENANT_ID, TOPIC, eventCollector, 2, 1);
        routes.putGroupMatching(unorderedGroupMatching(TENANT_ID, GROUP_FILTER, "groupA",
            Map.of(receiverUrl(1, "receiverA", "delivererA"), 1L)));

        IMatchedRoutes.AdjustResult result = routes.adjust(routes.maxPersistentFanout(), 2);

        assertEquals(result, IMatchedRoutes.AdjustResult.ReloadNeeded);
    }

    @Test
    public void adjustClampsPersistentFanoutWhenLimitDecreases() {
        MatchedRoutes routes = new MatchedRoutes(TENANT_ID, TOPIC, eventCollector, 3, 2);
        NormalMatching first = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        NormalMatching second = normalMatching(TENANT_ID, TOPIC, 1, "receiverB", "delivererB", 2);
        NormalMatching third = normalMatching(TENANT_ID, TOPIC, 1, "receiverC", "delivererC", 3);
        routes.addNormalMatching(first);
        routes.addNormalMatching(second);
        routes.addNormalMatching(third);

        IMatchedRoutes.AdjustResult result = routes.adjust(1, routes.maxGroupFanout());

        assertEquals(result, IMatchedRoutes.AdjustResult.Clamped);
        assertEquals(routes.persistentFanout(), 1);
        long persistentCount = routes.routes().stream()
            .filter(m -> m.type() == Matching.Type.Normal && ((NormalMatching) m).subBrokerId() == 1)
            .count();
        assertEquals(persistentCount, 1);

        IMatchedRoutes.AdjustResult secondResult = routes.adjust(1, routes.maxGroupFanout());
        assertEquals(secondResult, IMatchedRoutes.AdjustResult.Adjusted);
    }

    @Test
    public void adjustClampsGroupFanoutWhenLimitDecreases() {
        MatchedRoutes routes = new MatchedRoutes(TENANT_ID, TOPIC, eventCollector, 2, 3);
        GroupMatching g1 = unorderedGroupMatching(TENANT_ID, GROUP_FILTER, "groupA",
            Map.of(receiverUrl(1, "receiverA", "delivererA"), 1L));
        GroupMatching g2 = unorderedGroupMatching(TENANT_ID, GROUP_FILTER, "groupB",
            Map.of(receiverUrl(1, "receiverB", "delivererB"), 1L));
        GroupMatching g3 = unorderedGroupMatching(TENANT_ID, GROUP_FILTER, "groupC",
            Map.of(receiverUrl(1, "receiverC", "delivererC"), 1L));
        routes.putGroupMatching(g1);
        routes.putGroupMatching(g2);
        routes.putGroupMatching(g3);

        IMatchedRoutes.AdjustResult result = routes.adjust(routes.maxPersistentFanout(), 1);

        assertEquals(result, IMatchedRoutes.AdjustResult.Clamped);
        assertEquals(routes.groupFanout(), 1);
        long groupCount = routes.routes().stream().filter(m -> m.type() == Matching.Type.Group).count();
        assertEquals(groupCount, 1);

        IMatchedRoutes.AdjustResult secondResult = routes.adjust(routes.maxPersistentFanout(), 1);
        assertEquals(secondResult, IMatchedRoutes.AdjustResult.Adjusted);
    }

    @Test
    public void adjustUpdatesLimitsWhenWithinBounds() {
        MatchedRoutes routes = new MatchedRoutes(TENANT_ID, TOPIC, eventCollector, 2, 2);

        IMatchedRoutes.AdjustResult result = routes.adjust(4, 3);

        assertEquals(result, IMatchedRoutes.AdjustResult.Adjusted);
        assertEquals(routes.maxPersistentFanout(), 4);
        assertEquals(routes.maxGroupFanout(), 3);
    }
}
