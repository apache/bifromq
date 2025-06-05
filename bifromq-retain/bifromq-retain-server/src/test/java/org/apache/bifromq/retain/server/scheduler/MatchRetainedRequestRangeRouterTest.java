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

package org.apache.bifromq.retain.server.scheduler;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.inRange;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.split;
import static org.apache.bifromq.retain.server.scheduler.MatchCallRangeRouter.rangeLookup;
import static org.apache.bifromq.retain.store.schema.KVSchemaUtil.retainMessageKey;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MatchRetainedRequestRangeRouterTest {
    private final String tenantId = "testTenant";
    private AutoCloseable closeable;

    @Mock
    private KVRangeSetting rangeSetting1;
    @Mock
    private KVRangeSetting rangeSetting2;

    private NavigableMap<Boundary, KVRangeSetting> effectiveRouter;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        effectiveRouter = new TreeMap<>(BoundaryUtil::compare);
    }

    @SneakyThrows
    @AfterMethod
    public void tearDown() {
        closeable.close();
    }

    @Test
    public void testNonWildcardTopicFilter() {
        // Setup: Non-wildcard topic filter
        String topic = "a/b/c";
        effectiveRouter = createEffectiveRouter(tenantId);

        // Execute
        Map<KVRangeSetting, Set<String>> result = rangeLookup(tenantId, Set.of(topic), effectiveRouter);
        // Verify
        assertEquals(result.size(), 1);
        for (Map.Entry<KVRangeSetting, Set<String>> entry : result.entrySet()) {
            assertEquals(entry.getKey().boundary, FULL_BOUNDARY);
            assertEquals(entry.getValue().size(), 1);
            assertTrue(entry.getValue().contains(topic));
        }
    }

    @Test
    public void testWildcardTopicFilterWithoutMultiWildcard() {
        // Setup: Wildcard topic filter without '#'
        String topicFilter = "a/+/c";
        effectiveRouter = createEffectiveRouter(tenantId, "a/a", "a/b", "a/b/c");

        Map<KVRangeSetting, Set<String>> result = rangeLookup(tenantId, Set.of(topicFilter), effectiveRouter);

        assertTrue(result.keySet()
            .stream()
            .map(setting -> setting.boundary)
            .anyMatch(boundary -> inRange(retainMessageKey(tenantId, "a/a/c"), boundary)
                || inRange(retainMessageKey(tenantId, "a/c/c"), boundary)));
    }

    @Test
    public void testMultiWildcardTopicFilterWithEmptyFilterPrefix() {
        // Setup: Topic filter ending with "#" and empty filter prefix
        String topicFilter = "#";
        effectiveRouter = createEffectiveRouter(tenantId, "a/a", "a/b", "a/b/c");

        Map<KVRangeSetting, Set<String>> result = rangeLookup(tenantId, Set.of(topicFilter), effectiveRouter);

        assertEquals(result.keySet().stream()
            .map(setting -> setting.boundary)
            .collect(Collectors.toSet()), effectiveRouter.keySet());
    }

    @Test
    public void testMultiWildcardTopicFilterWithNonEmptyFilterPrefix() {
        // Setup: Topic filter "a/b/#" with non-empty filter prefix
        String topicFilter = "a/b/#";
        effectiveRouter = createEffectiveRouter(tenantId, "a/a", "a/b", "a/b/c");

        Map<KVRangeSetting, Set<String>> result = rangeLookup(tenantId, Set.of(topicFilter), effectiveRouter);

        // Verify: rangeSetting2 should be filtered out by findCandidates
        assertEquals(result.size(), 2);
        assertTrue(result.keySet()
            .stream()
            .map(setting -> setting.boundary)
            .anyMatch(boundary -> inRange(retainMessageKey(tenantId, "a/b"), boundary)
                || inRange(retainMessageKey(tenantId, "a/b/c"), boundary)));

    }

    private NavigableMap<Boundary, KVRangeSetting> createEffectiveRouter(String tenantId, String... topics) {
        NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);
        router.put(FULL_BOUNDARY, toRangeSetting(FULL_BOUNDARY));
        for (String topic : topics) {
            Iterator<Boundary> itr = router.keySet().iterator();
            ByteString retainKey = retainMessageKey(tenantId, topic);
            while (itr.hasNext()) {
                Boundary boundary = itr.next();
                if (inRange(retainKey, boundary)) {
                    itr.remove();
                    Boundary[] splitBoundaries = split(boundary, retainKey);
                    router.put(splitBoundaries[0], toRangeSetting(splitBoundaries[0]));
                    router.put(splitBoundaries[1], toRangeSetting(splitBoundaries[1]));
                } else {
                    router.put(boundary, toRangeSetting(boundary));
                }
            }
        }
        return router;
    }

    private KVRangeSetting toRangeSetting(Boundary boundary) {
        return new KVRangeSetting("test", "test", KVRangeDescriptor
            .newBuilder()
            .setBoundary(boundary)
            .build());
    }
}