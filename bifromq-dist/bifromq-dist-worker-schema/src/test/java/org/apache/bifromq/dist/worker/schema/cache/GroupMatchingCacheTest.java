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

package org.apache.bifromq.dist.worker.schema.cache;

import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toReceiverUrl;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.bifromq.dist.rpc.proto.RouteGroup;
import org.apache.bifromq.dist.worker.schema.GroupMatching;
import org.apache.bifromq.dist.worker.schema.NormalMatching;
import org.apache.bifromq.dist.worker.schema.RouteDetail;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.util.TopicUtil;
import org.testng.annotations.Test;

public class GroupMatchingCacheTest {

    private static RouteDetail detail(String tenantId, RouteMatcher matcher) {
        return new RouteDetail(tenantId, matcher, null);
    }

    private static RouteGroup groupOf(Map<String, Long> members) {
        RouteGroup.Builder b = RouteGroup.newBuilder();
        for (Map.Entry<String, Long> e : members.entrySet()) {
            b.putMembers(e.getKey(), e.getValue());
        }
        return b.build();
    }

    @Test
    public void sameKeyIntern() {
        String tenantId = "tenant-" + UUID.randomUUID();
        String topic = "$share/group-" + UUID.randomUUID() + "/home/" + UUID.randomUUID();
        RouteMatcher matcher = TopicUtil.from(topic);

        Map<String, Long> members = new HashMap<>();
        members.put(toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d1"), 1L);
        members.put(toReceiverUrl(2, "inbox-" + UUID.randomUUID(), "d2"), 2L);

        RouteDetail d = detail(tenantId, matcher);
        RouteGroup g = groupOf(members);

        GroupMatching m1 = (GroupMatching) GroupMatchingCache.get(d, g);
        GroupMatching m2 = (GroupMatching) GroupMatchingCache.get(d, g);
        assertSame(m1, m2);
    }

    @Test
    public void membershipChangeCreatesNewInstance() {
        String tenantId = "tenant-" + UUID.randomUUID();
        String topic = "$share/group-" + UUID.randomUUID() + "/home/" + UUID.randomUUID();
        RouteMatcher matcher = TopicUtil.from(topic);

        String url1 = toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d1");
        String url2 = toReceiverUrl(2, "inbox-" + UUID.randomUUID(), "d2");

        RouteDetail d = detail(tenantId, matcher);
        RouteGroup g1 = RouteGroup.newBuilder().putMembers(url1, 1L).putMembers(url2, 2L).build();
        RouteGroup g2 = RouteGroup.newBuilder().putMembers(url1, 1L).build(); // remove one member

        GroupMatching m1 = (GroupMatching) GroupMatchingCache.get(d, g1);
        GroupMatching m2 = (GroupMatching) GroupMatchingCache.get(d, g2);
        assertNotSame(m1, m2);
    }

    @Test
    public void orderedFlagPropagates() {
        String tenantId = "tenant-" + UUID.randomUUID();
        RouteMatcher unordered = TopicUtil.from("$share/g" + UUID.randomUUID() + "/a/b");
        RouteMatcher ordered = TopicUtil.from("$oshare/g" + UUID.randomUUID() + "/a/b");
        RouteGroup g = RouteGroup.newBuilder()
            .putMembers(toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d1"), 1L)
            .build();

        GroupMatching mu = (GroupMatching) GroupMatchingCache.get(detail(tenantId, unordered), g);
        GroupMatching mo = (GroupMatching) GroupMatchingCache.get(detail(tenantId, ordered), g);

        // unordered share => ordered=false; ordered share => ordered=true
        assertEquals(mu.ordered, false);
        assertEquals(mo.ordered, true);
    }

    @Test
    public void receiverListConsistency() {
        String tenantId = "tenant-" + UUID.randomUUID();
        RouteMatcher matcher = TopicUtil.from("$share/g" + UUID.randomUUID() + "/a/b");

        String url1 = toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d1");
        String url2 = toReceiverUrl(2, "inbox-" + UUID.randomUUID(), "d2");
        long inc1 = 1L, inc2 = 2L;

        RouteGroup g = RouteGroup.newBuilder().putMembers(url1, inc1).putMembers(url2, inc2).build();
        GroupMatching m = (GroupMatching) GroupMatchingCache.get(detail(tenantId, matcher), g);

        Map<String, Long> expected = g.getMembersMap();
        assertEquals(m.receiverList.size(), expected.size());

        Map<String, Long> actual = new HashMap<>();
        for (NormalMatching n : m.receiverList) {
            actual.put(n.receiverUrl(), n.incarnation());
            // Each sub matching preserves matcher mqttTopicFilter
            assertEquals(n.matchInfo().getMatcher().getMqttTopicFilter(), matcher.getMqttTopicFilter());
        }
        assertEquals(actual, expected);
    }

    @Test
    public void concurrentGetSameKey() throws Exception {
        String tenantId = "tenant-" + UUID.randomUUID();
        RouteMatcher matcher = TopicUtil.from("$share/g" + UUID.randomUUID() + "/a/b");
        RouteGroup g = RouteGroup.newBuilder()
            .putMembers(toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d1"), 1L)
            .putMembers(toReceiverUrl(2, "inbox-" + UUID.randomUUID(), "d2"), 2L)
            .build();
        RouteDetail d = detail(tenantId, matcher);

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<GroupMatching>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return (GroupMatching) GroupMatchingCache.get(d, g);
                }));
            }
            start.countDown();
            GroupMatching first = futures.get(0).get();
            for (Future<GroupMatching> f : futures) {
                assertSame(first, f.get(), "Concurrent gets should return same GroupMatching instance");
            }
        } finally {
            pool.shutdownNow();
        }
    }
}

