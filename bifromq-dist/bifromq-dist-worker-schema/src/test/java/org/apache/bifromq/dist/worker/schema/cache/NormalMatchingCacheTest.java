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

import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toNormalRouteKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toReceiverUrl;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.util.TopicUtil;
import org.testng.annotations.Test;

public class NormalMatchingCacheTest {

    private static RouteDetail detail(String tenantId, String topicFilter, String receiverUrl) {
        RouteMatcher matcher = TopicUtil.from(topicFilter);
        return new RouteDetail(tenantId, matcher, receiverUrl);
    }

    @Test
    public void reuseMatcherFromRouteDetailCache() {
        String tenantId = "tenant-" + UUID.randomUUID();
        String topicFilter = "/cache/" + UUID.randomUUID();
        String receiverUrl = toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d-" + UUID.randomUUID());
        RouteMatcher matcher = TopicUtil.from(topicFilter);
        ByteString routeKey = toNormalRouteKey(tenantId, matcher, receiverUrl);

        RouteDetail fromCache = RouteDetailCache.get(routeKey);
        NormalMatching m1 = (NormalMatching) NormalMatchingCache.get(fromCache, 9L);
        NormalMatching m2 = (NormalMatching) NormalMatchingCache.get(RouteDetailCache.get(routeKey), 9L);

        assertSame(fromCache, RouteDetailCache.get(routeKey));
        assertSame(m1, m2);
    }

    @Test
    public void sameKeySameIncarnationIntern() {
        String tenantId = "tenant-" + UUID.randomUUID();
        String topicFilter = "/s/" + UUID.randomUUID();
        String receiverUrl = toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d-" + UUID.randomUUID());
        RouteDetail d = detail(tenantId, topicFilter, receiverUrl);

        NormalMatching m1 = (NormalMatching) NormalMatchingCache.get(d, 1L);
        NormalMatching m2 = (NormalMatching) NormalMatchingCache.get(d, 1L);
        assertSame(m1, m2);
    }

    @Test
    public void differentIncarnationNotSame() {
        String tenantId = "tenant-" + UUID.randomUUID();
        String topicFilter = "/s/" + UUID.randomUUID();
        String receiverUrl = toReceiverUrl(2, "inbox-" + UUID.randomUUID(), "d-" + UUID.randomUUID());
        RouteDetail d = detail(tenantId, topicFilter, receiverUrl);

        NormalMatching m1 = (NormalMatching) NormalMatchingCache.get(d, 1L);
        NormalMatching m2 = (NormalMatching) NormalMatchingCache.get(d, 2L);
        assertNotSame(m1, m2);
    }

    @Test
    public void differentReceiverNotSame() {
        String tenantId = "tenant-" + UUID.randomUUID();
        String topicFilter = "/s/" + UUID.randomUUID();
        String receiverUrl1 = toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d1");
        String receiverUrl2 = toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d1");
        NormalMatching m1 = (NormalMatching) NormalMatchingCache.get(detail(tenantId, topicFilter, receiverUrl1), 1L);
        NormalMatching m2 = (NormalMatching) NormalMatchingCache.get(detail(tenantId, topicFilter, receiverUrl2), 1L);
        assertNotSame(m1, m2);
    }

    @Test
    public void differentMatcherNotSame() {
        String tenantId = "tenant-" + UUID.randomUUID();
        String receiverUrl = toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d1");
        NormalMatching m1 = (NormalMatching) NormalMatchingCache.get(detail(tenantId, "/a/#", receiverUrl), 1L);
        NormalMatching m2 = (NormalMatching) NormalMatchingCache.get(detail(tenantId, "/a/+", receiverUrl), 1L);
        assertNotSame(m1, m2);
    }

    @Test
    public void fieldCorrectness() {
        String tenantId = "tenant-" + UUID.randomUUID();
        String topicFilter = "/s/" + UUID.randomUUID();
        int subBrokerId = 7;
        String receiverId = "inbox-" + UUID.randomUUID();
        String deliverer = "d-" + UUID.randomUUID();
        String receiverUrl = toReceiverUrl(subBrokerId, receiverId, deliverer);
        NormalMatching m = (NormalMatching) NormalMatchingCache.get(detail(tenantId, topicFilter, receiverUrl), 42L);

        assertEquals(m.receiverUrl(), receiverUrl);
        assertEquals(m.subBrokerId(), subBrokerId);
        assertEquals(m.delivererKey(), deliverer);
        MatchInfo info = m.matchInfo();
        assertEquals(info.getReceiverId(), receiverId);
        assertEquals(info.getIncarnation(), 42L);
        assertEquals(info.getMatcher().getMqttTopicFilter(), topicFilter);
    }

    @Test
    public void concurrentGetSameKey() throws Exception {
        String tenantId = "tenant-" + UUID.randomUUID();
        String topicFilter = "/s/" + UUID.randomUUID();
        String receiverUrl = toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d-" + UUID.randomUUID());
        RouteDetail d = detail(tenantId, topicFilter, receiverUrl);

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<NormalMatching>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return (NormalMatching) NormalMatchingCache.get(d, 1L);
                }));
            }
            start.countDown();
            NormalMatching first = futures.get(0).get();
            for (Future<NormalMatching> f : futures) {
                assertSame(first, f.get());
            }
        } finally {
            pool.shutdownNow();
        }
    }
}

