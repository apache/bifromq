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

import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toGroupRouteKey;
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
import org.apache.bifromq.dist.worker.schema.RouteDetail;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.util.TopicUtil;
import org.testng.annotations.Test;

public class RouteDetailCacheTest {

    @Test
    public void parseNormalRouteDetailAndIntern() {
        String tenantId = "tenant-" + UUID.randomUUID();
        String topicFilter = "/home/" + UUID.randomUUID();
        RouteMatcher matcher = TopicUtil.from(topicFilter);
        String receiverUrl = toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d-" + UUID.randomUUID());

        ByteString routeKey = toNormalRouteKey(tenantId, matcher, receiverUrl);
        RouteDetail d1 = RouteDetailCache.get(routeKey);
        RouteDetail d2 = RouteDetailCache.get(routeKey);

        assertSame(d1, d2);
        assertEquals(d1.tenantId(), tenantId);
        assertEquals(d1.receiverUrl(), receiverUrl);
        assertEquals(d1.matcher().getType(), RouteMatcher.Type.Normal);
        assertEquals(d1.matcher().getMqttTopicFilter(), topicFilter);
    }

    @Test
    public void parseUnorderedShareRouteDetail() {
        String tenantId = "tenant-" + UUID.randomUUID();
        String origTopicFilter = "$share/group-" + UUID.randomUUID() + "/s/" + UUID.randomUUID();
        RouteMatcher matcher = TopicUtil.from(origTopicFilter);

        ByteString routeKey = toGroupRouteKey(tenantId, matcher);
        RouteDetail d = RouteDetailCache.get(routeKey);

        assertEquals(d.tenantId(), tenantId);
        assertEquals(d.receiverUrl(), null);
        assertEquals(d.matcher().getType(), RouteMatcher.Type.UnorderedShare);
        assertEquals(d.matcher().getGroup(), matcher.getGroup());
        assertEquals(d.matcher().getMqttTopicFilter(), origTopicFilter);
    }

    @Test
    public void parseOrderedShareRouteDetail() {
        String tenantId = "tenant-" + UUID.randomUUID();
        String origTopicFilter = "$oshare/group-" + UUID.randomUUID() + "/s/" + UUID.randomUUID();
        RouteMatcher matcher = TopicUtil.from(origTopicFilter);

        ByteString routeKey = toGroupRouteKey(tenantId, matcher);
        RouteDetail d = RouteDetailCache.get(routeKey);

        assertEquals(d.tenantId(), tenantId);
        assertEquals(d.receiverUrl(), null);
        assertEquals(d.matcher().getType(), RouteMatcher.Type.OrderedShare);
        assertEquals(d.matcher().getGroup(), matcher.getGroup());
        assertEquals(d.matcher().getMqttTopicFilter(), origTopicFilter);
    }

    @Test
    public void equalContentDifferentByteStringInstancesInterned() {
        String tenantId = "tenant-" + UUID.randomUUID();
        String topicFilter = "/room/" + UUID.randomUUID();
        RouteMatcher matcher = TopicUtil.from(topicFilter);
        String receiverUrl = toReceiverUrl(2, "inbox-" + UUID.randomUUID(), "d-" + UUID.randomUUID());

        ByteString k1 = toNormalRouteKey(tenantId, matcher, receiverUrl);
        ByteString k2 = ByteString.copyFrom(k1.toByteArray());

        RouteDetail d1 = RouteDetailCache.get(k1);
        RouteDetail d2 = RouteDetailCache.get(k2);
        assertSame(d1, d2);
    }

    @Test
    public void differentKeysProduceDifferentDetails() {
        String tenantId1 = "tenant-" + UUID.randomUUID();
        String tenantId2 = "tenant-" + UUID.randomUUID();
        String topicFilter = "/a/" + UUID.randomUUID();
        RouteMatcher matcher = TopicUtil.from(topicFilter);
        String receiverUrl = toReceiverUrl(3, "inbox-" + UUID.randomUUID(), "d-" + UUID.randomUUID());

        ByteString k1 = toNormalRouteKey(tenantId1, matcher, receiverUrl);
        ByteString k2 = toNormalRouteKey(tenantId2, matcher, receiverUrl);

        RouteDetail d1 = RouteDetailCache.get(k1);
        RouteDetail d2 = RouteDetailCache.get(k2);
        assertNotSame(d1, d2);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void unsupportedFlagThrows() {
        String tenantId = "tenant-" + UUID.randomUUID();
        String topicFilter = "/x/" + UUID.randomUUID();
        RouteMatcher matcher = TopicUtil.from(topicFilter);
        String receiverUrl = toReceiverUrl(9, "inbox-" + UUID.randomUUID(), "d-" + UUID.randomUUID());

        ByteString normalKey = toNormalRouteKey(tenantId, matcher, receiverUrl);
        // Rewrite flag byte to an unsupported value (e.g., 0x7F)
        short receiverBytesLen = normalKey.substring(normalKey.size() - Short.BYTES).asReadOnlyByteBuffer().getShort();
        int receiverBytesStartIdx = normalKey.size() - Short.BYTES - receiverBytesLen;
        int flagByteIdx = receiverBytesStartIdx - 1;
        ByteString prefix = normalKey.substring(0, flagByteIdx);
        ByteString invalidFlag = ByteString.copyFrom(new byte[] {(byte) 0x7F});
        ByteString suffix = normalKey.substring(flagByteIdx + 1);
        ByteString invalidKey = prefix.concat(invalidFlag).concat(suffix);

        // Should throw UnsupportedOperationException
        RouteDetailCache.get(invalidKey);
    }

    @Test
    public void concurrentGetSameKey() throws Exception {
        String tenantId = "tenant-" + UUID.randomUUID();
        String topicFilter = "/home/" + UUID.randomUUID();
        RouteMatcher matcher = TopicUtil.from(topicFilter);
        String receiverUrl = toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d-" + UUID.randomUUID());
        ByteString k = toNormalRouteKey(tenantId, matcher, receiverUrl);

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<RouteDetail>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return RouteDetailCache.get(k);
                }));
            }
            start.countDown();
            RouteDetail first = futures.get(0).get();
            for (Future<RouteDetail> f : futures) {
                assertSame(first, f.get());
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
