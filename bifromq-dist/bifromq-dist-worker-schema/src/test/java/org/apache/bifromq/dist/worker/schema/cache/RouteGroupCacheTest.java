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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.bifromq.dist.rpc.proto.RouteGroup;
import org.testng.annotations.Test;

public class RouteGroupCacheTest {

    @Test
    public void sameByteStringInstanceIsInterned() {
        RouteGroup group = RouteGroup.newBuilder()
            .putMembers(toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d1"), 1L)
            .putMembers(toReceiverUrl(2, "inbox-" + UUID.randomUUID(), "d2"), 2L)
            .build();
        ByteString bytes = group.toByteString();

        RouteGroup g1 = RouteGroupCache.get(bytes);
        RouteGroup g2 = RouteGroupCache.get(bytes);

        assertSame(g1, g2);
    }

    @Test
    public void equalContentDifferentByteString() {
        RouteGroup group = RouteGroup.newBuilder()
            .putMembers(toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d1"), 1L)
            .build();
        ByteString bs1 = group.toByteString();
        ByteString bs2 = ByteString.copyFrom(bs1.toByteArray());

        RouteGroup g1 = RouteGroupCache.get(bs1);
        RouteGroup g2 = RouteGroupCache.get(bs2);

        // Content equality must hold
        assertEquals(g1, g2);
        // Implementation may or may not intern across different ByteString instances; avoid over-constraining
        assertSame(g1, g2);
    }

    @Test
    public void differentContent() {
        ByteString bs1 = RouteGroup.newBuilder()
            .putMembers(toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d1"), 1L)
            .build().toByteString();
        ByteString bs2 = RouteGroup.newBuilder()
            .putMembers(toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d1"), 2L) // different incarnation
            .build().toByteString();

        RouteGroup g1 = RouteGroupCache.get(bs1);
        RouteGroup g2 = RouteGroupCache.get(bs2);

        assertNotEquals(g1, g2);
    }

    @Test
    public void invalidBytes() {
        ByteString invalid = ByteString.copyFromUtf8("not-a-route-group");
        assertThrows(RuntimeException.class, () -> RouteGroupCache.get(invalid));
    }

    @Test
    public void concurrentGetSameKey() throws Exception {
        RouteGroup group = RouteGroup.newBuilder()
            .putMembers(toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d1"), 1L)
            .putMembers(toReceiverUrl(2, "inbox-" + UUID.randomUUID(), "d2"), 2L)
            .build();
        ByteString bytes = group.toByteString();

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<RouteGroup>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return RouteGroupCache.get(bytes);
                }));
            }
            start.countDown();
            RouteGroup first = futures.get(0).get();
            for (Future<RouteGroup> f : futures) {
                assertSame(first, f.get());
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
