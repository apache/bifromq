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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.annotations.Test;

public class ReceiverCacheTest {

    @Test
    public void parseAndCache() {
        int brokerId = (int) (System.nanoTime() & 0x7fffffff);
        String receiverId = "inbox-" + UUID.randomUUID();
        String delivererKey = "deliverer-" + UUID.randomUUID();
        String url = toReceiverUrl(brokerId, receiverId, delivererKey);

        Receiver r1 = ReceiverCache.get(url);
        Receiver r2 = ReceiverCache.get(url);

        assertSame(r1, r2);
        assertEquals(r1.subBrokerId(), brokerId);
        assertEquals(r1.receiverId(), receiverId);
        assertEquals(r1.delivererKey(), delivererKey);
    }

    @Test
    public void differentReceiverNotSame() {
        String url1 = toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d1");
        String url2 = toReceiverUrl(2, "inbox-" + UUID.randomUUID(), "d2");

        Receiver r1 = ReceiverCache.get(url1);
        Receiver r2 = ReceiverCache.get(url2);

        assertNotSame(r1, r2);
    }

    @Test
    public void concurrentGetSameKey() throws Exception {
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            String url = toReceiverUrl(1, "inbox-" + UUID.randomUUID(), "d-" + UUID.randomUUID());
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Receiver>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return ReceiverCache.get(url);
                }));
            }
            start.countDown();
            Receiver first = futures.get(0).get();
            for (Future<Receiver> f : futures) {
                assertSame(first, f.get());
            }
        } finally {
            pool.shutdownNow();
        }
    }
}

