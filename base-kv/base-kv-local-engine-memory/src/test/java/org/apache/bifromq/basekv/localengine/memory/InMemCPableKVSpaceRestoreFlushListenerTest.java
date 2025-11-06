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

package org.apache.bifromq.basekv.localengine.memory;

import static org.testng.Assert.assertEquals;

import com.google.protobuf.ByteString;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVEngine;
import org.apache.bifromq.basekv.localengine.IRestoreSession;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InMemCPableKVSpaceRestoreFlushListenerTest {
    private IKVEngine<? extends ICPableKVSpace> engine;

    @BeforeMethod
    public void setup() {
        engine = new InMemCPableKVEngine(null, InMemDefaultConfigs.CP);
        engine.start();
    }

    @AfterMethod
    public void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    @Test
    public void testDoneReportsSingleAggregatedFlush() {
        String spaceId = "inmem_flush";
        ICPableKVSpace space = engine.createIfMissing(spaceId);

        AtomicInteger callbackCount = new AtomicInteger();
        AtomicLong totalEntries = new AtomicLong();
        AtomicLong totalBytes = new AtomicLong();
        IRestoreSession session = space.startRestore((c, b) -> {
            callbackCount.incrementAndGet();
            totalEntries.addAndGet(c);
            totalBytes.addAndGet(b);
        });

        int n = 1000;
        long expectBytes = 0;
        for (int i = 0; i < n; i++) {
            ByteString k = ByteString.copyFromUtf8("k" + i);
            ByteString v = ByteString.copyFromUtf8("v" + i);
            expectBytes += k.size() + v.size();
            session.put(k, v);
        }
        session.done();

        assertEquals(callbackCount.get(), 1);
        assertEquals(totalEntries.get(), n);
        assertEquals(totalBytes.get(), expectBytes);
    }
}
