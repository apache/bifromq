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

package org.apache.bifromq.mqtt.handler;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

public class AdaptiveReceiveQuotaTest {
    @Test
    public void shouldStabilizeWindowWhenAckEqualsRtt() {
        AdaptiveReceiveQuota quota = new AdaptiveReceiveQuota(
            100,
            TimeUnit.MILLISECONDS.toNanos(5),
            0.5,
            1.25,
            4.0,
            20.0,
            100);

        long ackInterval = TimeUnit.MILLISECONDS.toNanos(5);
        long now = 0;
        for (int i = 0; i < 20; i++) {
            long sendTime = now;
            now += ackInterval;
            quota.onPacketAcked(now, sendTime);
        }
        int expectedWindow = (int) Math.ceil(1.25);
        assertEquals(quota.window(), expectedWindow);
        assertTrue(quota.window() <= 100);
    }

    @Test
    public void shouldShrinkWindowWhenAckSlow() {
        AdaptiveReceiveQuota quota = new AdaptiveReceiveQuota(
            100,
            TimeUnit.MILLISECONDS.toNanos(5),
            0.5,
            1.25,
            4.0,
            20.0,
            100);

        long ackInterval = TimeUnit.MILLISECONDS.toNanos(5);
        long now = 0;
        for (int i = 0; i < 5; i++) {
            long sendTime = now;
            now += ackInterval;
            quota.onPacketAcked(now, sendTime);
        }
        int beforeSlowAck = quota.window();

        long slowTimeout = (long) (ackInterval * 4.0);
        quota.availableQuota(now + slowTimeout + 1, 0);

        assertTrue(quota.window() < beforeSlowAck);
    }

    @Test
    public void shouldRespectReceiveMaximumAndInFlight() {
        AdaptiveReceiveQuota quota = new AdaptiveReceiveQuota(
            10,
            TimeUnit.MILLISECONDS.toNanos(5),
            0.5,
            1.25,
            4.0,
            50.0,
            10);

        long ackInterval = TimeUnit.MILLISECONDS.toNanos(5);
        long now = 0;
        for (int i = 0; i < 20; i++) {
            long sendTime = now;
            now += ackInterval;
            quota.onPacketAcked(now, sendTime);
        }

        assertTrue(quota.window() <= 10);
        assertEquals(quota.availableQuota(now, 10), 0);
    }
}
