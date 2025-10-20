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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class AdaptiveReceiveQuotaTest {
    private static final long MS = MILLISECONDS.toNanos(1);
    private static final long EVAL = MILLISECONDS.toNanos(200);
    private static final long FREEZE = MILLISECONDS.toNanos(300);
    private static final double EMA_ALPHA = 0.15d;

    private void ack(AdaptiveReceiveQuota q, long now, long rtt, int inflight) {
        q.onPacketAcked(now, now - rtt, inflight);
    }

    @Test
    public void testInitialQuotaEqualsReceiveMin() {
        AdaptiveReceiveQuota q = new AdaptiveReceiveQuota(8, 64, EMA_ALPHA);
        assertEquals(q.availableQuota(), 8);
    }

    @Test
    public void testNoAdjustmentBeforeFirstEval() {
        AdaptiveReceiveQuota q = new AdaptiveReceiveQuota(8, 64, EMA_ALPHA);
        long t = 1_000_000_000L;
        ack(q, t, MS, 0);
        int w0 = q.availableQuota();
        // less than one eval period
        t += EVAL / 2;
        ack(q, t, MS, 0);
        assertEquals(q.availableQuota(), w0);
    }

    @Test
    public void testHealthyHighUtilizationGrowsUpToReceiveMax() {
        AdaptiveReceiveQuota q = new AdaptiveReceiveQuota(8, 64, EMA_ALPHA);
        long t = 2_000_000_000L;
        ack(q, t, MS, 0);
        for (int i = 0; i < 10; i++) {
            int w = q.availableQuota();
            t += EVAL;
            ack(q, t, MS, w);
            if (q.availableQuota() == 64) {
                break;
            }
        }
        assertEquals(q.availableQuota(), 64);
    }

    @Test
    public void testHealthyLowUtilizationShrinkRespectsReceiveMin() {
        AdaptiveReceiveQuota q = new AdaptiveReceiveQuota(8, 64, EMA_ALPHA);
        long t = 3_000_000_000L;
        ack(q, t, MS, 0);
        while (q.availableQuota() < 32) {
            int w = q.availableQuota();
            t += EVAL;
            ack(q, t, MS, w);
        }
        int before = q.availableQuota();
        // low utilization triggers ~5% shrink (ceil)
        t += EVAL;
        ack(q, t, MS, 0);
        int after = q.availableQuota();
        assertTrue(after < before);
        assertTrue(after >= 8);
    }

    @Test
    public void testCongestionShrinkAndFreeze() {
        AdaptiveReceiveQuota q = new AdaptiveReceiveQuota(8, 64, EMA_ALPHA);
        long t = 4_000_000_000L;
        ack(q, t, MS, 0);
        t += EVAL;
        ack(q, t, MS, q.availableQuota());
        int before = q.availableQuota();
        // trigger congestion with a higher RTT to exceed ratio threshold
        t += EVAL;
        ack(q, t, 5 * MS, before); // large RTT to push fast EWMA up
        int shrunk = q.availableQuota();
        assertTrue(shrunk <= before);
        // inside freeze: healthy high-utilization should not grow
        t += EVAL;
        ack(q, t, MS, shrunk);
        assertEquals(q.availableQuota(), shrunk);
        // after freeze: allow growth again
        t += EVAL * 2; // ensure beyond freeze window
        ack(q, t, MS, q.availableQuota());
        assertTrue(q.availableQuota() >= shrunk);
    }

    @Test
    public void testErrorSignalCooldownAndFreeze() {
        AdaptiveReceiveQuota q = new AdaptiveReceiveQuota(8, 64, EMA_ALPHA);
        long t = 5_000_000_000L;
        // ensure lastEvalAt large enough for cooldown check not to early-return
        q.onErrorSignal(t);
        int w1 = q.availableQuota();
        // within cooldown: no further shrink
        q.onErrorSignal(t + FREEZE / 2);
        int w2 = q.availableQuota();
        assertEquals(w2, w1);
        // after cooldown: another shrink
        q.onErrorSignal(t + FREEZE + MILLISECONDS.toNanos(50));
        int w3 = q.availableQuota();
        assertTrue(w3 <= w2);
    }

    @Test
    public void testCatchUpAcrossMultipleEvalPeriods() {
        AdaptiveReceiveQuota q = new AdaptiveReceiveQuota(8, 64, EMA_ALPHA);
        long t = 6_000_000_000L;
        ack(q, t, MS, 0);
        // jump forward 3 periods with high utilization
        t += EVAL * 3;
        ack(q, t, MS, q.availableQuota());
        assertTrue(q.availableQuota() >= 16);
    }

    @Test
    public void testNegativeRTTSampleIgnored() {
        AdaptiveReceiveQuota q = new AdaptiveReceiveQuota(8, 64, EMA_ALPHA);
        long t = 7_000_000_000L;
        ack(q, t, MS, 0);
        int before = q.availableQuota();
        // negative RTT sample
        long rtt = -MS;
        t += EVAL;
        q.onPacketAcked(t, t - rtt, 0);
        assertEquals(q.availableQuota(), before);
    }

    @Test
    public void testReceiveMinEqualsReceiveMaxProducesFixedWindow() {
        AdaptiveReceiveQuota q = new AdaptiveReceiveQuota(16, 16, EMA_ALPHA);
        long t = 8_000_000_000L;
        ack(q, t, MS, 0);
        assertEquals(q.availableQuota(), 16);
        // attempt to grow
        t += EVAL;
        ack(q, t, MS, 16);
        assertEquals(q.availableQuota(), 16);
        // attempt to shrink by congestion
        t += EVAL;
        ack(q, t, 5 * MS, 16);
        assertEquals(q.availableQuota(), 16);
    }

    @Test
    public void testReceiveMinEqualsReceiveMaxWithErrorSignalAndFreeze() {
        AdaptiveReceiveQuota q = new AdaptiveReceiveQuota(16, 16, EMA_ALPHA);
        long t = 8_500_000_000L;
        // initialize clock
        ack(q, t, MS, 0);
        assertEquals(q.availableQuota(), 16);
        // trigger error shrink after cooldown; still clamped at 16
        long tErr = t + FREEZE + MILLISECONDS.toNanos(1);
        q.onErrorSignal(tErr);
        assertEquals(q.availableQuota(), 16);
        // within cooldown: no further shrink (still clamped)
        q.onErrorSignal(tErr + FREEZE / 2);
        assertEquals(q.availableQuota(), 16);
        // after cooldown: another error signal; remains clamped
        q.onErrorSignal(tErr + FREEZE + MILLISECONDS.toNanos(1));
        assertEquals(q.availableQuota(), 16);
        // healthy high utilization during freeze should also not grow
        long tAfter = tErr + EVAL;
        ack(q, tAfter, MS, 16);
        assertEquals(q.availableQuota(), 16);
    }

    @Test
    public void testUtilizationBandSteeringBoundaries() {
        AdaptiveReceiveQuota q = new AdaptiveReceiveQuota(20, 100, EMA_ALPHA);
        long t = 9_000_000_000L;
        ack(q, t, MS, 0);
        assertEquals(q.availableQuota(), 20);
        // high utilization -> grow
        t += EVAL;
        ack(q, t, MS, 20);
        int w1 = q.availableQuota();
        assertTrue(w1 > 20);
        // low utilization -> shrink
        t += EVAL;
        ack(q, t, MS, 0);
        int w2 = q.availableQuota();
        assertTrue(w2 < w1);
        assertTrue(w2 >= 20);
        // mid utilization in band -> steady
        t += EVAL;
        ack(q, t, MS, Math.max(1, (int) Math.round(w2 * 0.07)));
        int w3 = q.availableQuota();
        assertEquals(w3, w2);
    }
}
