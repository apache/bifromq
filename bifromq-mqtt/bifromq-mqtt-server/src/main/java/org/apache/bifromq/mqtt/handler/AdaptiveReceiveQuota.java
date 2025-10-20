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

import java.util.concurrent.TimeUnit;

/**
 * Adaptive, congestion-aware receive quota controller for MQTT sessions.
 * <p/>
 * Estimates a proper in-flight window by tracking RTT with dual EWMAs,
 * detecting congestion via latency ratio, applying multiplicative decrease
 * with a short growth freeze on congestion, steering by utilization
 * when latency is healthy, and clamping within a fixed floor and receive maximum.
 */
final class AdaptiveReceiveQuota {
    private static final double EPS_LOW = 0.05;    // healthy if r <= 1+EPS_LOW
    private static final double EPS_HIGH = 0.15;   // congested if r >= 1+EPS_HIGH
    private static final double SHRINK_RATIO = 0.10; // multiplicative decrease factor
    private static final double U_LOW = 0.05;       // keep utilization above 5%
    private static final double U_HIGH = 0.10;      // and below 10%
    private static final double U_TARGET = 0.075;   // midpoint for steering when out of band
    private static final double AMP_MIN = 1.10;     // min grow amplification per window when util > U_HIGH
    private static final double AMP_MAX = 2.00;     // max grow amplification per window when util > U_HIGH
    private static final double HEALTHY_SHRINK = 0.05; // shrink 5% per window when util < U_LOW

    private static final long EVAL_PERIOD_NANOS = TimeUnit.MILLISECONDS.toNanos(200);
    private static final long SHRINK_COOLDOWN_NANOS = TimeUnit.MILLISECONDS.toNanos(300);
    private static final long ERROR_FREEZE_NANOS = TimeUnit.MILLISECONDS.toNanos(300);

    private static final double BASE_FLOOR_ALPHA = 0.01; // only move floor upward

    private final int recvMin; // quota floor
    private final int recvMax; // quota ceiling
    private final double emaAlpha;

    private double fastLatencyEWMA = 0;
    private double slowLatencyEWMA = 0;
    private double baseFloorNanos = 0;

    private int quotaEstimate;
    private int lastInflight = 0;

    private long lastEvalAtNanos = 0;
    private long growthFreezeUntilNanos = 0;

    /**
     * Construct a quota controller bounded by receiveMaximum and configured EWMA alpha.
     *
     * @param receiveMin lower bound for available quota
     * @param receiveMax upper bound for available quota
     * @param emaAlpha smoothing factor in [0, 1] for the fast EWMA
     */
    AdaptiveReceiveQuota(int receiveMin, int receiveMax, double emaAlpha) {
        assert receiveMin > 0 && receiveMin <= receiveMax;
        this.recvMin = receiveMin;
        this.recvMax = receiveMax;
        this.emaAlpha = Math.min(1, Math.max(0, emaAlpha));
        this.quotaEstimate = recvMin;
    }

    /**
     * Observe ACK to update latency statistics and utilization, then trigger periodic evaluation.
     *
     * @param ackTimeNanos time when ACK is received
     * @param lastSendTimeNanos time when the acked packet was sent
     * @param inflightCountAtAck in-flight count observed at ACK
     */
    void onPacketAcked(long ackTimeNanos, long lastSendTimeNanos, int inflightCountAtAck) {
        long rtt = ackTimeNanos - lastSendTimeNanos;
        if (rtt > 0) {
            // fast EWMA
            fastLatencyEWMA = ewma(fastLatencyEWMA, rtt, emaAlpha);
            // slow EWMA: up fast, down slow
            if (slowLatencyEWMA == 0) {
                slowLatencyEWMA = rtt;
            } else {
                double alphaSlowUp = Math.max(0.02, emaAlpha * 0.5);
                double alphaSlowDown = Math.max(0.005, emaAlpha * 0.1);
                double a = (fastLatencyEWMA > slowLatencyEWMA) ? alphaSlowUp : alphaSlowDown;
                slowLatencyEWMA = ewma(slowLatencyEWMA, rtt, a);
            }
            // baseline floor moves up only
            baseFloorNanos = baseFloorNanos == 0 ? rtt : decayFloor(baseFloorNanos, slowLatencyEWMA);
        }
        lastInflight = Math.max(0, inflightCountAtAck);
        maybeEvaluate(ackTimeNanos);
    }

    /**
     * React to error by shrinking quota and freezing growth briefly.
     * Consecutive shrinks are limited by cooldown.
     *
     * @param nowNanos current time
     */
    void onErrorSignal(long nowNanos) {
        if (nowNanos - lastEvalAtNanos < SHRINK_COOLDOWN_NANOS) {
            return;
        }
        quotaEstimate = clampWindow((int) Math.ceil(quotaEstimate * (1D - Math.min(1D, SHRINK_RATIO))));
        growthFreezeUntilNanos = nowNanos + ERROR_FREEZE_NANOS;
        lastEvalAtNanos = nowNanos;
    }

    /**
     * Get current available quota clamped within bounds.
     *
     * @return non-negative available quota
     */
    int availableQuota() {
        return clampWindow(quotaEstimate);
    }

    private void maybeEvaluate(long nowNanos) {
        if (lastEvalAtNanos == 0) {
            lastEvalAtNanos = nowNanos;
            return;
        }
        while (nowNanos - lastEvalAtNanos >= EVAL_PERIOD_NANOS) {
            long evalAt = lastEvalAtNanos + EVAL_PERIOD_NANOS;
            evaluateOnce(evalAt);
            lastEvalAtNanos = evalAt;
        }
    }

    private void evaluateOnce(long evalAtNanos) {
        if (evalAtNanos < growthFreezeUntilNanos) {
            // in freeze window, skip adjustment
            return;
        }
        // compute ratio r = fast / base
        double baseCandidate = slowLatencyEWMA > 0 ? slowLatencyEWMA : (double) TimeUnit.MILLISECONDS.toNanos(1);
        double base = Math.max(baseCandidate, baseFloorNanos);
        if (base <= 0 || fastLatencyEWMA <= 0) {
            return;
        }
        double r = fastLatencyEWMA / base;
        if (r >= 1 + EPS_HIGH) {
            // multiplicative decrease with cooldown
            quotaEstimate = clampWindow((int) Math.ceil(quotaEstimate * (1 - SHRINK_RATIO)));
            growthFreezeUntilNanos = evalAtNanos + SHRINK_COOLDOWN_NANOS;
        } else if (r <= 1 + EPS_LOW) {
            // healthy window: steer quota to keep utilization within [U_LOW, U_HIGH]
            int w = Math.max(recvMin, quotaEstimate);
            double util = (double) lastInflight / (double) w;
            if (util > U_HIGH) {
                double factor = Math.max(AMP_MIN, Math.min(AMP_MAX, util / U_TARGET));
                int newW = clampWindow((int) Math.ceil(w * factor));
                if (newW > w) {
                    quotaEstimate = newW;
                }
            } else if (util < U_LOW) {
                int newW = clampWindow((int) Math.ceil(w * (1 - HEALTHY_SHRINK)));
                if (newW < w) {
                    quotaEstimate = newW;
                }
            }
        }
        // keep
    }

    private int clampWindow(int value) {
        return Math.min(recvMax, Math.max(recvMin, value));
    }

    private double ewma(double current, double sample, double alpha) {
        return current == 0 ? sample : (1 - alpha) * current + alpha * sample;
    }

    private double decayFloor(double floor, double target) {
        if (target > floor) {
            return floor + BASE_FLOOR_ALPHA * (target - floor);
        }
        return floor;
    }
}
