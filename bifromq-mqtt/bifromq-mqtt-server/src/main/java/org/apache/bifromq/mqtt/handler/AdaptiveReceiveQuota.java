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

final class AdaptiveReceiveQuota {
    private final int receiveMaximum;
    private final long rttFloorNanos;
    private final double emaAlpha;
    private final double gain;
    private final double slowAckFactor;
    private final double minBandwidthPerNano;
    private final int maxWindowStep;

    private double bandwidthEstimate;
    private long minRttNanos;
    private int windowEstimate;
    private long lastAckTimeNanos;
    private boolean slowAckPenalized;

    AdaptiveReceiveQuota(int receiveMaximum,
                         long rttFloorNanos,
                         double emaAlpha,
                         double gain,
                         double slowAckFactor,
                         double minBandwidthPerSecond,
                         int maxWindowStep) {
        this.receiveMaximum = Math.max(1, receiveMaximum);
        this.rttFloorNanos = Math.max(1L, rttFloorNanos);
        this.emaAlpha = Math.min(1D, Math.max(0D, emaAlpha));
        this.gain = Math.max(0D, gain);
        this.slowAckFactor = Math.max(1D, slowAckFactor);
        this.maxWindowStep = maxWindowStep;
        this.minBandwidthPerNano = Math.max(0D, minBandwidthPerSecond) / TimeUnit.SECONDS.toNanos(1);
        this.bandwidthEstimate = Math.max(this.minBandwidthPerNano, 1D / this.rttFloorNanos);
        long baseRtt = baselineRtt();
        this.windowEstimate = clampWindow((int) Math.ceil(this.bandwidthEstimate * baseRtt * this.gain),
            this.receiveMaximum);
    }

    void onPacketAcked(long ackTimeNanos, long lastSendTimeNanos) {
        if (ackTimeNanos <= 0) {
            return;
        }
        long rtt = rttFloorNanos;
        if (lastSendTimeNanos > 0 && ackTimeNanos > lastSendTimeNanos) {
            rtt = ackTimeNanos - lastSendTimeNanos;
        }
        if (minRttNanos == 0 || rtt < minRttNanos) {
            minRttNanos = rtt;
        }
        long ackInterval;
        if (lastAckTimeNanos > 0 && ackTimeNanos > lastAckTimeNanos) {
            ackInterval = ackTimeNanos - lastAckTimeNanos;
        } else {
            ackInterval = rtt;
        }
        double sampleBandwidth = 1D / (double) ackInterval;
        if (bandwidthEstimate <= 0) {
            bandwidthEstimate = sampleBandwidth;
        } else {
            bandwidthEstimate = (1 - emaAlpha) * bandwidthEstimate + emaAlpha * sampleBandwidth;
        }
        if (bandwidthEstimate < minBandwidthPerNano) {
            bandwidthEstimate = minBandwidthPerNano;
        }
        lastAckTimeNanos = ackTimeNanos;
        slowAckPenalized = false;
        updateWindow();
    }

    int availableQuota(long nowNanos, int inFlightCount) {
        if (windowEstimate > receiveMaximum) {
            windowEstimate = receiveMaximum;
        }
        long baselineRtt = baselineRtt();
        long slowAckTimeout = (long) Math.max(baselineRtt * slowAckFactor, baselineRtt);
        if (!slowAckPenalized
            && lastAckTimeNanos > 0
            && nowNanos > lastAckTimeNanos
            && nowNanos - lastAckTimeNanos > slowAckTimeout) {
            windowEstimate = Math.max(1, windowEstimate / 2);
            bandwidthEstimate = Math.max(minBandwidthPerNano, bandwidthEstimate / 2);
            slowAckPenalized = true;
        }
        if (windowEstimate > receiveMaximum) {
            windowEstimate = receiveMaximum;
        }
        if (windowEstimate < 1) {
            windowEstimate = 1;
        }
        return Math.max(0, windowEstimate - Math.max(0, inFlightCount));
    }

    int window() {
        return windowEstimate;
    }

    private void updateWindow() {
        long baselineRtt = baselineRtt();
        double targetWindow = bandwidthEstimate * baselineRtt * gain;
        int desired = clampWindow((int) Math.ceil(targetWindow), receiveMaximum);
        if (desired > windowEstimate) {
            windowEstimate = Math.min(desired, windowEstimate + maxWindowStep);
        } else if (desired < windowEstimate) {
            windowEstimate = Math.max(desired, windowEstimate - maxWindowStep);
        }
    }

    private int clampWindow(int value, int receiveMaximum) {
        int max = Math.max(1, receiveMaximum);
        if (value < 1) {
            return 1;
        }
        return Math.min(max, value);
    }

    private long baselineRtt() {
        return minRttNanos > 0 ? minRttNanos : rttFloorNanos;
    }
}
