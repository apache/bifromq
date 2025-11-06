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

package org.apache.bifromq.basekv.localengine.rocksdb;

// Package-visible adaptive write budget copied from base-kv-store-server module
// to avoid cross-package visibility; used by RestoreSession to tune flush cadence.
final class AdaptiveWriteBudget {
    private static final long TARGET_LATENCY_MS = 50;
    private static final double EMA_ALPHA = 0.3;
    private static final double FAST_THRESHOLD = 0.8;
    private static final double SLOW_THRESHOLD = 1.5;
    private static final double MULTIPLICATIVE_DECREASE = 0.5;
    private static final double ADDITIVE_INCREASE_RATIO = 0.1;
    private static final int FAST_SUCCESS_THRESHOLD = 3;
    private static final long INITIAL_ENTRY_BUDGET = 1024;
    private static final long INITIAL_BYTE_BUDGET = 4L * 1024 * 1024; // 4MB
    private static final long MAX_ENTRY_BUDGET = 128_000;
    private static final long MAX_BYTE_BUDGET = 256L * 1024 * 1024; // 256MB
    private static final long MIN_ENTRY_BUDGET = 1;
    private static final long MIN_BYTE_BUDGET = 1;

    private double entryBudgetEstimate;
    private double byteBudgetEstimate;
    private double emaEntryRate = -1;
    private double emaByteRate = -1;
    private int consecutiveFastRounds = 0;

    AdaptiveWriteBudget() {
        this.entryBudgetEstimate = INITIAL_ENTRY_BUDGET;
        this.byteBudgetEstimate = INITIAL_BYTE_BUDGET;
    }

    boolean shouldFlush(long entries, long bytes) {
        return entries >= currentEntryLimit() || bytes >= currentByteLimit();
    }

    void recordFlush(long entries, long bytes, long latencyMillis) {
        if ((entries <= 0 && bytes <= 0) || latencyMillis <= 0) {
            return;
        }
        double entryRate = entries > 0 ? entries / (double) latencyMillis : 0;
        double byteRate = bytes > 0 ? bytes / (double) latencyMillis : 0;

        emaEntryRate = ema(emaEntryRate, entryRate);
        emaByteRate = ema(emaByteRate, byteRate);

        double targetEntryBudget = clampEntryBudget(emaEntryRate * TARGET_LATENCY_MS);
        double targetByteBudget = clampByteBudget(emaByteRate * TARGET_LATENCY_MS);

        entryBudgetEstimate = clampEntryBudget(ema(entryBudgetEstimate, targetEntryBudget));
        byteBudgetEstimate = clampByteBudget(ema(byteBudgetEstimate, targetByteBudget));

        if (latencyMillis > TARGET_LATENCY_MS * SLOW_THRESHOLD) {
            entryBudgetEstimate = clampEntryBudget(entryBudgetEstimate * MULTIPLICATIVE_DECREASE);
            byteBudgetEstimate = clampByteBudget(byteBudgetEstimate * MULTIPLICATIVE_DECREASE);
            consecutiveFastRounds = 0;
            return;
        }

        if (latencyMillis < TARGET_LATENCY_MS * FAST_THRESHOLD) {
            consecutiveFastRounds++;
            if (consecutiveFastRounds >= FAST_SUCCESS_THRESHOLD) {
                entryBudgetEstimate = clampEntryBudget(entryBudgetEstimate
                    + Math.max(MIN_ENTRY_BUDGET, entryBudgetEstimate * ADDITIVE_INCREASE_RATIO));
                byteBudgetEstimate = clampByteBudget(byteBudgetEstimate
                    + Math.max(MIN_BYTE_BUDGET, byteBudgetEstimate * ADDITIVE_INCREASE_RATIO));
                consecutiveFastRounds = 0;
            }
        } else {
            consecutiveFastRounds = 0;
        }
    }

    long currentEntryLimit() {
        double budget = clampEntryBudget(entryBudgetEstimate);
        return Math.max(1L, (long) Math.ceil(budget));
    }

    long currentByteLimit() {
        double budget = clampByteBudget(byteBudgetEstimate);
        return Math.max(1L, (long) Math.ceil(budget));
    }

    private double clampEntryBudget(double value) {
        return clampDouble(value, MIN_ENTRY_BUDGET, MAX_ENTRY_BUDGET);
    }

    private double clampByteBudget(double value) {
        return clampDouble(value, MIN_BYTE_BUDGET, MAX_BYTE_BUDGET);
    }

    private double clampDouble(double value, double min, double max) {
        return Math.min(Math.max(min, value), max);
    }

    private double ema(double current, double sample) {
        if (current < 0) {
            return sample;
        }
        return current + EMA_ALPHA * (sample - current);
    }
}
