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

package org.apache.bifromq.basescheduler;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

public class MovingAverage {
    private final long freshness;
    private final Observation[] observations;
    private final AtomicReference<Stat> lastStat = new AtomicReference<>(new Stat(0, 0, 0, 0));
    private long total;
    private int head;
    private int tail;

    public MovingAverage(int precision, Duration freshness) {
        assert precision > 0 : "Precision must be positive";
        observations = new Observation[precision + 1]; // one more for sentinel
        for (int i = 0; i < observations.length; i++) {
            observations[i] = new Observation();
        }
        this.freshness = freshness.toNanos();
    }

    public synchronized void observe(long read) {
        long lastObserveTs = System.nanoTime();
        observations[head].ts = lastObserveTs;
        observations[head].read = read;
        total += read;
        head = (head + 1) % observations.length;
        if (head == tail) {
            // make room for head
            total -= observations[head].read;
            tail = (tail + 1) % observations.length;
        }
        while (lastObserveTs - observations[tail].ts > freshness) {
            // remove staled observation
            total -= observations[tail].read;
            tail = (tail + 1) % observations.length;
        }
        updateStat(read, lastObserveTs);
    }

    public long max() {
        long now = System.nanoTime();
        Stat stat = lastStat.get();
        if (now - stat.lastObserveTs > freshness) {
            return 0;
        } else {
            return stat.max;
        }
    }

    public long estimate() {
        long now = System.nanoTime();
        Stat stat = lastStat.get();
        if (now - stat.lastObserveTs > freshness) {
            return 0;
        } else {
            return stat.avg;
        }
    }

    private long calcAvg() {
        int count = (head - tail + observations.length) % observations.length;
        long avg = 0;
        if (count > 0) {
            avg = total / count;
        }
        return avg;
    }

    private void updateStat(long read, long lastObserveTs) {
        Stat lastStat = this.lastStat.get();
        if (lastObserveTs - lastStat.lastMaxObserveTs > freshness || read >= lastStat.max) {
            lastStat = new Stat(calcAvg(), read, lastObserveTs, lastObserveTs);
        } else {
            lastStat = new Stat(calcAvg(), lastStat.max, lastObserveTs, lastStat.lastMaxObserveTs);
        }
        this.lastStat.set(lastStat);
    }

    private static class Observation {
        private long ts;
        private long read;
    }

    private record Stat(long avg, long max, long lastObserveTs, long lastMaxObserveTs) {
    }
}
