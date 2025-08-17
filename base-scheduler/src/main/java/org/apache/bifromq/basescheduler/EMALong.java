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

import com.google.common.base.Preconditions;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

class EMALong {
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;
    private final Supplier<Long> nowSupplier;
    private final double alpha; // (0,1]
    private final double decay; // (0,1]
    private final long decayDelayNanos;
    private final AtomicReference<State> state;

    public EMALong(Supplier<Long> nowSupplier, double alpha, double decay, long decayDelayNanos) {
        Preconditions.checkArgument(alpha > 0.0 && alpha <= 1.0, "alpha must be in (0,1]");
        Preconditions.checkArgument(decay > 0.0 && decay <= 1.0, "decay must be in (0,1]");
        Preconditions.checkArgument(decayDelayNanos >= 0, "decayDelayNanos must be non-negative");
        this.nowSupplier = nowSupplier;
        this.alpha = alpha;
        this.decay = decay;
        this.decayDelayNanos = decayDelayNanos;
        this.state = new AtomicReference<>(new State(0L, 0L));
    }

    public void update(long newValue) {
        long now = nowSupplier.get();
        while (true) {
            State prev = state.get();
            long newEma = (prev.ema == 0L) ? newValue : (long) Math.ceil(prev.ema * (1 - alpha) + newValue * alpha);
            State next = new State(newEma, now);
            if (state.compareAndSet(prev, next)) {
                return;
            }
        }
    }

    public long get() {
        long now = nowSupplier.get();
        State s = state.get();
        if (s.ema == 0L || s.lastTs == 0L) {
            return s.ema;
        }
        if (decayDelayNanos < Long.MAX_VALUE) {
            long dt = now - s.lastTs;
            if (dt > decayDelayNanos) {
                double seconds = Math.ceil((dt - decayDelayNanos) / NANOS_PER_SECOND);
                double decayed = s.ema * Math.pow(decay, seconds);
                return decayed < 1.0 ? 0L : Math.round(decayed);
            }
        }
        return s.ema;
    }

    private record State(long ema, long lastTs) {
    }
}
