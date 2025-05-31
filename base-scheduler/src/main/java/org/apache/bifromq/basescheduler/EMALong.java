/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.basescheduler;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

class EMALong {
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;
    private final Supplier<Long> nowSupplier;
    private final double alpha;
    private final double decay;
    private final long decayDelayNanos;
    private final AtomicLong value = new AtomicLong(0);
    private final AtomicLong lastUpdateTime = new AtomicLong(0);

    public EMALong(Supplier<Long> nowSupplier, double alpha, double decay, long decayDelayNanos) {
        this.nowSupplier = nowSupplier;
        this.alpha = alpha;
        this.decay = decay;
        this.decayDelayNanos = decayDelayNanos;
    }

    public void update(long newValue) {
        value.updateAndGet(v -> {
            lastUpdateTime.set(nowSupplier.get());
            if (v == 0) {
                return newValue;
            } else {
                return (long) Math.ceil(v * (1 - alpha) + newValue * alpha);
            }
        });
    }

    public long get() {
        long now = nowSupplier.get();
        long lastUpdate = lastUpdateTime.get();
        if (decayDelayNanos < Long.MAX_VALUE && lastUpdate + decayDelayNanos < now) {
            return (long) (value.get()
                * Math.pow(decay, Math.ceil((now - lastUpdate - decayDelayNanos) / NANOS_PER_SECOND)));
        }
        return value.get();
    }
}
