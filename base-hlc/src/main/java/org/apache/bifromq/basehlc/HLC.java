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

package org.apache.bifromq.basehlc;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * The HLC (Hybrid Logical Clock) is a logical clock that combines physical time with a causal counter.
 * It is designed to provide a consistent timestamp that can be used in distributed systems to order events
 * while also allowing for the physical time to be reflected in the timestamp.
 */
public class HLC {
    public static final HLC INST = new HLC();
    private static final long CAUSAL_MASK = 0x00_00_00_00_00_00_FF_FFL;
    private static final VarHandle CAE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.privateLookupIn(HLC.class, MethodHandles.lookup());
            CAE = l.findVarHandle(HLC.class, "hlc", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Assume cache line size is 64bytes, and first 16 bytes are reserved for object header.
     */
    private long p3;
    private long p4;
    private long p5;
    private long p6;
    private long p7;
    // noinspection FieldMayBeFinal
    private volatile long hlc = 0L;

    private long p9;
    private long p10;
    private long p11;
    private long p12;
    private long p13;
    private long p14;
    private long p15;

    private HLC() {
    }

    private static long nextMillis(long nowMillis) {
        long t;
        while ((t = System.currentTimeMillis()) == nowMillis) {
            Thread.onSpinWait();
        }
        return t;
    }

    /**
     * Get the current HLC timestamp.
     *
     * @return the current HLC timestamp
     */
    public long get() {
        for (; ; ) {
            long now = hlc;
            long l = logical(now);
            long c = causal(now);

            long phys = System.currentTimeMillis();
            long updateL = Math.max(l, phys);
            c = (updateL == l) ? c + 1 : 0;
            if (c > CAUSAL_MASK) {
                updateL = nextMillis(updateL);
                c = 0;
            }
            long newHLC = toTimestamp(updateL, c);
            long witness = (long) CAE.compareAndExchange(this, now, newHLC);
            if (witness == now) {
                return newHLC;
            }
            if (witness >= newHLC) {
                return witness;
            }
        }
    }

    /**
     * Update the HLC timestamp with another HLC timestamp.
     *
     * @param otherHLC the other HLC timestamp observed
     * @return the updated HLC timestamp
     */
    public long update(long otherHLC) {
        do {
            long now = hlc;
            long l = logical(now);
            long c = causal(now);
            long otherL = logical(otherHLC);
            long otherC = causal(otherHLC);
            long updateL = Math.max(l, Math.max(otherL, System.currentTimeMillis()));
            if (updateL == l && otherL == l) {
                c = Math.max(c, otherC) + 1;
            } else if (updateL == l) {
                c++;
            } else if (otherL == l) {
                c = otherC + 1;
            } else {
                c = 0;
            }
            if (c > CAUSAL_MASK) {
                updateL = nextMillis(updateL);
                c = 0;
            }
            long newHLC = toTimestamp(updateL, c);
            long witness = (long) CAE.compareAndExchange(this, now, newHLC);
            if (witness == now) {
                return newHLC;
            }
            if (witness >= newHLC) {
                return witness;
            }
        } while (true);
    }

    public long getPhysical() {
        return getPhysical(get());
    }

    public long getPhysical(long hlc) {
        return logical(hlc);
    }

    private long toTimestamp(long l, long c) {
        return (l << 16) | c;
    }

    private long logical(long hlc) {
        return hlc >> 16;
    }

    private long causal(long hlc) {
        return hlc & CAUSAL_MASK;
    }
}
