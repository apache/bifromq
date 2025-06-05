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
     * Assume cache line size is 64bytes, and first 16 bytes are reserved for object header
     */
    private long p3;
    private long p4;
    private long p5;
    private long p6;
    private long p7;
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

    public long get() {
        long now = hlc;
        long l = logical(now);
        long c = causal(now);
        long updateL = Math.max(l, System.currentTimeMillis());
        if (updateL == l) {
            c++;
        } else {
            c = 0;
        }
        long newHLC = toTimestamp(updateL, c);
        set(now, newHLC);
        return newHLC;
    }

    public long update(long otherHLC) {
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
        long newHLC = toTimestamp(updateL, c);
        set(now, newHLC);
        return newHLC;
    }

    public long getPhysical() {
        return getPhysical(get());
    }

    public long getPhysical(long hlc) {
        return logical(hlc);
    }

    private long toTimestamp(long l, long c) {
        return (l << 16) + c;
    }

    private long logical(long hlc) {
        return hlc >> 16;
    }

    private long causal(long hlc) {
        return hlc & CAUSAL_MASK;
    }

    private void set(long now, long newHLC) {
        long expected = now;
        long witness;
        do {
            witness = (long) CAE.compareAndExchange(this, expected, newHLC);
            if (witness == expected || witness >= newHLC) {
                break;
            } else {
                expected = witness;
            }
        } while (true);
    }
}
