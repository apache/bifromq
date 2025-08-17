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

import static org.testng.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EMALongTest {
    private AtomicLong fakeTime;
    private Supplier<Long> nowSupplier;

    @BeforeMethod
    void setUp() {
        fakeTime = new AtomicLong(0L);
        nowSupplier = fakeTime::get;
    }

    @Test
    void testInitialGet() {
        // alpha=0.5, decay=0.5, delay=1s
        EMALong ema = new EMALong(nowSupplier, 0.5, 0.5, 1_000_000_000L);
        // no update called yet, value=0
        assertEquals(ema.get(), 0);
    }

    @Test
    void testSingleUpdate() {
        EMALong ema = new EMALong(nowSupplier, 0.5, 0.5, 1_000_000_000L);
        fakeTime.set(100L);
        ema.update(10L);
        // get should return initial newValue
        assertEquals(ema.get(), 10);
    }

    @Test
    void testEMAUpdate() {
        // alpha=0.5
        EMALong ema = new EMALong(nowSupplier, 0.5, 0.5, 1_000_000_000L);
        fakeTime.set(0L);
        ema.update(10L);
        fakeTime.set(1L);
        ema.update(20L);
        // computed as ceil(10*(1-0.5) + 20*0.5) = ceil(5 + 10) = 15
        assertEquals(ema.get(), 15);
    }

    @Test
    void testDecayBeforeDelay() {
        // decayDelay=2s
        EMALong ema = new EMALong(nowSupplier, 0.5, 0.5, 2_000_000_000L);
        fakeTime.set(0L);
        ema.update(100L);
        // advance time to just before delay
        fakeTime.set(2_000_000_000L - 1L);
        // decay should not apply
        assertEquals(ema.get(), 100);
    }

    @Test
    void testDecayAfterDelay() {
        // set decay=0.5, decayDelay=1s
        EMALong ema = new EMALong(nowSupplier, 0.5, 0.5, 1_000_000_000L);
        fakeTime.set(1L);
        ema.update(100L);
        // advance time to after delay + 2s total => one decay period
        fakeTime.set(1_000_000_001L + 1_000_000_000L);
        // (now - lastUpdate - delay) / 1e9 = (2s - 1s)/1e9 = 1 => ceil(1) =1
        // value * decay^1 = 100 * 0.5 = 50
        assertEquals(ema.get(), 50);
    }

    @Test
    void testMultipleDecayPeriods() {
        // decay=0.5, delay=1s
        EMALong ema = new EMALong(nowSupplier, 0.5, 0.5, 1_000_000_000L);
        fakeTime.set(1L);
        ema.update(80L);
        // advance time to after delay + 3.2s => ceil(3.2)=4 periods
        fakeTime.set(1_000_000_000L + 3_200_000_000L);
        // expected = 80 * 0.5^4 = 80 / 16 = 5
        assertEquals(ema.get(), 5);
    }

    @Test
    void neverDecay() {
        // decay=0.5, decayDelay=Long.MAX_VALUE
        EMALong ema = new EMALong(nowSupplier, 0.5, 0.5, Long.MAX_VALUE);
        fakeTime.set(0L);
        ema.update(80L);
        // advance time to forever
        fakeTime.set(Integer.MAX_VALUE);
        // expected = 80
        assertEquals(ema.get(), 80);
    }
}
