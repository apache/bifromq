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

package org.apache.bifromq.basekv.store.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.MockableTest;
import org.testng.annotations.Test;

public class VerUtilTest extends MockableTest {
    @Test
    void testBumpLowSimple() {
        // high=1, low=0x00FF -> bumpLow => high=1, low=0x0100
        long ver = 0x00000001_000000FFL;
        long bumped = VerUtil.bump(ver, false);
        assertEquals(bumped, 0x00000001_00000100L);
    }

    @Test
    void testBumpHighSimple() {
        // high=1, low=0x00FF -> bumpHigh => high=2, low=0
        long ver = 0x00000001_000000FFL;
        long bumped = VerUtil.bump(ver, true);
        assertEquals(bumped, 0x00000002_00000000L);
    }

    @Test
    void testLowOverflow() {
        // low at max: high=0x12345678, low=0xFFFFFFFF -> bumpLow => high unchanged, low wraps to 0
        long ver = 0x12345678_FFFFFFFFL;
        long bumped = VerUtil.bump(ver, false);
        assertEquals(bumped, 0x12345678_00000000L);
    }

    @Test
    void testHighOverflow() {
        // high at max: high=0xFFFFFFFF, low=0xAAAAAAAA -> bumpHigh => high wraps to 0, low=0
        long ver = 0xFFFFFFFF_AAAAAAAAL;
        long bumped = VerUtil.bump(ver, true);
        assertEquals(bumped, 0x00000000_00000000L);
    }

    @Test
    void testRepeatedBumpsMaintainMonotonicity() {
        long ver = 0;
        // do 10 low bumps, should just increment low
        for (int i = 1; i <= 10; i++) {
            ver = VerUtil.bump(ver, false);
            assertEquals(i, ver & 0xFFFFFFFFL);
            assertEquals(0, ver >>> 32);
        }
        // bump high once
        ver = VerUtil.bump(ver, true);
        assertEquals(ver >>> 32, 1);
        assertEquals(ver & 0xFFFFFFFFL, 0);
    }

    @Test
    void testBoundaryCompatible_sameHighDifferentLow() {
        long v1 = 0x00000001_00000002L;
        long v2 = 0x00000001_ABCDEF01L;
        assertTrue(VerUtil.boundaryCompatible(v1, v2));
    }

    @Test
    void testBoundaryCompatible_differentHighSameLow() {
        long v1 = 0x00000001_00000000L;
        long v2 = 0x00000002_00000000L;
        assertFalse(VerUtil.boundaryCompatible(v1, v2));
    }

    @Test
    void testBoundaryCompatible_bothHighZero() {
        long v1 = 0x00000000_00000000L;
        long v2 = 0x00000000_FFFFFFFFL;
        assertTrue(VerUtil.boundaryCompatible(v1, v2));
    }

    @Test
    void testBoundaryCompatible_highOverflowWraps() {
        long original = (0xFFFFFFFFL << 32) | 0x1234L;
        long wrapped = VerUtil.bump(original, true);
        assertFalse(VerUtil.boundaryCompatible(original, wrapped));
    }

    @Test
    void testBoundaryCompatible_multipleMix() {
        long ver = 0;
        for (int i = 0; i < 100; i++) {
            long next = VerUtil.bump(ver, false);
            assertTrue(VerUtil.boundaryCompatible(ver, next));
            ver = next;
        }
        long bumpedHigh = VerUtil.bump(ver, true);
        assertFalse(VerUtil.boundaryCompatible(ver, bumpedHigh));
    }
}
