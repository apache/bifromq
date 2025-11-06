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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class AdaptiveWriteBudgetTest {

    @Test
    public void initialLimitsPositive() {
        AdaptiveWriteBudget b = new AdaptiveWriteBudget();
        assertTrue(b.currentEntryLimit() > 0);
        assertTrue(b.currentByteLimit() > 0);
        assertFalse(b.shouldFlush(0, 0));
        assertTrue(b.shouldFlush(b.currentEntryLimit(), 0));
        assertTrue(b.shouldFlush(0, b.currentByteLimit()));
    }

    @Test
    public void fastRoundsIncreaseBudgets() {
        AdaptiveWriteBudget b = new AdaptiveWriteBudget();
        long e1 = b.currentEntryLimit();
        long by1 = b.currentByteLimit();
        for (int i = 0; i < 3; i++) {
            b.recordFlush(e1, by1, 10); // fast
        }
        assertTrue(b.currentEntryLimit() >= e1);
        assertTrue(b.currentByteLimit() >= by1);
    }

    @Test
    public void slowRoundDecreasesBudgets() {
        AdaptiveWriteBudget b = new AdaptiveWriteBudget();
        for (int i = 0; i < 3; i++) {
            b.recordFlush(b.currentEntryLimit(), b.currentByteLimit(), 10);
        }
        long beforeE = b.currentEntryLimit();
        long beforeB = b.currentByteLimit();
        b.recordFlush(beforeE, beforeB, 200); // slow
        assertTrue(b.currentEntryLimit() <= beforeE);
        assertTrue(b.currentByteLimit() <= beforeB);
    }

    @Test
    public void emaAndClampBehavior() {
        AdaptiveWriteBudget b = new AdaptiveWriteBudget();
        for (int i = 0; i < 10; i++) {
            b.recordFlush(100, 1024 * 1024, 30); // fast-ish
            b.recordFlush(100, 1024 * 1024, 120); // slow-ish
        }
        assertTrue(b.currentEntryLimit() >= 1);
        assertTrue(b.currentByteLimit() >= 1);
    }

    @Test
    public void noopOnZeroInputs() {
        AdaptiveWriteBudget b = new AdaptiveWriteBudget();
        long e = b.currentEntryLimit();
        long by = b.currentByteLimit();
        b.recordFlush(0, 0, 30);
        b.recordFlush(100, 1024, 0);
        assertEquals(b.currentEntryLimit(), e);
        assertEquals(b.currentByteLimit(), by);
    }
}
