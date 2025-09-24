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

package org.apache.bifromq.basekv.store.range;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class AdaptiveWriteBudgetTest {
    @Test
    public void shouldRespectDefaultMaxLimits() {
        AdaptiveWriteBudget budget = new AdaptiveWriteBudget();

        for (int i = 0; i < 20; i++) {
            budget.recordFlush(1_000_000, 10_000_000, 1);
        }

        assertEquals(budget.currentEntryLimit(), 128_000);
        assertEquals(budget.currentByteLimit(), 256L * 1024 * 1024);
    }

    @Test
    public void shouldGrowWhenFlushIsFast() {
        AdaptiveWriteBudget budget = new AdaptiveWriteBudget();
        long initialEntryLimit = budget.currentEntryLimit();
        long initialByteLimit = budget.currentByteLimit();

        budget.recordFlush(initialEntryLimit, initialByteLimit, 10);

        assertTrue(budget.currentEntryLimit() > initialEntryLimit);
        assertTrue(budget.currentByteLimit() > initialByteLimit);
    }

    @Test
    public void shouldShrinkWhenFlushIsSlow() {
        AdaptiveWriteBudget budget = new AdaptiveWriteBudget();
        long initialEntryLimit = budget.currentEntryLimit();
        long initialByteLimit = budget.currentByteLimit();

        budget.recordFlush(initialEntryLimit * 2, initialByteLimit * 2, 10);
        long increasedEntryLimit = budget.currentEntryLimit();
        long increasedByteLimit = budget.currentByteLimit();
        assertTrue(increasedEntryLimit > initialEntryLimit);
        assertTrue(increasedByteLimit > initialByteLimit);

        budget.recordFlush(increasedEntryLimit, increasedByteLimit, 200);

        assertTrue(budget.currentEntryLimit() < increasedEntryLimit);
        assertTrue(budget.currentByteLimit() < increasedByteLimit);
    }

    @Test
    public void shouldFlushWhenBudgetReached() {
        AdaptiveWriteBudget budget = new AdaptiveWriteBudget();
        long entryLimit = budget.currentEntryLimit();
        long byteLimit = budget.currentByteLimit();

        assertTrue(budget.shouldFlush(entryLimit, 0));
        assertTrue(budget.shouldFlush(0, byteLimit));
    }
}
