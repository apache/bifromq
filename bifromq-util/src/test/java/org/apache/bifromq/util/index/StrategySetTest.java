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

package org.apache.bifromq.util.index;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class StrategySetTest {

    @Test
    public void testDeduplicationByStrategy() {
        StrategySet<TestVal> set = new StrategySet<>(new TestValueStrategy());
        assertTrue(set.add(new TestVal("k", "p1")));
        assertFalse(set.add(new TestVal("k", "p2")));
        assertEquals(set.size(), 1);
    }

    @Test
    public void testContainsAndRemoveByStrategy() {
        StrategySet<TestVal> set = new StrategySet<>(new TestValueStrategy());
        TestVal v1 = new TestVal("k", "p1");
        TestVal v2 = new TestVal("k", "p2");
        set.add(v1);
        assertTrue(set.contains(v2));
        assertTrue(set.remove(v2));
        assertTrue(set.isEmpty());
    }

    @Test
    public void testIteratorReturnsOriginalValues() {
        StrategySet<TestVal> set = new StrategySet<>(new TestValueStrategy());
        TestVal a = new TestVal("a", "p1");
        TestVal b = new TestVal("b", "p2");
        set.add(a);
        set.add(b);
        assertEquals(set.size(), 2);
        int seen = 0;
        for (TestVal v : set) {
            assertTrue(v.key().equals("a") || v.key().equals("b"));
            seen++;
        }
        assertEquals(seen, 2);
    }

    @Test
    public void testNaturalStrategyBasics() {
        StrategySet<String> set = new StrategySet<>(ValueStrategy.natural());
        assertTrue(set.add("x"));
        assertFalse(set.add("x"));
        assertTrue(set.contains("x"));
        assertTrue(set.remove("x"));
        assertFalse(set.contains("x"));
    }

    @Test
    public void testIdentityStrategyBehavior() {
        StrategySet<TestVal> set = new StrategySet<>(ValueStrategy.identity());
        TestVal a1 = new TestVal("a", "p1");
        TestVal a2 = new TestVal("a", "p1");
        assertTrue(set.add(a1));
        assertTrue(set.add(a2));
        assertEquals(set.size(), 2);
        assertTrue(set.contains(a1));
        assertTrue(set.remove(a1));
        assertFalse(set.contains(a1));
        assertTrue(set.contains(a2));
    }
}
