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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import java.util.Objects;
import org.testng.annotations.Test;

public class ValueRefStrategyTest {

    @Test
    public void testValueRefEqualityWithSameStrategyInstance() {
        ValueStrategy<TestVal> s = new TestValueStrategy();
        TestVal v1 = new TestVal("k", "d1");
        TestVal v2 = new TestVal("k", "d2");
        TestVal v3 = new TestVal("k2", "d3");

        ValueRef<TestVal> r1 = ValueRef.of(v1, s);
        ValueRef<TestVal> r2 = ValueRef.of(v2, s);
        ValueRef<TestVal> r3 = ValueRef.of(v3, s);

        assertEquals(r2, r1);
        assertEquals(r1.hashCode(), r2.hashCode());
        assertNotEquals(r3, r1);
    }

    @Test
    public void testValueRefEqualityRequiresSameStrategyInstance() {
        ValueStrategy<TestVal> s1 = new TestValueStrategy();
        ValueStrategy<TestVal> s2 = new TestValueStrategy();
        TestVal v1 = new TestVal("k", "d1");
        TestVal v2 = new TestVal("k", "d2");

        ValueRef<TestVal> r1 = ValueRef.of(v1, s1);
        ValueRef<TestVal> r2 = ValueRef.of(v2, s2);

        assertNotEquals(r2, r1);
    }

    @Test
    public void testNaturalStrategyDelegatesToObjectEqualsHash() {
        ValueStrategy<Plain> s = ValueStrategy.natural();
        Plain p1 = new Plain("x", 1);
        Plain p2 = new Plain("x", 1);
        Plain p3 = new Plain("x", 2);

        ValueRef<Plain> r1 = ValueRef.of(p1, s);
        ValueRef<Plain> r2 = ValueRef.of(p2, s);
        ValueRef<Plain> r3 = ValueRef.of(p3, s);

        assertEquals(r2, r1);
        assertEquals(r1.hashCode(), r2.hashCode());
        assertNotEquals(r3, r1);
    }

    @Test
    public void testNaturalStrategyHandlesNulls() {
        ValueStrategy<Object> s = ValueStrategy.natural();
        assertEquals(s.hash(null), 0);
        assertTrue(s.equivalent(null, null));
        assertFalse(s.equivalent(null, new Object()));
    }

    private static final class Plain {
        final String a;
        final int b;

        Plain(String a, int b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Plain p)) {
                return false;
            }
            return b == p.b && (Objects.equals(a, p.a));
        }

        @Override
        public int hashCode() {
            int h = 17;
            h = 31 * h + (a == null ? 0 : a.hashCode());
            h = 31 * h + b;
            return h;
        }
    }
}

