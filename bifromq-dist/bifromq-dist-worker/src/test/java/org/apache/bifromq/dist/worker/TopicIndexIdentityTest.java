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

package org.apache.bifromq.dist.worker;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests to validate identity-based value semantics in TopicIndex.
 */
public class TopicIndexIdentityTest {
    private TopicIndex<Box> index;

    @BeforeMethod
    public void setup() {
        index = new TopicIndex<>();
    }

    @Test
    public void testKeepDifferentInstancesOnSameTopic() {
        String topic = "a/b";
        Box b1 = new Box("x");
        Box b2 = new Box("x"); // equals but different instance
        index.add(topic, b1);
        index.add(topic, b2);

        Set<Box> res = index.get(topic);
        assertEquals(res.size(), 2);
        assertTrue(res.contains(b1));
        assertTrue(res.contains(b2));
    }

    @Test
    public void testRemoveRequiresSameInstance() {
        String topic = "a/b";
        Box b1 = new Box("x");
        Box b2 = new Box("x"); // equal but not same instance
        index.add(topic, b1);
        index.add(topic, b2);

        // Remove with an equal but different instance should not remove anything
        index.remove(topic, new Box("x"));
        assertEquals(index.get(topic).size(), 2);

        // Remove exact instance
        index.remove(topic, b1);
        Set<Box> res = index.get(topic);
        assertEquals(res.size(), 1);
        assertFalse(res.contains(b1));
        assertTrue(res.contains(b2));
    }

    @Test
    public void testMatchWildcardWithIdentityValues() {
        Box b1 = new Box("x");
        Box b2 = new Box("x");
        index.add("a/b", b1);
        index.add("a/c", b2);
        Set<Box> res = index.match("a/#");
        // Both should be present; identity does not affect branch selection
        assertTrue(res.containsAll(List.of(b1, b2)));
        assertEquals(res.size(), 2);
    }

    private record Box(String v) {

        @Override
        public boolean equals(Object o) {
            return o instanceof Box b && v.equals(b.v);
        }
    }
}

