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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.apache.bifromq.type.RouteMatcher;
import java.util.Arrays;
import org.testng.annotations.Test;

public class ComparatorsTest {
    @Test
    public void testFilterLevelsComparatorEqual() {
        Iterable<String> list1 = Arrays.asList("a", "b", "c");
        Iterable<String> list2 = Arrays.asList("a", "b", "c");
        int cmp = Comparators.FilterLevelsComparator.compare(list1, list2);
        assertEquals(0, cmp);
    }

    @Test
    public void testFilterLevelsComparatorDifferent() {
        Iterable<String> list1 = Arrays.asList("a", "b", "c");
        Iterable<String> list2 = Arrays.asList("a", "b", "d");
        int cmp = Comparators.FilterLevelsComparator.compare(list1, list2);
        assertTrue(cmp < 0);
    }

    @Test
    public void testFilterLevelsComparatorPrefix() {
        Iterable<String> list1 = Arrays.asList("a", "b");
        Iterable<String> list2 = Arrays.asList("a", "b", "c");
        int cmp = Comparators.FilterLevelsComparator.compare(list1, list2);
        assertTrue(cmp < 0);

        cmp = Comparators.FilterLevelsComparator.compare(list2, list1);
        assertTrue(cmp > 0);
    }

    @Test
    public void testRouteMatcherComparatorEqual() {
        RouteMatcher matcher1 =
            RouteMatcher.newBuilder().setType(RouteMatcher.Type.Normal).addFilterLevel("a").addFilterLevel("b")
                .setMqttTopicFilter("test").build();
        RouteMatcher matcher2 =
            RouteMatcher.newBuilder().setType(RouteMatcher.Type.Normal).addFilterLevel("a").addFilterLevel("b")
                .setMqttTopicFilter("test-different").build();
        int cmp = Comparators.RouteMatcherComparator.compare(matcher1, matcher2);
        assertEquals(0, cmp);
    }

    @Test
    public void testRouteMatcherComparatorDifferent() {
        RouteMatcher matcher1 =
            RouteMatcher.newBuilder().setType(RouteMatcher.Type.Normal).addFilterLevel("a").addFilterLevel("b").build();
        RouteMatcher matcher2 =
            RouteMatcher.newBuilder().setType(RouteMatcher.Type.Normal).addFilterLevel("a").addFilterLevel("c").build();
        int cmp = Comparators.RouteMatcherComparator.compare(matcher1, matcher2);
        assertTrue(cmp < 0);
    }
}
