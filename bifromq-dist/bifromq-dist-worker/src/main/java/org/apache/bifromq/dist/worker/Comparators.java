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

import org.apache.bifromq.type.RouteMatcher;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Comparators for RouteMatcher.
 */
public class Comparators {
    public static final Comparator<Iterable<String>> FilterLevelsComparator = (l1, l2) -> {
        Iterator<String> it1 = l1.iterator();
        Iterator<String> it2 = l2.iterator();
        while (it1.hasNext() && it2.hasNext()) {
            int cmp = it1.next().compareTo(it2.next());
            if (cmp != 0) {
                return cmp;
            }
        }
        if (it1.hasNext()) {
            return 1;
        } else if (it2.hasNext()) {
            return -1;
        }
        return 0;
    };
    public static final Comparator<RouteMatcher> RouteMatcherComparator =
        (tf1, tf2) -> FilterLevelsComparator.compare(tf1.getFilterLevelList(), tf2.getFilterLevelList());
}
