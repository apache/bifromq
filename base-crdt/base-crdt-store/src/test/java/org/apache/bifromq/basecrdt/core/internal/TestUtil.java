/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.basecrdt.core.internal;

import static org.testng.Assert.assertEquals;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

public class TestUtil {
    public static <E> void assertSame(Iterator<E> a, Iterator<E> b) {
        while (a.hasNext() && b.hasNext()) {
            assertEquals(b.next(), a.next());
        }
        assertEquals(b.hasNext(), a.hasNext());
    }

    public static <E> void assertUnorderedSame(Iterator<E> a, Iterator<E> b) {
        Set<E> aSet = Sets.newHashSet();
        Set<E> bSet = Sets.newHashSet();
        a.forEachRemaining(aSet::add);
        b.forEachRemaining(bSet::add);
        assertEquals(bSet, aSet);
    }

    public static Map<ByteString, NavigableMap<Long, Long>> toLatticeEvents(ByteString replicaId, long... boundaries) {
        Map<ByteString, NavigableMap<Long, Long>> histories = Maps.newHashMap();
        NavigableMap<Long, Long> ranges = histories.computeIfAbsent(replicaId, k -> Maps.newTreeMap());
        for (int i = 0; i < boundaries.length; i = i + 2) {
            ranges.put(boundaries[i], boundaries[i + 1]);
        }
        return histories;
    }
}
