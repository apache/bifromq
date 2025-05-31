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

import static org.apache.bifromq.basecrdt.core.internal.EventHistoryUtil.diff;
import static org.apache.bifromq.basecrdt.core.internal.EventHistoryUtil.forget;
import static org.apache.bifromq.basecrdt.core.util.LatticeIndexUtil.remember;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class EventHistoryUtilTest {
    private final ByteString replicaA = copyFromUtf8("A");
    private final ByteString replicaB = copyFromUtf8("B");

    @Test
    public void testCommon() {
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            assertEquals(EventHistoryUtil.common(a, b), c);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(0L, 1L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            assertEquals(EventHistoryUtil.common(a, b), c);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(0L, 1L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            b.put(2L, 3L);
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            assertEquals(EventHistoryUtil.common(a, b), c);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(0L, 1L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            b.put(0L, 1L);
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            c.put(0L, 1L);
            assertEquals(EventHistoryUtil.common(a, b), c);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(0L, 10L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            b.put(0L, 1L);
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            c.put(0L, 1L);
            assertEquals(EventHistoryUtil.common(a, b), c);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(0L, 10L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            b.put(1L, 1L);
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            c.put(1L, 1L);
            assertEquals(EventHistoryUtil.common(a, b), c);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(0L, 5L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            b.put(0L, 10L);
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            c.put(0L, 5L);
            assertEquals(EventHistoryUtil.common(a, b), c);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(0L, 5L);
            a.put(7L, 10L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            b.put(4L, 8L);
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            c.put(4L, 5L);
            c.put(7L, 8L);
            assertEquals(EventHistoryUtil.common(a, b), c);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(0L, 5L);
            a.put(7L, 10L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            b.put(4L, 11L);
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            c.put(4L, 5L);
            c.put(7L, 10L);
            assertEquals(EventHistoryUtil.common(a, b), c);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(4L, 11L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            b.put(0L, 5L);
            b.put(7L, 10L);
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            c.put(4L, 5L);
            c.put(7L, 10L);
            assertEquals(EventHistoryUtil.common(a, b), c);
        }
    }

    @Test
    public void testForget() {
        Map<ByteString, NavigableMap<Long, Long>> historyMap = Maps.newLinkedHashMap();
        remember(historyMap, replicaA, 1);
        remember(historyMap, replicaA, 2);
        remember(historyMap, replicaA, 3);
        remember(historyMap, replicaA, 5);
        remember(historyMap, replicaA, 7);
        remember(historyMap, replicaA, 8);
        remember(historyMap, replicaB, 1);

        forget(historyMap, replicaB, 0);
        assertTrue(historyMap.containsKey(replicaB));

        forget(historyMap, replicaB, 1);
        assertFalse(historyMap.containsKey(replicaB));

        forget(historyMap, replicaA, 1);
        assertFalse(historyMap.get(replicaA).containsKey(1L));

        TreeMap<Long, Long> expected = new TreeMap<>();
        expected.put(2L, 3L);
        expected.put(5L, 5L);
        expected.put(7L, 8L);
        assertEquals(historyMap.get(replicaA), expected);
    }

    @Test
    public void testDiff() {
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            assertEquals(diff(a, b), c);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(0L, 1L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            assertEquals(diff(a, b), a);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(0L, 1L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            b.put(0L, 1L);
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            assertEquals(diff(a, b), c);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(0L, 1L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            b.put(0L, 2L);
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            assertEquals(diff(a, b), c);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(0L, 1L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            b.put(2L, 3L);
            assertEquals(diff(a, b), a);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(0L, 1L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            b.put(1L, 3L);
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            c.put(0L, 0L);
            assertEquals(diff(a, b), c);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(1L, 3L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            b.put(0L, 1L);
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            c.put(2L, 3L);
            assertEquals(diff(a, b), c);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(1L, 3L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            b.put(2L, 4L);
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            c.put(1L, 1L);
            assertEquals(diff(a, b), c);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(2L, 5L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            b.put(1L, 3L);
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            c.put(4L, 5L);
            assertEquals(diff(a, b), c);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(0L, 5L);
            a.put(7L, 10L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            b.put(4L, 11L);
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            c.put(0L, 3L);
            assertEquals(diff(a, b), c);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            a.put(4L, 11L);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            b.put(0L, 5L);
            b.put(7L, 10L);
            NavigableMap<Long, Long> c = Maps.newTreeMap();
            c.put(6L, 6L);
            c.put(11L, 11L);
            assertEquals(diff(a, b), c);
        }
    }
}
