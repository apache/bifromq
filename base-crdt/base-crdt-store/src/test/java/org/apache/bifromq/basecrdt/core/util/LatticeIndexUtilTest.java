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

package org.apache.bifromq.basecrdt.core.util;

import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import java.util.NavigableMap;
import org.testng.annotations.Test;

public class LatticeIndexUtilTest {
    private final ByteString replicaA = copyFromUtf8("A");
    private final ByteString replicaB = copyFromUtf8("B");

    @Test
    public void testRemember() {
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            LatticeIndexUtil.remember(a, 1, 2);
            LatticeIndexUtil.remember(a, 3, 3);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            LatticeIndexUtil.remember(b, 1, 3);
            assertEquals(b, a);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            LatticeIndexUtil.remember(a, 3, 3);
            LatticeIndexUtil.remember(a, 1, 2);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            LatticeIndexUtil.remember(b, 1, 3);
            assertEquals(b, a);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            LatticeIndexUtil.remember(a, 1, 3);
            LatticeIndexUtil.remember(a, 3, 5);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            LatticeIndexUtil.remember(b, 1, 5);
            assertEquals(b, a);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            LatticeIndexUtil.remember(a, 1, 3);
            LatticeIndexUtil.remember(a, 4, 5);
            LatticeIndexUtil.remember(a, 6, 8);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            LatticeIndexUtil.remember(b, 1, 8);
            assertEquals(b, a);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            LatticeIndexUtil.remember(a, 1, 3);
            LatticeIndexUtil.remember(a, 6, 8);
            LatticeIndexUtil.remember(a, 4, 5);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            LatticeIndexUtil.remember(b, 1, 8);
            assertEquals(b, a);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            LatticeIndexUtil.remember(a, 6, 8);
            LatticeIndexUtil.remember(a, 1, 3);
            LatticeIndexUtil.remember(a, 4, 5);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            LatticeIndexUtil.remember(b, 1, 8);
            assertEquals(b, a);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            LatticeIndexUtil.remember(a, 1, 2);
            LatticeIndexUtil.remember(a, 1, 5);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            LatticeIndexUtil.remember(b, 1, 5);
            assertEquals(b, a);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            LatticeIndexUtil.remember(a, 1, 55);
            LatticeIndexUtil.remember(a, 10, 10);
            LatticeIndexUtil.remember(a, 56, 95);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            LatticeIndexUtil.remember(b, 1, 95);
            assertEquals(b, a);
        }
        {
            NavigableMap<Long, Long> a = Maps.newTreeMap();
            LatticeIndexUtil.remember(a, 1, 44);
            LatticeIndexUtil.remember(a, 14, 14);
            LatticeIndexUtil.remember(a, 17, 17);
            LatticeIndexUtil.remember(a, 21, 22);
            LatticeIndexUtil.remember(a, 24, 24);
            LatticeIndexUtil.remember(a, 45, 96);
            NavigableMap<Long, Long> b = Maps.newTreeMap();
            LatticeIndexUtil.remember(b, 1, 96);
            assertEquals(b, a);
        }
    }
}
