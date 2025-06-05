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

package org.apache.bifromq.basecrdt.core.internal;

import static org.apache.bifromq.basecrdt.core.internal.ProtoUtils.dot;
import static org.apache.bifromq.basecrdt.core.internal.ProtoUtils.singleValue;
import static org.apache.bifromq.basecrdt.core.internal.TestUtil.assertUnorderedSame;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

public class DotFuncTest {
    private final ByteString replicaA = copyFromUtf8("A");
    private final ByteString replicaB = copyFromUtf8("B");

    private final ByteString val1 = copyFromUtf8("val1");
    private final ByteString val2 = copyFromUtf8("val2");


    @Test
    public void testAdd() {
        DotFunc dotFunc = new DotFunc();
        assertTrue(dotFunc.isBottom());
        assertFalse(dotFunc.iterator().hasNext());
        assertFalse(dotFunc.values().iterator().hasNext());

        assertTrue(dotFunc.add(singleValue(replicaA, 1, val1)));
        assertTrue(dotFunc.add(singleValue(replicaB, 1, val2)));
        assertFalse(dotFunc.isBottom());
        assertEquals(Sets.newHashSet(dotFunc), Sets.newHashSet(dot(replicaA, 1), dot(replicaB, 1)));
        assertUnorderedSame(Lists.newArrayList(val1, val2).iterator(), dotFunc.values().iterator());
    }

    @Test
    public void testAddSameDotWithDifferentVal() {
        DotFunc dotFunc = new DotFunc();

        dotFunc.add(singleValue(replicaA, 1, val1));
        assertFalse(dotFunc.add(singleValue(replicaA, 1, val2)));
        assertEquals(Sets.newHashSet(dotFunc), Sets.newHashSet(dot(replicaA, 1)));
        assertUnorderedSame(Lists.<ByteString>newArrayList(val1).iterator(), dotFunc.values().iterator());
    }

    @Test
    public void testAddDifferentDotWithSameVal() {
        DotFunc dotFunc = new DotFunc();

        dotFunc.add(singleValue(replicaA, 1, val1));
        assertFalse(dotFunc.add(singleValue(replicaA, 1, val1)));
        assertEquals(Sets.newHashSet(dotFunc), Sets.newHashSet(dot(replicaA, 1)));
        assertUnorderedSame(Lists.newArrayList(val1, val1).iterator(), dotFunc.values().iterator());
    }

    @Test
    public void testRemove() {
        DotFunc dotFunc = new DotFunc();

        dotFunc.add(singleValue(replicaA, 1, val1));

        // value is not match
        assertFalse(dotFunc.remove(singleValue(replicaA, 1, val2)));
        assertFalse(dotFunc.isBottom());

        assertTrue(dotFunc.remove(singleValue(replicaA, 1, val1)));
        assertTrue(dotFunc.isBottom());
    }
}
