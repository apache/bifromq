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

import static org.apache.bifromq.basecrdt.core.internal.ProtoUtils.dot;
import static org.apache.bifromq.basecrdt.core.internal.ProtoUtils.singleDot;
import static org.apache.bifromq.basecrdt.core.internal.ProtoUtils.singleMap;
import static org.apache.bifromq.basecrdt.core.internal.ProtoUtils.singleValue;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.Optional;
import org.testng.annotations.Test;

public class DotMapTest {
    private final ByteString replicaA = copyFromUtf8("A");
    private final ByteString replicaB = copyFromUtf8("B");
    private final ByteString key1 = copyFromUtf8("key1");
    private final ByteString key2 = copyFromUtf8("key2");
    private final ByteString key3 = copyFromUtf8("key3");
    private final ByteString key4 = copyFromUtf8("key4");
    private final ByteString val1 = copyFromUtf8("val1");
    private final ByteString val2 = copyFromUtf8("val2");

    @Test
    public void testAddDotSet() {
        DotMap dotMap = new DotMap();
        assertTrue(dotMap.isBottom());
        assertFalse(dotMap.dotSetKeys().hasNext());
        assertFalse(dotMap.iterator().hasNext());

        assertTrue(dotMap.add(singleMap(key1, singleDot(replicaA, 1))));
        assertFalse(dotMap.isBottom());
        TestUtil.assertUnorderedSame(Sets.<ByteString>newHashSet(key1).iterator(), dotMap.dotSetKeys());
        assertEquals(Sets.newHashSet(dotMap), Sets.newHashSet(dot(replicaA, 1)));
    }

    @Test
    public void testRemoveDotSet() {
        DotMap dotMap = new DotMap();
        dotMap.add(singleMap(key1, singleDot(replicaA, 1)));

        // remove non-exist dot
        assertFalse(dotMap.remove(singleMap(key1, singleDot(replicaA, 2))));
        assertFalse(dotMap.isBottom());

        assertTrue(dotMap.remove(singleMap(key1, singleDot(replicaA, 1))));
        assertTrue(dotMap.isBottom());
        assertFalse(dotMap.dotSetKeys().hasNext());
        assertFalse(dotMap.iterator().hasNext());
    }

    @Test
    public void testAddSubDotSet() {
        DotMap dotMap = new DotMap();
        assertTrue(dotMap.isBottom());
        assertFalse(dotMap.dotSetKeys().hasNext());
        assertFalse(dotMap.iterator().hasNext());

        assertTrue(dotMap.add(singleMap(key1, singleMap(key2, singleDot(replicaA, 1)))));
        assertFalse(dotMap.isBottom());
        assertFalse(dotMap.dotSetKeys().hasNext());
        TestUtil.assertUnorderedSame(Sets.<ByteString>newHashSet(key1).iterator(), dotMap.dotMapKeys());
        TestUtil.assertUnorderedSame(Sets.newHashSet(dot(replicaA, 1)).iterator(), dotMap.iterator());

        Optional<IDotSet> subDotSet = dotMap.subDotSet(key1, key2);
        assertTrue(subDotSet.isPresent());
    }

    @Test
    public void testRemoveSubDotSet() {
        DotMap dotMap = new DotMap();
        dotMap.add(singleMap(key1, singleMap(key2, singleDot(replicaA, 1))));

        // remove non-exist dot
        dotMap.remove(singleMap(key1, singleMap(key2, singleDot(replicaA, 2))));
        assertFalse(dotMap.isBottom());

        // remove non-exist dot
        dotMap.remove(singleMap(key1, singleDot(replicaA, 1)));
        assertFalse(dotMap.isBottom());

        assertTrue(dotMap.remove(singleMap(key1, singleMap(key2, singleDot(replicaA, 1)))));
        assertTrue(dotMap.isBottom());
    }

    @Test
    public void testAddDotFunc() {
        DotMap dotMap = new DotMap();
        assertTrue(dotMap.isBottom());
        assertFalse(dotMap.dotFuncKeys().hasNext());
        assertFalse(dotMap.iterator().hasNext());

        dotMap.add(singleMap(key1, singleValue(replicaA, 1, val1)));
        assertFalse(dotMap.isBottom());
        TestUtil.assertUnorderedSame(Sets.<ByteString>newHashSet(key1).iterator(), dotMap.dotFuncKeys());
        TestUtil.assertUnorderedSame(Sets.newHashSet(dot(replicaA, 1)).iterator(), dotMap.iterator());
    }

    @Test
    public void testRemoveDotFunc() {
        DotMap dotMap = new DotMap();
        dotMap.add(singleMap(key1, singleValue(replicaA, 1, val1)));

        // remove non-exist dot
        dotMap.remove(singleMap(key1, singleValue(replicaA, 2, val1)));
        assertFalse(dotMap.isBottom());

        dotMap.remove(singleMap(key1, singleValue(replicaA, 1, val2)));
        assertFalse(dotMap.isBottom());

        dotMap.remove(singleMap(key1, singleValue(replicaA, 1, val1)));
        assertTrue(dotMap.isBottom());
        assertFalse(dotMap.dotFuncKeys().hasNext());
        assertFalse(dotMap.iterator().hasNext());
    }

    @Test
    public void testAddSubDotFunc() {
        DotMap dotMap = new DotMap();
        assertTrue(dotMap.isBottom());
        assertFalse(dotMap.dotFuncKeys().hasNext());
        assertFalse(dotMap.iterator().hasNext());

        dotMap.add(singleMap(key1, singleMap(key2, singleValue(replicaA, 1, val1))));
        assertFalse(dotMap.isBottom());
        assertFalse(dotMap.dotFuncKeys().hasNext());
        TestUtil.assertUnorderedSame(Sets.<ByteString>newHashSet(key1).iterator(), dotMap.dotMapKeys());
        TestUtil.assertUnorderedSame(Sets.newHashSet(dot(replicaA, 1)).iterator(), dotMap.iterator());

        Optional<IDotFunc> subDotFunc = dotMap.subDotFunc(key1, key2);
        assertTrue(subDotFunc.isPresent());
    }

    @Test
    public void testRemoveSubDotFunc() {
        DotMap dotMap = new DotMap();
        dotMap.add(singleMap(key1, singleMap(key2, singleValue(replicaA, 1, val1))));

        // remove non-exist dot
        dotMap.remove(singleMap(key1, singleMap(key2, singleValue(replicaA, 2, val1))));
        assertFalse(dotMap.isBottom());

        // remove non-exist dot
        dotMap.remove(singleMap(key1, singleValue(replicaA, 1, val1)));
        assertFalse(dotMap.isBottom());

        dotMap.remove(singleMap(key1, singleMap(key2, singleValue(replicaA, 1, val1))));
        assertTrue(dotMap.isBottom());
    }

    @Test
    public void testSubDotMap() {
        DotMap dotMap = new DotMap();
        dotMap.add(singleMap(key1, singleMap(key2, singleDot(replicaA, 1))));
        dotMap.add(singleMap(key1, singleMap(key2, singleMap(key3, singleDot(replicaA, 2)))));
        dotMap.add(singleMap(key1, singleMap(key3, singleValue(replicaB, 1, val2))));
        dotMap.add(singleMap(key1, singleMap(key3, singleMap(key4, singleValue(replicaB, 2, val2)))));
        TestUtil.assertUnorderedSame(Sets.newHashSet(dot(replicaA, 1), dot(replicaA, 2),
            dot(replicaB, 1), dot(replicaB, 2)).iterator(), dotMap.iterator());
        TestUtil.assertUnorderedSame(Sets.<ByteString>newHashSet(key1).iterator(), dotMap.dotMapKeys());
        TestUtil.assertUnorderedSame(Sets.newHashSet(key2, key3).iterator(), dotMap.dotMapKeys(key1));

        dotMap.remove(singleMap(key1, singleMap(key2, singleDot(replicaA, 1))));
        assertFalse(dotMap.isBottom());
        TestUtil.assertUnorderedSame(Sets.newHashSet(dot(replicaA, 2), dot(replicaB, 1), dot(replicaB, 2)).iterator(),
            dotMap.iterator());

        dotMap.remove(singleMap(key1, singleMap(key2, singleMap(key3, singleDot(replicaA, 2)))));
        assertFalse(dotMap.isBottom());
        TestUtil.assertUnorderedSame(Sets.newHashSet(dot(replicaB, 1), dot(replicaB, 2)).iterator(),
            dotMap.iterator());

        dotMap.remove(singleMap(key1, singleMap(key3, singleValue(replicaB, 1, val2))));
        assertFalse(dotMap.isBottom());
        TestUtil.assertUnorderedSame(Sets.newHashSet(dot(replicaB, 2)).iterator(), dotMap.iterator());

        dotMap.remove(singleMap(key1, singleMap(key3, singleMap(key4, singleValue(replicaB, 2, val2)))));
        assertTrue(dotMap.isBottom());
    }
}
