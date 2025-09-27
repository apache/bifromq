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

package org.apache.bifromq.basekv.utils;

import static com.google.protobuf.ByteString.EMPTY;
import static com.google.protobuf.ByteString.copyFrom;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.MIN_KEY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.NULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.compareEndKeys;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.inRange;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.isSplittable;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.split;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.Set;
import org.apache.bifromq.basekv.proto.Boundary;
import org.testng.annotations.Test;

public class BoundaryUtilTest {

    @Test
    public void boundaryValidation() {
        assertTrue(BoundaryUtil.isValid(null, null));
        assertTrue(BoundaryUtil.isValid(null, ByteString.EMPTY));
        assertTrue(BoundaryUtil.isValid(ByteString.EMPTY, null));
        assertFalse(BoundaryUtil.isValid(ByteString.EMPTY, ByteString.EMPTY));

        assertTrue(BoundaryUtil.isValid(ByteString.EMPTY, copyFromUtf8("a")));

        assertFalse(BoundaryUtil.isValid(copyFromUtf8("a"), ByteString.EMPTY));
        assertFalse(BoundaryUtil.isValid(copyFromUtf8("a"), copyFromUtf8("a")));
        assertFalse(BoundaryUtil.isValid(copyFromUtf8("b"), copyFromUtf8("a")));
    }

    @Test
    public void keyInBoundary() {
        Boundary range = Boundary.newBuilder().setStartKey(copyFromUtf8("a")).setEndKey(copyFromUtf8("z")).build();
        assertFalse(inRange(copyFromUtf8("1"), range));
        assertTrue(inRange(copyFromUtf8("a"), range));
        assertFalse(inRange(copyFromUtf8("z"), range));

        assertTrue(inRange(MIN_KEY, null, null));

        assertTrue(inRange(copyFromUtf8("a"), null, null));

        assertTrue(inRange(copyFromUtf8("a"), copyFromUtf8("a"), null));

        assertFalse(inRange(copyFromUtf8("a"), copyFromUtf8("b"), null));

        assertFalse(inRange(copyFromUtf8("a"), null, copyFromUtf8("a")));

        assertFalse(inRange(copyFromUtf8("a"), copyFromUtf8("b"), copyFromUtf8("c")));

        assertTrue(inRange(copyFromUtf8("b"), copyFromUtf8("a"), copyFromUtf8("c")));

        assertFalse(inRange(copyFromUtf8("d"), copyFromUtf8("b"), copyFromUtf8("c")));

        try {
            assertFalse(inRange(copyFromUtf8("b"), copyFromUtf8("c"), copyFromUtf8("a")));
            fail();
        } catch (AssertionError error) {

        }
    }

    @Test
    public void boundaryInBoundary() {

        assertTrue(inRange(copyFromUtf8("a"), copyFromUtf8("b"), null, null));
        assertTrue(inRange(null, EMPTY, null, null));

        assertTrue(inRange(copyFromUtf8("a"), copyFromUtf8("b"), copyFromUtf8("a"), null));
        assertTrue(inRange(null, EMPTY, copyFromUtf8("a"), null));

        assertTrue(inRange(copyFromUtf8("a"), copyFromUtf8("b"), null, copyFromUtf8("b")));
        assertTrue(inRange(null, EMPTY, null, copyFromUtf8("b")));

        assertTrue(inRange(copyFromUtf8("a"), copyFromUtf8("b"), copyFromUtf8("a"), copyFromUtf8("c")));
        assertTrue(inRange(null, EMPTY, copyFromUtf8("a"), copyFromUtf8("c")));

        assertTrue(inRange(copyFromUtf8("b"), copyFromUtf8("c"), copyFromUtf8("a"), copyFromUtf8("c")));

        assertFalse(inRange(copyFromUtf8("a"), copyFromUtf8("c"), copyFromUtf8("a"), copyFromUtf8("b")));

        assertFalse(inRange(copyFromUtf8("a"), copyFromUtf8("c"), copyFromUtf8("b"), copyFromUtf8("d")));

        assertFalse(inRange(copyFromUtf8("b"), copyFromUtf8("d"), copyFromUtf8("a"), copyFromUtf8("c")));

        Boundary _a = boundary(null, "a");
        Boundary _b = boundary(null, "b");
        Boundary a_ = boundary("a", null);
        Boundary b_ = boundary("b", null);
        Boundary a_e = boundary("a", "e");
        Boundary b_d = boundary("b", "d");

        assertTrue(inRange(_a, _a));
        assertTrue(inRange(_a, _b));
        assertFalse(inRange(_b, _a));
        assertTrue(inRange(a_, a_));
        assertFalse(inRange(a_, b_));
        assertTrue(inRange(b_, a_));
        assertFalse(inRange(_a, a_));
        assertTrue(inRange(a_e, a_));
        assertFalse(inRange(a_, a_e));
        assertFalse(inRange(a_e, b_d));
        assertTrue(inRange(b_d, a_e));
        assertTrue(inRange(a_e, a_e));

        assertTrue(inRange(NULL_BOUNDARY, a_e));
        assertFalse(inRange(a_e, NULL_BOUNDARY));

        assertTrue(inRange(NULL_BOUNDARY, FULL_BOUNDARY));
        assertFalse(inRange(FULL_BOUNDARY, NULL_BOUNDARY));

        assertTrue(inRange(a_e, FULL_BOUNDARY));
        assertFalse(inRange(FULL_BOUNDARY, a_e));

        assertTrue(inRange(FULL_BOUNDARY, FULL_BOUNDARY));
        assertTrue(inRange(NULL_BOUNDARY, NULL_BOUNDARY));
    }

    @Test
    public void findUpperBound() {
        ByteString key = MIN_KEY;
        assertNull(upperBound(key));
        assertTrue(compareEndKeys(key, upperBound(key)) < 0);

        key = copyFrom(new byte[] {1, 2, 3});
        assertEquals(upperBound(key), copyFrom(new byte[] {1, 2, 4}));
        assertTrue(compareEndKeys(key, upperBound(key)) < 0);

        key = copyFrom(new byte[] {1, 2, (byte) 0xFF});
        assertEquals(upperBound(key), copyFrom(new byte[] {1, 3}));
        assertTrue(compareEndKeys(key, upperBound(key)) < 0);

        key = copyFrom(new byte[] {1, (byte) 0xFF, (byte) 0xFF});
        assertEquals(upperBound(key), copyFrom(new byte[] {2}));
        assertTrue(compareEndKeys(key, upperBound(key)) < 0);

        key = copyFrom(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        assertNull(upperBound(key));
        assertTrue(compareEndKeys(key, upperBound(key)) < 0);
    }

    @Test
    public void isOverlap() {
        Boundary _a = boundary(null, "a");
        Boundary a_b = boundary("a", "b");
        Boundary b_c = boundary("b", "c");
        Boundary a_e = boundary("a", "e");
        Boundary b_f = boundary("b", "f");
        Boundary f_ = boundary("f", null);
        assertTrue(BoundaryUtil.isOverlap(FULL_BOUNDARY, _a));
        assertTrue(BoundaryUtil.isOverlap(_a, FULL_BOUNDARY));
        assertFalse(BoundaryUtil.isOverlap(a_b, b_c));
        assertFalse(BoundaryUtil.isOverlap(b_c, a_b));
        assertTrue(BoundaryUtil.isOverlap(a_b, FULL_BOUNDARY));
        assertTrue(BoundaryUtil.isOverlap(FULL_BOUNDARY, f_));
        assertTrue(BoundaryUtil.isOverlap(f_, FULL_BOUNDARY));
        assertTrue(BoundaryUtil.isOverlap(a_b, a_e));
        assertTrue(BoundaryUtil.isOverlap(b_f, a_e));
        assertTrue(BoundaryUtil.isOverlap(a_e, b_f));
        // null boundary is overlapped by any boundary
        assertTrue(BoundaryUtil.isOverlap(NULL_BOUNDARY, NULL_BOUNDARY));
        assertTrue(BoundaryUtil.isOverlap(NULL_BOUNDARY, FULL_BOUNDARY));
        assertTrue(BoundaryUtil.isOverlap(FULL_BOUNDARY, NULL_BOUNDARY));
        assertTrue(BoundaryUtil.isOverlap(a_e, NULL_BOUNDARY));
        assertTrue(BoundaryUtil.isOverlap(NULL_BOUNDARY, a_e));
        assertTrue(BoundaryUtil.isOverlap(_a, NULL_BOUNDARY));
        assertTrue(BoundaryUtil.isOverlap(NULL_BOUNDARY, _a));
        assertTrue(BoundaryUtil.isOverlap(f_, NULL_BOUNDARY));
        assertTrue(BoundaryUtil.isOverlap(NULL_BOUNDARY, f_));
    }

    @Test
    public void isValidSplitSet() {
        Boundary _a = boundary(null, "a");
        Boundary a_b = boundary("a", "b");
        Boundary b_c = boundary("b", "c");
        Boundary c_d = boundary("c", "d");
        Boundary d_e = boundary("d", "e");
        Boundary e_ = boundary("e", null);

        // Valid split set with contiguous boundaries
        Set<Boundary> validSplitSet = Set.of(_a, a_b, b_c, c_d, d_e, e_);
        assertTrue(BoundaryUtil.isValidSplitSet(validSplitSet));
        assertFalse(BoundaryUtil.isValidSplitSet(Collections.emptyNavigableSet()));

        // Single boundary that is FULL_BOUNDARY
        Set<Boundary> singleFullBoundary = Set.of(FULL_BOUNDARY);
        assertTrue(BoundaryUtil.isValidSplitSet(singleFullBoundary));

        // Empty boundary set
        Set<Boundary> emptySet = Set.of();
        assertFalse(BoundaryUtil.isValidSplitSet(emptySet));

        // Empty and Full boundary set
        Set<Boundary> emptyAndFull = Set.of(NULL_BOUNDARY, FULL_BOUNDARY);
        assertTrue(BoundaryUtil.isValidSplitSet(emptyAndFull));

        // Single boundary that is not FULL_BOUNDARY
        Set<Boundary> singleNonFullBoundary = Set.of(a_b);
        assertFalse(BoundaryUtil.isValidSplitSet(singleNonFullBoundary));

        Set<Boundary> fullBoundaryWithHole = Set.of(_a, a_b, c_d, e_);
        assertFalse(BoundaryUtil.isValidSplitSet(fullBoundaryWithHole));

        // Set with non-contiguous boundaries
        Set<Boundary> nonContiguousSet = Set.of(a_b, c_d);
        assertFalse(BoundaryUtil.isValidSplitSet(nonContiguousSet));

        // Set with overlap (invalid split set)
        Set<Boundary> overlappingSet = Set.of(a_b, b_c, boundary("a", "c"));
        assertFalse(BoundaryUtil.isValidSplitSet(overlappingSet));
    }

    @Test
    public void intersect() {
        Boundary _a = boundary(null, "a");
        Boundary a_e = boundary("a", "e");
        Boundary b_f = boundary("b", "f");
        Boundary f_g = boundary("f", "g");
        Boundary f_ = boundary("f", null);
        assertEquals(BoundaryUtil.intersect(a_e, a_e), a_e);
        assertEquals(BoundaryUtil.intersect(_a, _a), _a);
        assertEquals(BoundaryUtil.intersect(f_, f_), f_);
        assertEquals(BoundaryUtil.intersect(a_e, b_f), boundary("b", "e"));
        assertEquals(BoundaryUtil.intersect(b_f, a_e), boundary("b", "e"));
        assertEquals(BoundaryUtil.intersect(f_g, FULL_BOUNDARY), boundary("f", "g"));
        assertEquals(BoundaryUtil.intersect(f_g, f_), boundary("f", "g"));
        assertEquals(BoundaryUtil.intersect(_a, FULL_BOUNDARY), _a);
        assertEquals(BoundaryUtil.intersect(f_, FULL_BOUNDARY), f_);
        assertEquals(BoundaryUtil.intersect(_a, NULL_BOUNDARY), NULL_BOUNDARY);
        assertEquals(BoundaryUtil.intersect(f_, NULL_BOUNDARY), NULL_BOUNDARY);
        assertEquals(BoundaryUtil.intersect(_a, a_e), NULL_BOUNDARY);
        assertEquals(BoundaryUtil.intersect(b_f, f_g), NULL_BOUNDARY);
        assertEquals(BoundaryUtil.intersect(NULL_BOUNDARY, FULL_BOUNDARY), NULL_BOUNDARY);
        assertEquals(BoundaryUtil.intersect(NULL_BOUNDARY, NULL_BOUNDARY), NULL_BOUNDARY);
        assertEquals(BoundaryUtil.intersect(FULL_BOUNDARY, FULL_BOUNDARY), FULL_BOUNDARY);
    }

    @Test
    public void inFullBoundary() {
        assertTrue(inRange(copyFromUtf8(""), FULL_BOUNDARY));
        assertTrue(inRange(copyFrom(new byte[0]), FULL_BOUNDARY));
        assertTrue(inRange(copyFromUtf8("abc"), FULL_BOUNDARY));
    }

    @Test
    public void inBounaryWithEmptyStartKey() {
        assertTrue(inRange(copyFromUtf8(""), Boundary.newBuilder().setEndKey(copyFromUtf8("a")).build()));
        assertTrue(inRange(copyFrom(new byte[0]), Boundary.newBuilder().setEndKey(copyFromUtf8("a")).build()));
        assertTrue(inRange(copyFromUtf8("a"), Boundary.newBuilder().setEndKey(copyFromUtf8("b")).build()));
        assertTrue(inRange(copyFromUtf8("a1"), Boundary.newBuilder().setEndKey(copyFromUtf8("b")).build()));
        assertFalse(inRange(copyFromUtf8("b"), Boundary.newBuilder().setEndKey(copyFromUtf8("b")).build()));
    }

    @Test
    public void inBoundaryWithEmptyEndKey() {
        assertFalse(inRange(copyFromUtf8(""), Boundary.newBuilder().setStartKey(copyFromUtf8("a")).build()));
        assertFalse(inRange(copyFrom(new byte[0]), Boundary.newBuilder().setStartKey(copyFromUtf8("a")).build()));
        assertTrue(inRange(copyFromUtf8("a"), Boundary.newBuilder().setStartKey(copyFromUtf8("a")).build()));
        assertFalse(inRange(copyFromUtf8("a"), Boundary.newBuilder().setStartKey(copyFromUtf8("b")).build()));
        assertTrue(inRange(copyFromUtf8("b"), Boundary.newBuilder().setStartKey(copyFromUtf8("b")).build()));
        assertTrue(inRange(copyFromUtf8("b1"), Boundary.newBuilder().setStartKey(copyFromUtf8("b")).build()));
    }

    @Test
    public void canCombineTest() {
        Boundary a_b = Boundary.newBuilder().setStartKey(copyFromUtf8("a")).setEndKey(copyFromUtf8("b")).build();
        Boundary b_c = Boundary.newBuilder().setStartKey(copyFromUtf8("b")).setEndKey(copyFromUtf8("c")).build();
        Boundary d_e = Boundary.newBuilder().setStartKey(copyFromUtf8("d")).setEndKey(copyFromUtf8("e")).build();
        Boundary _a = Boundary.newBuilder().setEndKey(copyFromUtf8("a")).build();
        Boundary a_ = Boundary.newBuilder().setStartKey(copyFromUtf8("a")).build();
        Boundary e_ = Boundary.newBuilder().setStartKey(copyFromUtf8("e")).build();

        assertFalse(BoundaryUtil.canCombine(a_b, a_b));
        assertTrue(BoundaryUtil.canCombine(NULL_BOUNDARY, NULL_BOUNDARY));

        assertTrue(BoundaryUtil.canCombine(a_b, b_c));
        assertTrue(BoundaryUtil.canCombine(b_c, a_b));

        assertFalse(BoundaryUtil.canCombine(a_b, d_e));
        assertFalse(BoundaryUtil.canCombine(d_e, a_b));

        assertTrue(BoundaryUtil.canCombine(a_b, NULL_BOUNDARY));
        assertTrue(BoundaryUtil.canCombine(NULL_BOUNDARY, a_b));

        assertTrue(BoundaryUtil.canCombine(_a, a_b));
        assertTrue(BoundaryUtil.canCombine(a_b, _a));

        assertTrue(BoundaryUtil.canCombine(e_, d_e));
        assertTrue(BoundaryUtil.canCombine(d_e, e_));

        assertTrue(BoundaryUtil.canCombine(_a, a_));
        assertTrue(BoundaryUtil.canCombine(a_, _a));

        assertFalse(BoundaryUtil.canCombine(_a, e_));
        assertFalse(BoundaryUtil.canCombine(e_, _a));

        assertTrue(BoundaryUtil.canCombine(a_, NULL_BOUNDARY));
        assertTrue(BoundaryUtil.canCombine(NULL_BOUNDARY, a_));
    }

    @Test
    public void combineTest() {
        Boundary a_b = boundary("a", "b");
        Boundary b_c = boundary("b", "c");
        Boundary a_c = boundary("a", "c");
        Boundary d_e = boundary("d", "e");
        Boundary _a = boundary(null, "a");
        Boundary _b = boundary(null, "b");
        Boundary _c = boundary(null, "c");
        Boundary d_ = boundary("d", null);
        Boundary a_ = boundary("a", null);
        Boundary e_ = boundary("e", null);

        assertEquals(NULL_BOUNDARY, BoundaryUtil.combine(NULL_BOUNDARY, NULL_BOUNDARY));

        assertEquals(a_c, BoundaryUtil.combine(a_b, b_c));
        assertEquals(a_c, BoundaryUtil.combine(b_c, a_b));

        assertEquals(a_b, BoundaryUtil.combine(a_b, NULL_BOUNDARY));
        assertEquals(a_b, BoundaryUtil.combine(NULL_BOUNDARY, a_b));

        assertEquals(_b, BoundaryUtil.combine(_a, a_b));
        assertEquals(_b, BoundaryUtil.combine(a_b, _a));

        assertEquals(d_, BoundaryUtil.combine(e_, d_e));
        assertEquals(d_, BoundaryUtil.combine(d_e, e_));

        assertEquals(BoundaryUtil.combine(_a, a_), FULL_BOUNDARY);
        assertEquals(BoundaryUtil.combine(a_, _a), FULL_BOUNDARY);

        assertEquals(a_, BoundaryUtil.combine(a_, NULL_BOUNDARY));
        assertEquals(a_, BoundaryUtil.combine(NULL_BOUNDARY, a_));

        assertEquals(_c, BoundaryUtil.combine(_a, a_b, b_c));
        assertEquals(_c, BoundaryUtil.combine(a_b, _a, b_c));
        assertEquals(_c, BoundaryUtil.combine(b_c, a_b, _a));
    }

    @Test
    public void isSplittableTest() {
        assertFalse(isSplittable(boundary(null, null), MIN_KEY));
        assertTrue(isSplittable(boundary(null, null), copyFromUtf8("a")));
        assertFalse(isSplittable(boundary("a", null), copyFromUtf8("a")));
        assertFalse(isSplittable(boundary("a", "b"), copyFromUtf8("a")));
        assertTrue(isSplittable(boundary("a", "b"), copyFromUtf8("aa")));
        assertFalse(isSplittable(boundary("a", "b"), copyFromUtf8("b")));
        assertTrue(isSplittable(boundary(null, "b"), copyFromUtf8("a")));
        assertFalse(isSplittable(boundary(null, "a"), copyFromUtf8("a")));
    }

    @Test
    public void splitTest() {
        Boundary[] boundaries = split(boundary(null, null), copyFromUtf8("a"));
        assertEquals(boundaries[0], boundary(null, "a"));
        assertEquals(boundaries[1], boundary("a", null));

        boundaries = split(boundary("a", "b"), copyFromUtf8("aa"));
        assertEquals(boundaries[0], boundary("a", "aa"));
        assertEquals(boundaries[1], boundary("aa", "b"));

        boundaries = split(boundary(null, "b"), copyFromUtf8("a"));
        assertEquals(boundaries[0], boundary(null, "a"));
        assertEquals(boundaries[1], boundary("a", "b"));
    }

    @Test
    public void compareStartKeyBothNull() {
        ByteString key1 = null;
        ByteString key2 = null;
        assertEquals(BoundaryUtil.compareStartKey(key1, key2), 0);
    }

    @Test
    public void compareStartKeyFirstNull() {
        ByteString key1 = null;
        ByteString key2 = ByteString.copyFromUtf8("a");
        assertEquals(BoundaryUtil.compareStartKey(key1, key2), -1);
    }

    @Test
    public void compareStartKeySecondNull() {
        ByteString key1 = ByteString.copyFromUtf8("a");
        ByteString key2 = null;
        assertEquals(BoundaryUtil.compareStartKey(key1, key2), 1);
    }

    @Test
    public void compareStartKeyBothNonNull() {
        ByteString key1 = ByteString.copyFromUtf8("a");
        ByteString key2 = ByteString.copyFromUtf8("b");
        assertEquals(BoundaryUtil.compareStartKey(key1, key2), -1);
    }

    @Test
    public void compareEndKeysBothNull() {
        ByteString key1 = null;
        ByteString key2 = null;
        assertEquals(BoundaryUtil.compareEndKeys(key1, key2), 0);
    }

    @Test
    public void compareEndKeysFirstNull() {
        ByteString key1 = null;
        ByteString key2 = ByteString.copyFromUtf8("a");
        assertEquals(BoundaryUtil.compareEndKeys(key1, key2), 1);
    }

    @Test
    public void compareEndKeysSecondNull() {
        ByteString key1 = ByteString.copyFromUtf8("a");
        ByteString key2 = null;
        assertEquals(BoundaryUtil.compareEndKeys(key1, key2), -1);
    }

    @Test
    public void compareEndKeysBothNonNull() {
        ByteString key1 = ByteString.copyFromUtf8("a");
        ByteString key2 = ByteString.copyFromUtf8("b");
        assertEquals(BoundaryUtil.compareEndKeys(key1, key2), -1);
    }

    @Test
    public void compareBothStartAndEndKeysEqual() {
        Boundary b1 =
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a")).setEndKey(ByteString.copyFromUtf8("c"))
                .build();
        Boundary b2 =
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a")).setEndKey(ByteString.copyFromUtf8("c"))
                .build();
        assertEquals(BoundaryUtil.compare(b1, b2), 0);
    }

    @Test
    public void compareDifferentStartKeys() {
        Boundary b1 =
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a")).setEndKey(ByteString.copyFromUtf8("c"))
                .build();
        Boundary b2 =
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("b")).setEndKey(ByteString.copyFromUtf8("c"))
                .build();
        assertEquals(BoundaryUtil.compare(b1, b2), -1);
    }

    @Test
    public void compareDifferentEndKeys() {
        Boundary b1 =
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a")).setEndKey(ByteString.copyFromUtf8("b"))
                .build();
        Boundary b2 =
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a")).setEndKey(ByteString.copyFromUtf8("c"))
                .build();
        assertEquals(BoundaryUtil.compare(b1, b2), -1);
    }

    @Test
    public void compareSameStartKeyDifferentEndKey() {
        Boundary b1 =
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a")).setEndKey(ByteString.copyFromUtf8("b"))
                .build();
        Boundary b2 =
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a")).setEndKey(ByteString.copyFromUtf8("c"))
                .build();
        assertEquals(BoundaryUtil.compare(b1, b2), -1);
    }

    @Test
    public void compareOneBoundaryFullOnePartial() {
        Boundary b1 = Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a"))
            .build(); // No end key, meaning it’s open-ended
        Boundary b2 =
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a")).setEndKey(ByteString.copyFromUtf8("c"))
                .build();
        assertEquals(BoundaryUtil.compare(b1, b2), 1);
    }

    @Test
    public void compareNullStartKeyAndEndKey() {
        Boundary b1 = Boundary.newBuilder().build(); // Both startKey and endKey are null
        Boundary b2 =
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a")).setEndKey(ByteString.copyFromUtf8("b"))
                .build();
        assertEquals(BoundaryUtil.compare(b1, b2), -1);
    }

    @Test
    public void compareOneBoundaryOpenEnded() {
        Boundary b1 =
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a")).build(); // End key is null, open-ended
        Boundary b2 =
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a")).setEndKey(ByteString.copyFromUtf8("b"))
                .build();
        assertEquals(BoundaryUtil.compare(b1, b2), 1);
    }

    private Boundary boundary(String startKey, String endKey) {
        return BoundaryUtil.toBoundary(startKey != null ? copyFromUtf8(startKey) : null,
            endKey != null ? copyFromUtf8(endKey) : null);
    }
}
