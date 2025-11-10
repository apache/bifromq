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

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import org.apache.bifromq.basekv.proto.Boundary;

/**
 * Utility class for Boundary calculation.
 *
 * <p>
 * Boundary represents a range in a key space defined over byte strings. Boundaries are based on the lexicographical
 * (unsigned) order of byte strings and are used to partition the key space.
 * </p>
 *
 * <p>
 * There are four forms of boundaries:
 * </p>
 * <ul>
 *   <li>
 *     <b>Left Open</b>: Represented as <code>(null, endKey]</code>. In this case the
 *     <code>startKey</code> is <code>null</code> (denoting negative infinity) and the boundary
 *     includes the <code>endKey</code> (which must be non-null).
 *   </li>
 *   <li>
 *     <b>Right Open</b>: Represented as <code>[startKey, null)</code>. Here the
 *     <code>startKey</code> is non-null and the <code>endKey</code> is <code>null</code>
 *     (denoting positive infinity). The boundary includes the <code>startKey</code> but excludes the end.
 *   </li>
 *   <li>
 *     <b>Full Open</b>: Represented as <code>(null, null)</code>, meaning that both ends are unbounded,
 *     thus covering the entire key space.
 *   </li>
 *   <li>
 *     <b>Left Closed, Right Open</b>: Represented as <code>[startKey, endKey)</code>. In this case,
 *     both <code>startKey</code> and <code>endKey</code> are non-null, the boundary includes the
 *     <code>startKey</code> but excludes the <code>endKey</code>.
 *   </li>
 * </ul>
 *
 * <p>
 * In all legal boundary definitions, the condition <code>startKey &lt; endKey</code> must be met
 * (using unsigned lexicographical comparison). A boundary where <code>startKey != null</code> and
 * <code>startKey.equals(endKey)</code> is considered illegal.
 * </p>
 *
 * <p>
 * The special value <code>null</code> is used to denote an open (unbounded) end:
 * </p>
 * <ul>
 *   <li>
 *     When used as a <code>startKey</code>, <code>null</code> denotes negative infinity; that is,
 *     <code>null</code> is considered less than any actual byte string (including the empty byte string).
 *   </li>
 *   <li>
 *     When used as an <code>endKey</code>, <code>null</code> denotes positive infinity; that is,
 *     <code>null</code> is considered greater than any actual byte string.
 *   </li>
 * </ul>
 *
 * <p>
 * With this convention, a full open boundary <code>(null, null)</code> logically represents an interval where
 * <code>null &lt; anyByteString &lt; null</code> (with the first <code>null</code> as the lower bound and the second as the upper bound).
 * </p>
 *
 * <p>
 * A special boundary, termed <b>NULL_BOUNDARY</b>, is defined as <code>(null, EMPTY]</code>, where <code>EMPTY</code>
 * denotes the empty byte string. According to the ordering, <code>null &lt; EMPTY</code>, but since the empty byte
 * string is considered smaller than any non-empty byte string, <b>NULL_BOUNDARY</b> does not contain any non-empty
 * byte strings.
 * </p>
 *
 * <p>
 * Moreover, <b>NULL_BOUNDARY</b> is defined to overlap with any other boundary – in other words, in terms of
 * set intersection (the mathematical notion of overlapping), <b>NULL_BOUNDARY</b> can be considered as an empty set
 * that is treated as overlapping with every boundary.
 * </p>
 *
 * <p>
 * Note: Methods such as {@link #isOverlap(Boundary, Boundary)} and {@link #intersect(Boundary, Boundary)}
 * assume that the provided boundaries are legal.
 * </p>
 *
 * @see #isOverlap(Boundary, Boundary)
 * @see #intersect(Boundary, Boundary)
 */
public class BoundaryUtil {
    public static final ByteString MIN_KEY = ByteString.EMPTY;
    public static final Boundary NULL_BOUNDARY = Boundary.newBuilder().setEndKey(MIN_KEY).build();
    public static final Boundary FULL_BOUNDARY = Boundary.getDefaultInstance();

    /**
     * Compare two byte arrays lexicographically.
     *
     * @param a byte array a
     * @param b byte array b
     * @return -1 if a < b, 0 if a == b, 1 if a > b
     */
    public static int compare(byte[] a, byte[] b) {
        return compare(unsafeWrap(a), unsafeWrap(b));
    }

    public static int compare(ByteString a, ByteString b) {
        return ByteString.unsignedLexicographicalComparator().compare(a, b);
    }

    /**
     * The comparison is based on the start key and end key of the boundaries, using following rules.
     * <pre>
     * b1 <  b2: b1.startKey < b2.endKey || b1.startKey == b2.startKey && b1.endKey < b2.endKey
     * b1 == b2: b1.startKey == b2.startKey && b1.endKey == b2.endKey
     * b1 >  b2: b1.startKey > b2.startKey || b1.startKey == b2.startKey && b1.endKey > b2.endKey
     * </pre>
     *
     * @param b1 left boundary operand
     * @param b2 right boundary operand
     * @return -1 if b1 < b2, 0 if b1 == b2, 1 if b1 > b2
     */
    public static int compare(Boundary b1, Boundary b2) {
        int startComparison = compareStartKey(startKey(b1), startKey(b2));
        return startComparison != 0 ? startComparison : compareEndKeys(endKey(b1), endKey(b2));
    }

    /**
     * Compare keys if any of the key is a startKey of a boundary, using following rules.
     * <pre>
     * key1 <  key2: (key1 == null && key2 != null) || (key1 != null && key2 != null && key1 < key2)
     * key1 == key2:  key1 == key2
     * key1 >  key2: (key != null && key2 == null)  || (key1 != null && key2 != null && key1 > key2)
     * </pre>
     *
     * @param key1 left key operand
     * @param key2 right key operand
     * @return -1 if key1 < key2, 0 if key1 == key2, 1 if key1 > key2
     */
    public static int compareStartKey(ByteString key1, ByteString key2) {
        if (key1 == null && key2 == null) {
            return 0;
        }
        if (key1 == null) {
            return -1;
        }
        if (key2 == null) {
            return 1;
        }
        return compare(key1, key2);
    }

    /**
     * Compare keys if any of the key is a endKey of a boundary, using following rules.
     * <pre>
     * key1 < key2: (key1 != null && key2 == null) || (key1 != null && key2 != null && key1 < key2)
     * key1 == key2: key1 == key2
     * key1 > key2: (key1 == null && key2 != null) || (key1 != null && key2 != null && key1 > key2)
     * </pre>
     *
     * @param key1 the left key operand
     * @param key2 the right key operand
     * @return -1 if key1 < key2, 0 if key1 == key2, 1 if key1 > key2
     */
    public static int compareEndKeys(ByteString key1, ByteString key2) {
        if (key1 == null && key2 == null) {
            return 0;
        }
        if (key1 == null) {
            return 1;
        }
        if (key2 == null) {
            return -1;
        }
        return compare(key1, key2);
    }

    /**
     * Create a boundary with start key and end key.
     *
     * @param start start key if null means open start
     * @param end   end key if null means open end
     * @return boundary
     */
    public static Boundary toBoundary(ByteString start, ByteString end) {
        Boundary.Builder builder = Boundary.newBuilder();
        if (start != null) {
            builder.setStartKey(start);
        }
        if (end != null) {
            builder.setEndKey(end);
        }
        return builder.build();
    }

    /**
     * Check if the startKey and endKey of a boundary is valid.
     *
     * @param startKey startKey
     * @param endKey   endKey
     * @return true if startKey <= endKey
     */
    public static boolean isValid(ByteString startKey, ByteString endKey) {
        if (startKey == null || endKey == null) {
            return true;
        }
        return compare(startKey, endKey) < 0;
    }

    public static boolean isValid(Boundary boundary) {
        return isValid(startKey(boundary), endKey(boundary));
    }

    /**
     * Check if key is in range of startKey and endKey of a valid boundary.
     *
     * @param key      key
     * @param startKey startKey
     * @param endKey   endKey
     * @return true if key is in range of startKey and endKey
     */
    public static boolean inRange(ByteString key, ByteString startKey, ByteString endKey) {
        assert isValid(startKey, endKey);
        if (startKey != null) {
            if (compare(key, startKey) < 0) {
                return false;
            }
        }
        if (endKey != null) {
            return compare(key, endKey) < 0;
        }
        return true;
    }

    /**
     * Check if key is in range of boundary.
     *
     * @param key      key
     * @param boundary boundary
     * @return true if key is in range of boundary
     */
    public static boolean inRange(ByteString key, Boundary boundary) {
        return inRange(key, startKey(boundary), endKey(boundary));
    }

    /**
     * Check if boundary1 is in range of boundary2.
     *
     * @param boundary1 boundary1
     * @param boundary2 boundary2
     * @return true if boundary1 is in range of boundary2
     */
    public static boolean inRange(Boundary boundary1, Boundary boundary2) {
        return inRange(startKey(boundary1), endKey(boundary1), startKey(boundary2), endKey(boundary2));
    }

    /**
     * Check if boundary1 is in range of boundary2.
     *
     * @param startKey1 startKey of boundary1
     * @param endKey1   endKey of boundary1
     * @param startKey2 startKey of boundary2
     * @param endKey2   endKey of boundary2
     * @return true if boundary1 is in range of boundary2
     */
    public static boolean inRange(ByteString startKey1, ByteString endKey1, ByteString startKey2, ByteString endKey2) {
        assert isValid(startKey1, endKey1) && isValid(startKey2, endKey2);
        if (startKey1 == null && ByteString.EMPTY.equals(endKey1)) {
            return true;
        }
        return compareStartKey(startKey2, startKey1) <= 0 && compareEndKeys(endKey1, endKey2) <= 0;
    }

    /**
     * Get the least upper bound of a key.
     *
     * @param key key
     * @return least upper bound of the key, or null stands for open end
     */
    public static ByteString upperBound(ByteString key) {
        int upperBoundIdx = upperBoundIdx(key::byteAt, key.size());
        if (upperBoundIdx < 0) {
            return null;
        }
        byte[] upper = key.substring(0, upperBoundIdx + 1).toByteArray();
        upper[upperBoundIdx]++;
        return unsafeWrap(upper);
    }

    /**
     * Get the least upper bound of a key.
     *
     * @param key key
     * @return least upper bound of the key, or null stands for open end
     */
    public static byte[] upperBound(byte[] key) {
        int upperBoundIdx = upperBoundIdx(i -> key[i], key.length);
        if (upperBoundIdx < 0) {
            return null;
        }
        byte[] upper = new byte[upperBoundIdx + 1];
        System.arraycopy(key, 0, upper, 0, upperBoundIdx + 1);
        upper[upperBoundIdx]++;
        return upper;
    }

    private static int upperBoundIdx(ByteGetter byteGetter, int size) {
        int i = size;
        if (i == 0) {
            // empty key has open end as upper bound
            return -1;
        }
        while (--i >= 0) {
            byte b = byteGetter.get(i);
            if (Byte.compareUnsigned(b, (byte) 0xFF) < 0) {
                break;
            }
        }
        return i;
    }

    public static ByteString startKey(Boundary boundary) {
        return boundary.hasStartKey() ? boundary.getStartKey() : null;
    }

    public static ByteString endKey(Boundary boundary) {
        return boundary.hasEndKey() ? boundary.getEndKey() : null;
    }

    public static byte[] startKeyBytes(Boundary boundary) {
        return boundary.hasStartKey() ? boundary.getStartKey().toByteArray() : null;
    }

    public static byte[] endKeyBytes(Boundary boundary) {
        return boundary.hasEndKey() ? boundary.getEndKey().toByteArray() : null;
    }

    public static ByteString clampToBoundary(ByteString key, Boundary boundary) {
        if (key == null) {
            return null;
        }
        ByteString startBoundary = BoundaryUtil.startKey(boundary);
        ByteString endBoundary = BoundaryUtil.endKey(boundary);
        if (startBoundary != null && BoundaryUtil.compare(key, startBoundary) < 0) {
            return startBoundary;
        }
        if (endBoundary != null && BoundaryUtil.compare(key, endBoundary) >= 0) {
            return startBoundary;
        }
        return key;
    }

    private static ByteString minStartKey(ByteString a, ByteString b) {
        return (compareStartKey(a, b) < 0) ? a : b;
    }

    private static ByteString maxStartKey(ByteString a, ByteString b) {
        return (compareStartKey(a, b) >= 0) ? a : b;
    }

    private static ByteString minEndKey(ByteString a, ByteString b) {
        return (compareEndKeys(a, b) <= 0) ? a : b;
    }

    private static ByteString maxEndKey(ByteString a, ByteString b) {
        return (compareEndKeys(a, b) >= 0) ? a : b;
    }

    /**
     * Check if two boundaries overlap.
     *
     * @param boundary1 boundary1
     * @param boundary2 boundary2
     * @return true if two boundaries overlap
     */
    public static boolean isOverlap(Boundary boundary1, Boundary boundary2) {
        assert isValid(boundary1);
        assert isValid(boundary2);
        if (isNULLRange(boundary1) || isNULLRange(boundary2)) {
            return true;
        }
        ByteString maxStartKey = maxStartKey(startKey(boundary1), startKey(boundary2));
        ByteString minEndKey = minEndKey(endKey(boundary1), endKey(boundary2));
        return (maxStartKey == null || minEndKey == null) || compare(maxStartKey, minEndKey) < 0;
    }

    public static boolean isValidSplitSet(Set<Boundary> boundaries) {
        if (boundaries.isEmpty()) {
            return false;
        }
        if (boundaries.size() == 1) {
            return boundaries.iterator().next().equals(FULL_BOUNDARY);
        }
        if (boundaries instanceof NavigableSet) {
            return isValidSplitSet((NavigableSet<Boundary>) boundaries);
        }
        NavigableSet<Boundary> sorted = new TreeSet<>(BoundaryUtil::compare);
        sorted.addAll(boundaries);
        return isValidSplitSet(sorted);
    }

    public static boolean isValidSplitSet(NavigableSet<Boundary> sorted) {
        ByteString checkKey = null;
        Iterator<Boundary> iterator = sorted.iterator();
        if (!iterator.hasNext()) {
            return false;
        }
        while (iterator.hasNext()) {
            Boundary boundary = iterator.next();
            if (checkKey == null) {
                if (!boundary.hasStartKey()) {
                    checkKey = boundary.hasEndKey() ? boundary.getEndKey() : null;
                } else {
                    return false;
                }
            } else {
                if (checkKey.equals(boundary.getStartKey())) {
                    if (boundary.hasEndKey()) {
                        checkKey = boundary.getEndKey();
                    } else if (iterator.hasNext()) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    public static Boundary intersect(Boundary boundary1, Boundary boundary2) {
        assert isValid(boundary1);
        assert isValid(boundary2);
        ByteString maxStartKey = maxStartKey(startKey(boundary1), startKey(boundary2));
        ByteString minEndKey = minEndKey(endKey(boundary1), endKey(boundary2));
        if (maxStartKey != null && minEndKey != null && compare(maxStartKey, minEndKey) >= 0) {
            return NULL_BOUNDARY;
        }
        return toBoundary(maxStartKey, minEndKey);
    }

    public static boolean canCombine(Boundary boundary1, Boundary boundary2) {
        assert isValid(boundary1);
        assert isValid(boundary2);
        if (isNULLRange(boundary1) || isNULLRange(boundary2)) {
            // null boundary can combine with any boundary
            return true;
        }
        ByteString maxStartKey = maxStartKey(startKey(boundary1), startKey(boundary2));
        ByteString minEndKey = minEndKey(endKey(boundary1), endKey(boundary2));
        return maxStartKey != null && minEndKey != null && compare(maxStartKey, minEndKey) == 0;
    }

    public static Boundary combine(Boundary... boundaries) {
        assert boundaries.length >= 2;
        Boundary range = boundaries[0];
        for (int i = 1; i < boundaries.length; i++) {
            range = combine2Range(range, boundaries[i]);
        }
        return range;
    }

    /**
     * Check if the boundary can be split into two adjacent non-empty boundaries using provided splitKey.
     *
     * @param boundary boundary to be checked
     * @param splitKey split key
     * @return true if the range is splittable by the split key
     */
    public static boolean isSplittable(Boundary boundary, ByteString splitKey) {
        assert isValid(boundary);
        assert splitKey != null;
        // null boundary is not splittable
        if (isNULLRange(boundary)) {
            return false;
        }
        if (splitKey.equals(MIN_KEY)) {
            return false;
        }
        return compareStartKey(startKey(boundary), splitKey) < 0 && compareEndKeys(splitKey, endKey(boundary)) < 0;
    }

    /**
     * Split a splittable boundary.
     *
     * @param boundary boundary to be split
     * @param splitKey split key
     * @return two boundaries after split
     */
    public static Boundary[] split(Boundary boundary, ByteString splitKey) {
        assert isSplittable(boundary, splitKey);
        Boundary left = boundary.toBuilder().setEndKey(splitKey).build();
        Boundary right = boundary.toBuilder().setStartKey(splitKey).build();
        return new Boundary[] {left, right};
    }

    private static Boundary combine2Range(Boundary boundary1, Boundary boundary2) {
        assert canCombine(boundary1, boundary2);
        if (isNULLRange(boundary1)) {
            return boundary2;
        }
        if (isNULLRange(boundary2)) {
            return boundary1;
        }
        ByteString minStartKey = minStartKey(startKey(boundary1), startKey(boundary2));
        ByteString maxEndKey = maxEndKey(endKey(boundary1), endKey(boundary2));
        return toBoundary(minStartKey, maxEndKey);
    }

    /**
     * Check if the range is NULL. NULL defined as (null, Empty].
     *
     * @param boundary boundary to be checked.
     * @return true if the range is NULL.
     */
    public static boolean isNULLRange(Boundary boundary) {
        return NULL_BOUNDARY.equals(boundary);
    }

    public static boolean isNonEmptyRange(Boundary boundary) {
        return isValid(boundary) && !isNULLRange(boundary);
    }

    private interface ByteGetter {
        byte get(int size);
    }
}
