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

package org.apache.bifromq.dist.worker.schema;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Compares filter levels and tenant sort keys in storage order.
 */
public final class RouteKeyOrder {
    public static final Comparator<String> TOPIC_LEVEL_ORDER = (left, right) -> {
        if (left.isEmpty() || right.isEmpty()) {
            return Boolean.compare(left.isEmpty(), right.isEmpty());
        }
        return compareUtf8(left, right);
    };

    private RouteKeyOrder() {
    }

    /**
     * Prefix the tenant ID with two characters that hold its UTF-8 length bytes.
     * The returned string is an internal sort key, not a tenant ID.
     */
    public static String tenantIdOrderKey(String tenantId) {
        int length = utf8Length(tenantId);
        return "" + (char) ((length >>> 8) & 0xFF) + (char) (length & 0xFF) + tenantId;
    }

    /**
     * Copy global filter levels and replace the tenant ID with its internal sort key.
     */
    public static List<String> orderedGlobalFilterLevels(List<String> levels) {
        List<String> ordered = new ArrayList<>(levels);
        ordered.set(0, tenantIdOrderKey(levels.get(0)));
        return ordered;
    }

    public static int compareLevels(Iterable<String> left, Iterable<String> right) {
        Iterator<String> leftLevels = left.iterator();
        Iterator<String> rightLevels = right.iterator();
        while (leftLevels.hasNext() && rightLevels.hasNext()) {
            int cmp = TOPIC_LEVEL_ORDER.compare(leftLevels.next(), rightLevels.next());
            if (cmp != 0) {
                return cmp;
            }
        }
        return Boolean.compare(leftLevels.hasNext(), rightLevels.hasNext());
    }

    private static int compareUtf8(String left, String right) {
        if (left == right) {
            return 0;
        }
        int i = 0;
        int j = 0;
        while (i < left.length() && j < right.length()) {
            int leftCodePoint = encodedCodePoint(left, i);
            int rightCodePoint = encodedCodePoint(right, j);
            if (leftCodePoint != rightCodePoint) {
                return Integer.compare(leftCodePoint, rightCodePoint);
            }
            i += Character.charCount(leftCodePoint);
            j += Character.charCount(rightCodePoint);
        }
        return Boolean.compare(i < left.length(), j < right.length());
    }

    private static int utf8Length(String value) {
        int length = 0;
        for (int i = 0; i < value.length();) {
            int codePoint = encodedCodePoint(value, i);
            length += codePoint < 0x80 ? 1 : codePoint < 0x800 ? 2 : codePoint < 0x10000 ? 3 : 4;
            i += Character.charCount(codePoint);
        }
        return length;
    }

    private static int encodedCodePoint(String value, int index) {
        int codePoint = value.codePointAt(index);
        // String.getBytes(UTF_8) replaces each unpaired surrogate with '?'.
        return codePoint >= Character.MIN_SURROGATE && codePoint <= Character.MAX_SURROGATE ? '?' : codePoint;
    }
}
