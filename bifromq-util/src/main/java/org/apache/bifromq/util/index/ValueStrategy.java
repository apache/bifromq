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

package org.apache.bifromq.util.index;

/**
 * Strategy to define hashing and equivalence for values stored in TopicLevelTrie.
 *
 * @param <V> the value type
 */
public interface ValueStrategy<V> {
    /** Compute hash for value. */
    int hash(V value);

    /** Determine equivalence between two values. */
    boolean equivalent(V a, V b);

    /**
     * Default strategy using object's own hashCode/equals.
     */
    static <T> ValueStrategy<T> natural() {
        return new ValueStrategy<>() {
            @Override
            public int hash(T value) {
                return value == null ? 0 : value.hashCode();
            }

            @Override
            public boolean equivalent(T a, T b) {
                if (a == b) {
                    return true;
                }
                if (a == null || b == null) {
                    return false;
                }
                return a.equals(b);
            }
        };
    }

    /**
     * Identity strategy using reference equality and System.identityHashCode.
     */
    static <T> ValueStrategy<T> identity() {
        return new ValueStrategy<>() {
            @Override
            public int hash(T value) {
                return System.identityHashCode(value);
            }

            @Override
            public boolean equivalent(T a, T b) {
                return a == b;
            }
        };
    }
}
