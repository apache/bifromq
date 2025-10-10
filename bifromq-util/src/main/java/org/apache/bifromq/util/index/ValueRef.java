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
 * Wrapper that delegates hash/equals to a ValueStrategy while carrying the original value.
 */
final class ValueRef<V> {
    final V value;
    private final ValueStrategy<V> strategy;
    private final int hash;

    private ValueRef(V value, ValueStrategy<V> strategy) {
        this.value = value;
        this.strategy = strategy;
        this.hash = strategy.hash(value);
    }

    static <T> ValueRef<T> of(T value, ValueStrategy<T> strategy) {
        return new ValueRef<>(value, strategy);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ValueRef<?> other)) {
            return false;
        }
        @SuppressWarnings("unchecked")
        ValueRef<V> o = (ValueRef<V>) other;
        if (this.strategy != o.strategy) {
            return false; // must be same strategy instance
        }
        return strategy.equivalent(this.value, o.value);
    }
}

