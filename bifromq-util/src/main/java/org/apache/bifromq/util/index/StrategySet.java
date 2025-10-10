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

import java.util.AbstractSet;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Strategy-backed mutable set used by lookup to preserve strategy semantics.
 */
final class StrategySet<T> extends AbstractSet<T> {
    private final ValueStrategy<T> strategy;
    private final HashSet<ValueRef<T>> refs = new HashSet<>();

    StrategySet(ValueStrategy<T> strategy) {
        this.strategy = strategy;
    }

    @Override
    public boolean add(T t) {
        return refs.add(ValueRef.of(t, strategy));
    }

    @Override
    public boolean contains(Object o) {
        @SuppressWarnings("unchecked")
        T t = (T) o;
        return refs.contains(ValueRef.of(t, strategy));
    }

    @Override
    public boolean remove(Object o) {
        @SuppressWarnings("unchecked")
        T t = (T) o;
        return refs.remove(ValueRef.of(t, strategy));
    }

    @Override
    public Iterator<T> iterator() {
        Iterator<ValueRef<T>> it = refs.iterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public T next() {
                return it.next().value;
            }
        };
    }

    @Override
    public int size() {
        return refs.size();
    }
}

