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
import java.util.Iterator;
import java.util.Set;
import lombok.ToString;
import org.pcollections.HashTreePSet;
import org.pcollections.PSet;

@ToString
public class Branch<V> {
    final INode<V> iNode;
    private final ValueStrategy<V> strategy;
    PSet<ValueRef<V>> values;

    Branch(INode<V> iNode, ValueStrategy<V> strategy) {
        this.iNode = iNode;
        this.strategy = strategy;
        this.values = HashTreePSet.empty();
    }

    Branch(V value, ValueStrategy<V> strategy) {
        this.iNode = null;
        this.strategy = strategy;
        this.values = HashTreePSet.empty();
        this.values = this.values.plus(ValueRef.of(value, strategy));
    }

    private Branch(INode<V> iNode, PSet<ValueRef<V>> values, ValueStrategy<V> strategy) {
        this.iNode = iNode;
        this.values = values;
        this.strategy = strategy;
    }

    Branch<V> updated(INode<V> iNode) {
        return new Branch<>(iNode, values, strategy);
    }

    Branch<V> updated(V value) {
        PSet<ValueRef<V>> newSubs = values;
        newSubs = newSubs.plus(ValueRef.of(value, strategy));
        return new Branch<>(iNode, newSubs, strategy);
    }

    Branch<V> removed(V sub) {
        PSet<ValueRef<V>> newSubs = values;
        newSubs = newSubs.minus(ValueRef.of(sub, strategy));
        return new Branch<>(iNode, newSubs, strategy);
    }

    Set<V> values() {
        // lightweight view without copying
        return new AbstractSet<>() {
            @Override
            public Iterator<V> iterator() {
                Iterator<ValueRef<V>> it = values.iterator();
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public V next() {
                        return it.next().value;
                    }
                };
            }

            @Override
            public int size() {
                return values.size();
            }

            @Override
            public boolean contains(Object o) {
                @SuppressWarnings("unchecked")
                V v = (V) o;
                return values.contains(ValueRef.of(v, strategy));
            }
        };
    }

    boolean contains(V value) {
        return values.contains(ValueRef.of(value, strategy));
    }
}
