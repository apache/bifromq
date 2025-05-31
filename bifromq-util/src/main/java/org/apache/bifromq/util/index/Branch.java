/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.util.index;

import java.util.Set;
import org.pcollections.HashTreePSet;
import org.pcollections.PSet;

public class Branch<V> {
    final INode<V> iNode;
    PSet<V> values;

    Branch(INode<V> iNode) {
        this.iNode = iNode;
        this.values = HashTreePSet.empty();
    }

    Branch(V value) {
        this.iNode = null;
        this.values = HashTreePSet.empty();
        this.values = this.values.plus(value);
    }

    private Branch(INode<V> iNode, PSet<V> values) {
        this.iNode = iNode;
        this.values = values;
    }

    Branch<V> updated(INode<V> iNode) {
        return new Branch<>(iNode, values);
    }

    Branch<V> updated(V value) {
        PSet<V> newSubs = values;
        newSubs = newSubs.plus(value);
        return new Branch<>(iNode, newSubs);
    }

    Branch<V> removed(V sub) {
        PSet<V> newSubs = values;
        newSubs = newSubs.minus(sub);
        return new Branch<>(iNode, newSubs);
    }

    Set<V> values() {
        return values;
    }
}
