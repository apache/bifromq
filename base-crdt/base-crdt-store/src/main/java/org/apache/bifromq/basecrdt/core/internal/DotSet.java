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

import org.apache.bifromq.basecrdt.proto.Dot;
import org.apache.bifromq.basecrdt.proto.StateLattice;
import com.google.common.collect.Sets;
import java.util.Iterator;
import java.util.Set;

class DotSet extends DotStore implements IDotSet {
    public static final IDotSet BOTTOM = new DotSet();
    private final Set<Dot> dots = Sets.newConcurrentHashSet();

    @Override
    public Iterator<Dot> iterator() {
        return dots.iterator();
    }

    @Override
    public boolean isBottom() {
        return dots.isEmpty();
    }

    @Override
    boolean add(StateLattice addState) {
        assert addState.getStateTypeCase() == StateLattice.StateTypeCase.SINGLEDOT;
        return dots.add(ProtoUtils.dot(addState.getSingleDot().getReplicaId(), addState.getSingleDot().getVer()));
    }

    @Override
    boolean remove(StateLattice removeState) {
        assert removeState.getStateTypeCase() == StateLattice.StateTypeCase.SINGLEDOT;
        return dots.remove(ProtoUtils.dot(removeState.getSingleDot().getReplicaId(),
            removeState.getSingleDot().getVer()));
    }

    @Override
    public String toString() {
        return "DotSet{dots=" + dots + '}';
    }
}
