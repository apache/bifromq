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

package org.apache.bifromq.basecrdt.core.internal;

import org.apache.bifromq.basecrdt.proto.Dot;
import org.apache.bifromq.basecrdt.proto.StateLattice;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

class DotFunc extends DotStore implements IDotFunc {
    public static final IDotFunc BOTTOM = new DotFunc();
    private final Map<Dot, ByteString> dots = Maps.newConcurrentMap();

    @Override
    public Iterator<Dot> iterator() {
        return dots.keySet().iterator();
    }

    @Override
    public boolean isBottom() {
        return dots.isEmpty();
    }

    @Override
    public Iterable<ByteString> values() {
        return dots.values();
    }

    @Override
    public Optional<ByteString> value(Dot dot) {
        return Optional.ofNullable(dots.get(dot));
    }

    boolean add(StateLattice addState) {
        assert addState.getStateTypeCase() == StateLattice.StateTypeCase.SINGLEVALUE;
        return dots.putIfAbsent(ProtoUtils.dot(addState.getSingleValue().getReplicaId(),
                addState.getSingleValue().getVer()),
            addState.getSingleValue().getValue()) == null;
    }

    boolean remove(StateLattice removeState) {
        assert removeState.getStateTypeCase() == StateLattice.StateTypeCase.SINGLEVALUE;
        return dots.remove(ProtoUtils.dot(removeState.getSingleValue().getReplicaId(),
                removeState.getSingleValue().getVer()),
            removeState.getSingleValue().getValue());
    }

    @Override
    public String toString() {
        return "DotFunc{dots=" + dots + '}';
    }
}
