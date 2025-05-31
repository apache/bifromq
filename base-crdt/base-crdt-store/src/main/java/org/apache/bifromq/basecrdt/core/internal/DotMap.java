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

import static java.util.Collections.emptyIterator;

import org.apache.bifromq.basecrdt.proto.Dot;
import org.apache.bifromq.basecrdt.proto.StateLattice;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

class DotMap extends DotStore implements IDotMap {
    public static final IDotMap BOTTOM = new DotMap();
    private final Map<ByteString, DotSet> dotSetMap = Maps.newConcurrentMap();
    private final Map<ByteString, DotFunc> dotFuncMap = Maps.newConcurrentMap();
    private final Map<ByteString, DotMap> dotMapMap = Maps.newConcurrentMap();

    @Override
    public Optional<IDotSet> subDotSet(ByteString... keys) {
        if (keys.length == 0) {
            throw new IllegalArgumentException("No key specified");
        }
        Optional<DotMap> parentMap = getParentMap(keys);
        return parentMap.map(dots -> dots.dotSetMap.get(keys[keys.length - 1]));
    }

    @Override
    public Optional<IDotFunc> subDotFunc(ByteString... keys) {
        if (keys.length == 0) {
            throw new IllegalArgumentException("No key specified");
        }
        Optional<DotMap> parentMap = getParentMap(keys);
        return parentMap.map(dots -> dots.dotFuncMap.get(keys[keys.length - 1]));
    }

    @Override
    public Optional<IDotMap> subDotMap(ByteString... keys) {
        if (keys.length == 0) {
            throw new IllegalArgumentException("No key specified");
        }
        Optional<DotMap> parentMap = getParentMap(keys);
        return parentMap.map(dots -> dots.dotMapMap.get(keys[keys.length - 1]));
    }

    @Override
    public Iterator<ByteString> dotSetKeys() {
        return dotSetMap.keySet().iterator();
    }

    @Override
    public Iterator<ByteString> dotFuncKeys() {
        return dotFuncMap.keySet().iterator();
    }

    @Override
    public Iterator<ByteString> dotMapKeys(ByteString... keys) {
        if (keys.length == 0) {
            return dotMapMap.keySet().iterator();
        }
        Optional<DotMap> parentMap = getParentMap(keys);
        return parentMap.map(dots -> dots.dotMapMap.get(keys[keys.length - 1]).dotMapMap.keySet().iterator())
            .orElse(emptyIterator());
    }

    @Override
    public boolean isBottom() {
        return dotSetMap.isEmpty() && dotFuncMap.isEmpty() && dotMapMap.isEmpty();
    }

    @Override
    public Iterator<Dot> iterator() {
        return Iterators.concat(
            Iterators.concat(dotSetMap.values().stream().map(DotSet::iterator).iterator()),
            Iterators.concat(dotFuncMap.values().stream().map(DotFunc::iterator).iterator()),
            Iterators.concat(dotMapMap.values().stream().map(DotMap::iterator).iterator())
        );
    }

    @Override
    boolean add(StateLattice addState) {
        assert addState.getStateTypeCase() == StateLattice.StateTypeCase.SINGLEMAP;
        ByteString key = addState.getSingleMap().getKey();
        StateLattice val = addState.getSingleMap().getVal();
        AtomicBoolean inflated = new AtomicBoolean();
        switch (val.getStateTypeCase()) {
            case SINGLEDOT -> dotSetMap.compute(key, (k, v) -> {
                if (v == null) {
                    v = new DotSet();
                }
                inflated.set(v.add(val));
                return v;
            });
            case SINGLEVALUE -> dotFuncMap.compute(key, (k, v) -> {
                if (v == null) {
                    v = new DotFunc();
                }
                inflated.set(v.add(val));
                return v;
            });
            case SINGLEMAP -> dotMapMap.compute(key, (k, v) -> {
                if (v == null) {
                    v = new DotMap();
                }
                inflated.set(v.add(val));
                return v;
            });
        }
        return inflated.get();
    }

    @Override
    boolean remove(StateLattice removeState) {
        assert removeState.getStateTypeCase() == StateLattice.StateTypeCase.SINGLEMAP;
        ByteString key = removeState.getSingleMap().getKey();
        StateLattice val = removeState.getSingleMap().getVal();
        AtomicBoolean inflated = new AtomicBoolean();
        switch (val.getStateTypeCase()) {
            case SINGLEDOT -> dotSetMap.computeIfPresent(key, (k, v) -> {
                inflated.set(v.remove(val));
                if (v.isBottom()) {
                    return null;
                }
                return v;
            });
            case SINGLEVALUE -> dotFuncMap.computeIfPresent(key, (k, v) -> {
                inflated.set(v.remove(val));
                if (v.isBottom()) {
                    return null;
                }
                return v;
            });
            case SINGLEMAP -> dotMapMap.computeIfPresent(key, (k, v) -> {
                inflated.set(v.remove(val));
                if (v.isBottom()) {
                    return null;
                }
                return v;
            });
        }
        return inflated.get();
    }

    @Override
    public String toString() {
        return "DotMap{" +
            "dotSetMap=" + dotSetMap +
            ", dotFuncMap=" + dotFuncMap +
            ", dotMapMap=" + dotMapMap +
            '}';
    }

    private Optional<DotMap> getParentMap(ByteString... keys) {
        DotMap m = this;
        for (int i = 0; i < keys.length - 1; i++) {
            m = m.dotMapMap.get(keys[i]);
            if (m == null) {
                break;
            }
        }
        return Optional.ofNullable(m);
    }
}
