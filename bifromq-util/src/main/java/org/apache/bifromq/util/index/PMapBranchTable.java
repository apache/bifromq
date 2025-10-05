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

import java.util.Map;
import java.util.function.BiConsumer;
import org.pcollections.PMap;

/**
 * BranchTable backed by a single PMap.
 */
final class PMapBranchTable<V> implements BranchTable<V> {
    private final PMap<String, Branch<V>> map;

    PMapBranchTable(PMap<String, Branch<V>> map) {
        this.map = map;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public Branch<V> get(String key) {
        return map.get(key);
    }

    @Override
    public BranchTable<V> plus(String key, Branch<V> value) {
        PMap<String, Branch<V>> nm = map.plus(key, value);
        if (nm == map) {
            return this;
        }
        return new PMapBranchTable<>(nm);
    }

    @Override
    public BranchTable<V> minus(String key) {
        PMap<String, Branch<V>> nm = map.minus(key);
        if (nm == map) {
            return this;
        }
        return new PMapBranchTable<>(nm);
    }

    @Override
    public void forEach(BiConsumer<String, Branch<V>> consumer) {
        for (Map.Entry<String, Branch<V>> e : map.entrySet()) {
            consumer.accept(e.getKey(), e.getValue());
        }
    }

    @Override
    public Map<String, Branch<V>> asMapView() {
        return map;
    }
}

