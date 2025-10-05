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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

/**
 * BranchTable containing single entry.
 */
final class SingleEntryBranchTable<V> implements BranchTable<V> {
    private final String key;
    private final Branch<V> value;

    SingleEntryBranchTable(String key, Branch<V> value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public Branch<V> get(String key) {
        return Objects.equals(this.key, key) ? value : null;
    }

    @Override
    public BranchTable<V> plus(String key, Branch<V> value) {
        if (Objects.equals(this.key, key)) {
            if (this.value == value) {
                return this;
            }
            return new SingleEntryBranchTable<>(key, value);
        }
        // upgrade to PMap
        PMap<String, Branch<V>> m = HashTreePMap.empty();
        m = m.plus(this.key, this.value);
        m = m.plus(key, value);
        return new PMapBranchTable<>(m);
    }

    @Override
    public BranchTable<V> minus(String key) {
        if (Objects.equals(this.key, key)) {
            return EmptyBranchTable.empty();
        }
        return this;
    }

    @Override
    public void forEach(BiConsumer<String, Branch<V>> consumer) {
        consumer.accept(key, value);
    }

    @Override
    public Map<String, Branch<V>> asMapView() {
        return Collections.singletonMap(key, value);
    }
}

