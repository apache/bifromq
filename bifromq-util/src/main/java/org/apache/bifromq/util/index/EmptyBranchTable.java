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
import java.util.function.BiConsumer;

/**
 * BranchTable specialization for empty table.
 */
final class EmptyBranchTable<V> implements BranchTable<V> {
    @SuppressWarnings("rawtypes")
    private static final EmptyBranchTable INSTANCE = new EmptyBranchTable();

    private EmptyBranchTable() {}

    @SuppressWarnings("unchecked")
    static <T> EmptyBranchTable<T> empty() {
        return (EmptyBranchTable<T>) INSTANCE;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Branch<V> get(String key) {
        return null;
    }

    @Override
    public BranchTable<V> plus(String key, Branch<V> value) {
        return new SingleEntryBranchTable<>(key, value);
    }

    @Override
    public BranchTable<V> minus(String key) {
        return this;
    }

    @Override
    public void forEach(BiConsumer<String, Branch<V>> consumer) {
        // no-op
    }

    @Override
    public Map<String, Branch<V>> asMapView() {
        return Collections.emptyMap();
    }
}

