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

/**
 * Immutable branch table abstraction.
 */
interface BranchTable<V> {
    /**
     * The number of branches in the table.
     *
     * @return number of branches
     * */
    int size();

    /** Get a branch by key or null if absent. */
    Branch<V> get(String key);

    /**
     * Return a new table with the given key updated/replaced to value.
     */
    BranchTable<V> plus(String key, Branch<V> value);

    /** Return a new table with the given key removed. */
    BranchTable<V> minus(String key);

    /** Iterate all entries without materializing an intermediate map. */
    void forEach(BiConsumer<String, Branch<V>> consumer);

    /**
     * Return a read-only Map view.
     */
    Map<String, Branch<V>> asMapView();
}

