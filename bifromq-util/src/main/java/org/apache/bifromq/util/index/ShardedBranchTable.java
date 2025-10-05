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

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiConsumer;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

/**
 * BranchTable implementation with segmented shards. Each segment holds a small array
 * of PMap shards.
 */
final class ShardedBranchTable<V> implements BranchTable<V> {
    private final PMap<String, Branch<V>>[][] segments; // [segment][within]
    private final int shardBits;
    private final int segBits;
    private final int size;

    ShardedBranchTable(PMap<String, Branch<V>>[][] segments, int shardBits, int segBits, int size) {
        this.segments = segments;
        this.shardBits = shardBits;
        this.segBits = segBits;
        this.size = size;
    }

    static <T> ShardedBranchTable<T> from(PMap<String, Branch<T>> map, int shardBits, int segBits) {
        int segNum = 1 << segBits;
        int perSeg = 1 << (shardBits - segBits);
        @SuppressWarnings("unchecked")
        PMap<String, Branch<T>>[][] segs = (PMap<String, Branch<T>>[][]) new PMap[segNum][perSeg];
        for (int s = 0; s < segNum; s++) {
            for (int i = 0; i < perSeg; i++) {
                segs[s][i] = HashTreePMap.empty();
            }
        }
        int count = 0;
        for (Map.Entry<String, Branch<T>> e : map.entrySet()) {
            int idx = idx(e.getKey(), shardBits);
            int si = segIdx(idx, shardBits, segBits);
            int wi = withinIdx(idx, shardBits, segBits);
            PMap<String, Branch<T>> shard = segs[si][wi];
            segs[si][wi] = shard.plus(e.getKey(), e.getValue());
            count++;
        }
        return new ShardedBranchTable<>(segs, shardBits, segBits, count);
    }

    // borrowed from common hash spreaders
    private static int smear(int h) {
        h ^= (h >>> 16);
        h *= 0x7feb352d;
        h ^= (h >>> 15);
        h *= 0x846ca68b;
        h ^= (h >>> 16);
        return h;
    }

    private static int idx(String key, int shardBits) {
        int h = smear(key.hashCode());
        return h & ((1 << shardBits) - 1);
    }

    private static int segIdx(int idx, int shardBits, int segBits) {
        return idx >>> (shardBits - segBits);
    }

    private static int withinIdx(int idx, int shardBits, int segBits) {
        return idx & ((1 << (shardBits - segBits)) - 1);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Branch<V> get(String key) {
        int idx = idx(key, shardBits);
        PMap<String, Branch<V>> shard = segments[segIdx(idx, shardBits, segBits)][withinIdx(idx, shardBits, segBits)];
        return shard.get(key);
    }

    @Override
    public BranchTable<V> plus(String key, Branch<V> value) {
        int idx = idx(key, shardBits);
        int si = segIdx(idx, shardBits, segBits);
        int wi = withinIdx(idx, shardBits, segBits);
        PMap<String, Branch<V>> old = segments[si][wi];
        Branch<V> existed = old.get(key);
        PMap<String, Branch<V>> neu = old.plus(key, value);
        if (neu == old) {
            return this;
        }
        PMap<String, Branch<V>>[] rowCopy = segments[si].clone();
        rowCopy[wi] = neu;
        PMap<String, Branch<V>>[][] segsCopy = segments.clone();
        segsCopy[si] = rowCopy;
        return new ShardedBranchTable<>(segsCopy, shardBits, segBits, existed == null ? size + 1 : size);
    }

    @Override
    public BranchTable<V> minus(String key) {
        int idx = idx(key, shardBits);
        int si = segIdx(idx, shardBits, segBits);
        int wi = withinIdx(idx, shardBits, segBits);
        PMap<String, Branch<V>> old = segments[si][wi];
        Branch<V> existed = old.get(key);
        if (existed == null) {
            return this;
        }
        PMap<String, Branch<V>> neu = old.minus(key);
        PMap<String, Branch<V>>[] rowCopy = segments[si].clone();
        rowCopy[wi] = neu;
        PMap<String, Branch<V>>[][] segsCopy = segments.clone();
        segsCopy[si] = rowCopy;
        return new ShardedBranchTable<>(segsCopy, shardBits, segBits, size - 1);
    }

    @Override
    public void forEach(BiConsumer<String, Branch<V>> consumer) {
        for (PMap<String, Branch<V>>[] row : segments) {
            for (PMap<String, Branch<V>> shard : row) {
                for (Map.Entry<String, Branch<V>> e : shard.entrySet()) {
                    consumer.accept(e.getKey(), e.getValue());
                }
            }
        }
    }

    @Override
    public Map<String, Branch<V>> asMapView() {
        // Lightweight Map view backed by shards
        return new AbstractMap<>() {
            @Override
            public Branch<V> get(Object key) {
                if (!(key instanceof String s)) {
                    return null;
                }
                return ShardedBranchTable.this.get(s);
            }

            @Override
            public boolean containsKey(Object key) {
                return get(key) != null;
            }

            @Override
            public Set<Entry<String, Branch<V>>> entrySet() {
                return new EntrySetView();
            }

            @Override
            public int size() {
                return ShardedBranchTable.this.size();
            }
        };
    }

    private final class EntrySetView extends AbstractSet<Map.Entry<String, Branch<V>>> {
        @Override
        public Iterator<Map.Entry<String, Branch<V>>> iterator() {
            return new Iterator<>() {
                int si = 0;
                int wi = 0;
                Iterator<Map.Entry<String, Branch<V>>> it = segments.length == 0
                    ? Collections.emptyIterator()
                    : segments[0][0].entrySet().iterator();

                private void advance() {
                    while (it != null && !it.hasNext()) {
                        wi++;
                        if (si < segments.length && wi < segments[si].length) {
                            it = segments[si][wi].entrySet().iterator();
                        } else {
                            si++;
                            if (si >= segments.length) {
                                it = Collections.emptyIterator();
                                return;
                            }
                            wi = 0;
                            it = segments[si][wi].entrySet().iterator();
                        }
                    }
                }

                @Override
                public boolean hasNext() {
                    advance();
                    return it != null && it.hasNext();
                }

                @Override
                public Map.Entry<String, Branch<V>> next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    return it.next();
                }
            };
        }

        @Override
        public int size() {
            return ShardedBranchTable.this.size();
        }
    }
}
