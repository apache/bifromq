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

package org.apache.bifromq.baserpc.client.loadbalancer;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class TrieMap<V> extends AbstractMap<String, V> {
    /**
     * Child tries.
     */
    private Map<String, TrieMap<V>> children;
    /**
     * Value at a leaf node (leaf node is indicated by non-null value).
     */
    private V value;

    // Should we have explicit marking if this element is a leaf node without requiring value?
    public TrieMap() {
    }

    private boolean isLeaf() {
        return value != null;
    }

    private TrieMap<V> getChildTrie(String key) {
        TrieMap<V> curTrie = this;
        // go through each element
        for (int i = 0; i < key.length(); i++) {
            String element = String.valueOf(key.charAt(i));
            curTrie = (curTrie.children != null) ? curTrie.children.get(element) : null;
            if (curTrie == null) {
                return null;
            }
        }
        return curTrie;
    }

    // Trie specific functions
    public V bestMatch(String key) {
        TrieMap<V> curTrie = this;
        V matched = curTrie.value;
        for (int i = 0; i < key.length(); i++) {
            String element = String.valueOf(key.charAt(i));
            if (curTrie.children == null) {
                break;
            }
            if (curTrie.children.containsKey(element)) {
                curTrie = curTrie.children.get(element);
                if (curTrie.value != null) {
                    matched = curTrie.value;
                }
            } else {
                break;
            }
        }
        return matched;
    }

    // Functions to support map interface to lookup using sequence
    @Override
    public int size() {
        int s = 0;
        if (children != null) {
            for (Entry<String, TrieMap<V>> kTrieMapEntry : children.entrySet()) {
                s += kTrieMapEntry.getValue().size();
            }
        }
        if (isLeaf()) {
            s++;
        }
        return s;
    }

    @Override
    public boolean isEmpty() {
        return (children == null && !isLeaf());
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        return values().contains(value);
    }

    @Override
    public V get(Object key) {
        if (key instanceof String) {
            return get((String) key);
        }
        return null;
    }

    public V get(String key) {
        TrieMap<V> curTrie = getChildTrie(key);
        return (curTrie != null) ? curTrie.value : null;
    }

    @Override
    public V put(String key, V value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        TrieMap<V> curTrie = this;
        // go through each element
        for (int i = 0; i < key.length(); i++) {
            String element = String.valueOf(key.charAt(i));
            if (curTrie.children == null) {
                // Generics.newConcurrentHashMap();
                curTrie.children = new HashMap<>();
            }
            TrieMap<V> parent = curTrie;
            curTrie = curTrie.children.get(element);
            if (curTrie == null) {
                parent.children.put(element, curTrie = new TrieMap<>());
            }
        }
        V oldValue = curTrie.value;
        curTrie.value = value;
        return oldValue;
    }

    @Override
    public V remove(Object key) {
        if (key instanceof String) {
            return remove((String) key);
        }
        return null;
    }

    public V remove(String key) {
        TrieMap<V> parent = null;
        TrieMap<V> curTrie = this;
        String lastKey = null;
        // go through each element
        for (int i = 0; i < key.length(); i++) {
            String element = String.valueOf(key.charAt(i));
            if (curTrie.children == null) {
                return null;
            }
            lastKey = element;
            parent = curTrie;
            curTrie = curTrie.children.get(element);
            if (curTrie == null) {
                return null;
            }
        }
        V v = curTrie.value;
        if (parent != null) {
            parent.children.remove(lastKey);
        } else {
            value = null;
        }
        return v;
    }

    @Override
    public void clear() {
        value = null;
        children = null;
    }

    @Override
    public Set<String> keySet() {
        Set<String> keys = new LinkedHashSet<>();
        gatherKeys(keys, new StringBuffer());
        return keys;
    }

    private void gatherKeys(Set<String> keys, StringBuffer prefix) {
        if (children != null) {
            for (Entry<String, TrieMap<V>> kTrieMapEntry : children.entrySet()) {
                StringBuffer p = new StringBuffer(prefix.length() + 1);
                p.append(prefix);
                p.append(kTrieMapEntry.getKey());
                kTrieMapEntry.getValue().gatherKeys(keys, p);
            }
        }
        if (value != null) {
            keys.add(prefix.toString());
        }
    }

    @Override
    public Collection<V> values() {
        List<V> values = new ArrayList<>();
        gatherValues(values);
        return values;
    }

    private void gatherValues(List<V> values) {
        if (children != null) {
            for (Entry<String, TrieMap<V>> kTrieMapEntry : children.entrySet()) {
                kTrieMapEntry.getValue().gatherValues(values);
            }
        }
        if (value != null) {
            values.add(value);
        }
    }

    @Override
    public Set<Entry<String, V>> entrySet() {
        Set<Entry<String, V>> entries = new LinkedHashSet<>();
        gatherEntries(entries, new StringBuffer());
        return entries;
    }

    private void gatherEntries(Set<Entry<String, V>> entries, final StringBuffer prefix) {
        if (children != null) {
            for (Entry<String, TrieMap<V>> kTrieMapEntry : children.entrySet()) {
                StringBuffer p = new StringBuffer(prefix.length() + 1);
                p.append(prefix);
                p.append(kTrieMapEntry.getKey());
                kTrieMapEntry.getValue().gatherEntries(entries, p);
            }
        }
        if (value != null) {
            entries.add(new Entry<>() {
                @Override
                public String getKey() {
                    return prefix.toString();
                }

                @Override
                public V getValue() {
                    return value;
                }

                @Override
                public V setValue(V value) {
                    throw new UnsupportedOperationException();
                }
            });
        }
    }
}
