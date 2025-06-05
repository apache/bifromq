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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.testng.annotations.Test;

public class TrieMapTest {
    @Test
    void putAndGet() {
        TrieMap<String> trieMap = new TrieMap<>();

        trieMap.put("hello", "world");
        trieMap.put("hi", "there");

        assertEquals("world", trieMap.get("hello"));
        assertEquals("there", trieMap.get("hi"));
        assertNull(trieMap.get("he"));
    }

    @Test
    void remove() {
        TrieMap<String> trieMap = new TrieMap<>();

        trieMap.put("hello", "world");
        trieMap.put("hi", "there");

        assertEquals("world", trieMap.remove("hello"));
        assertNull(trieMap.get("hello"));
        assertEquals("there", trieMap.get("hi"));

        assertNull(trieMap.remove("unknown"));
    }

    @Test
    void containsKeyAndValue() {
        TrieMap<String> trieMap = new TrieMap<>();

        trieMap.put("apple", "fruit");
        trieMap.put("banana", "fruit");
        trieMap.put("car", "vehicle");

        assertTrue(trieMap.containsKey("apple"));
        assertFalse(trieMap.containsKey("ap"));

        assertTrue(trieMap.containsValue("fruit"));
        assertFalse(trieMap.containsValue("vegetable"));
    }

    @Test
    void size() {
        TrieMap<String> trieMap = new TrieMap<>();

        assertEquals(0, trieMap.size());

        trieMap.put("cat", "animal");
        trieMap.put("car", "vehicle");

        assertEquals(2, trieMap.size());

        trieMap.remove("car");
        assertEquals(1, trieMap.size());
    }

    @Test
    void bestMatch() {
        TrieMap<String> trieMap = new TrieMap<>();

        trieMap.put("hello", "greeting");
        trieMap.put("hell", "place");
        trieMap.put("he", "pronoun");

        assertEquals("pronoun", trieMap.bestMatch("heaven"));
        assertEquals("place", trieMap.bestMatch("hellfire"));
        assertEquals("greeting", trieMap.bestMatch("hello"));
        assertNull(trieMap.bestMatch("unknown"));
    }

    @Test
    void keySetValuesAndEntrySet() {
        TrieMap<String> trieMap = new TrieMap<>();

        trieMap.put("a", "1");
        trieMap.put("ab", "2");
        trieMap.put("abc", "3");

        Set<String> expectedKeys = Set.of("a", "ab", "abc");
        Set<String> expectedValues = Set.of("1", "2", "3");

        assertEquals(expectedKeys, trieMap.keySet());
        assertEquals(expectedValues, new HashSet<>(trieMap.values()));
        assertEquals(trieMap.entrySet().size(), 3);
    }

    @Test
    void putAndGetWithEmptyString() {
        TrieMap<String> trieMap = new TrieMap<>();

        trieMap.put("", "empty");

        assertEquals("empty", trieMap.get(""));
        assertNull(trieMap.get("non-empty"));
    }

    @Test
    void removeWithEmptyString() {
        TrieMap<String> trieMap = new TrieMap<>();

        trieMap.put("", "empty");
        trieMap.put("non-empty", "value");

        assertEquals("empty", trieMap.remove(""));
        assertNull(trieMap.get(""));
        assertEquals("value", trieMap.get("non-empty"));
    }

    @Test
    void containsKeyWithEmptyString() {
        TrieMap<String> trieMap = new TrieMap<>();

        trieMap.put("", "empty");

        assertTrue(trieMap.containsKey(""));
        assertFalse(trieMap.containsKey("non-empty"));
    }

    @Test
    void bestMatchWithEmptyString() {
        TrieMap<String> trieMap = new TrieMap<>();

        trieMap.put("", "empty");
        trieMap.put("hello", "world");

        assertEquals("empty", trieMap.bestMatch(""));

        assertEquals("world", trieMap.bestMatch("hello"));
        // Empty string is a prefix of "unknown"
        assertEquals("empty", trieMap.bestMatch("unknown"));
    }
}
