/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.dist.trie;

import static org.apache.bifromq.util.TopicConst.NUL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

public class TopicTrieNodeTest {
    @Test
    public void topicRoot() {
        TopicTrieNode<String> topicTrieNode = TopicTrieNode.<String>builder(true).build();
        assertEquals(topicTrieNode.levelName(), NUL);
        assertFalse(topicTrieNode.isUserTopic());
        assertFalse(topicTrieNode.wildcardMatchable());
        assertTrue(topicTrieNode.values().isEmpty());
        assertNull(topicTrieNode.topic());
    }

    @Test
    public void globalTopicTrie() {
        TopicTrieNode<String> root = TopicTrieNode.<String>builder(true)
            .addTopic(List.of("TenantA", "a"), "value1")
            .addTopic(List.of("TenantA", "$sys", "a"), "value2")
            .addTopic(List.of("TenantA", "$sys", "a"), "value3")
            .build();
        assertEquals(root.children().size(), 1);

        TopicTrieNode<String> tenantANode = root.child("TenantA");
        assertEquals(tenantANode.children().size(), 2);
        assertFalse(tenantANode.wildcardMatchable());
        assertFalse(tenantANode.isUserTopic());
        assertEquals(tenantANode.levelName(), "TenantA");

        TopicTrieNode<String> aNode = tenantANode.child("a");
        assertEquals(aNode.children().size(), 0);
        assertTrue(aNode.isUserTopic());
        assertTrue(aNode.wildcardMatchable());
        assertEquals(aNode.levelName(), "a");
        assertEquals(aNode.values(), Set.of("value1"));

        TopicTrieNode<String> sysNode = tenantANode.child("$sys");
        assertEquals(sysNode.children().size(), 1);
        assertFalse(sysNode.wildcardMatchable());
        assertFalse(sysNode.isUserTopic());
        assertEquals(sysNode.levelName(), "$sys");

        TopicTrieNode<String> sysANode = sysNode.child("a");
        assertEquals(sysANode.children().size(), 0);
        assertTrue(sysANode.isUserTopic());
        assertTrue(sysANode.wildcardMatchable());
        assertEquals(sysANode.levelName(), "a");
        assertEquals(sysANode.values(), Set.of("value2", "value3"));
    }

    @Test
    public void localTopicTrie() {
        TopicTrieNode<String> root = TopicTrieNode.<String>builder(false)
            .addTopic(List.of("a"), "value1")
            .addTopic(List.of("$sys", "a"), "value2")
            .addTopic(List.of("$sys", "a"), "value3")
            .build();

        assertEquals(root.children().size(), 2);

        TopicTrieNode<String> aNode = root.child("a");
        assertNotNull(aNode);
        assertEquals(aNode.levelName(), "a");
        assertTrue(aNode.wildcardMatchable());
        assertTrue(aNode.isUserTopic());
        assertEquals(aNode.values(), Set.of("value1"));

        TopicTrieNode<String> sysNode = root.child("$sys");
        assertNotNull(sysNode);
        assertEquals(sysNode.levelName(), "$sys");
        assertFalse(sysNode.wildcardMatchable());
        assertFalse(sysNode.isUserTopic());

        TopicTrieNode<String> sysANode = sysNode.child("a");
        assertNotNull(sysANode);
        assertEquals(sysANode.levelName(), "a");
        assertTrue(sysANode.wildcardMatchable());
        assertTrue(sysANode.isUserTopic());
        assertEquals(sysANode.values(), Set.of("value2", "value3"));
    }
}
