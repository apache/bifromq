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

package org.apache.bifromq.retain.store.index;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RetainTopicIndexTest {
    private RetainTopicIndex index;

    @BeforeMethod
    public void setUp() {
        index = new RetainTopicIndex();
    }

    @Test
    public void testMatch() {
        String tenantId = "tenantA";
        add(tenantId, "/", "/a", "/b", "a", "a/", "a/b", "a/b/c", "$a", "$a/", "$a/b").join();
        assertMatch(tenantId, index.match(tenantId, "/"), "/");
        assertMatch(tenantId, index.match(tenantId, "/a"), "/a");
        assertMatch(tenantId, index.match(tenantId, "/b"), "/b");
        assertMatch(tenantId, index.match(tenantId, "a"), "a");
        assertMatch(tenantId, index.match(tenantId, "a/"), "a/");
        assertMatch(tenantId, index.match(tenantId, "a/b"), "a/b");
        assertMatch(tenantId, index.match(tenantId, "a/b/c"), "a/b/c");
        assertMatch(tenantId, index.match(tenantId, "$a"), "$a");
        assertMatch(tenantId, index.match(tenantId, "$a/"), "$a/");
        assertMatch(tenantId, index.match(tenantId, "$a/b"), "$a/b");

        assertMatch(tenantId, index.match(tenantId, ""));
        assertMatch(tenantId, index.match(tenantId, "fakeTopic"));
        assertMatch(tenantId, index.match(tenantId, "+"), "a");
        assertMatch(tenantId, index.match(tenantId, "+/#"), "/", "/a", "/b", "a", "a/", "a/b", "a/b/c");
        assertMatch(tenantId, index.match(tenantId, "+/+"), "/", "/a", "/b", "a/", "a/b");

        assertMatch(tenantId, index.match(tenantId, "#"), "/", "/a", "/b", "a", "a/", "a/b", "a/b/c");

        assertMatch(tenantId, index.match(tenantId, "/+"), "/", "/a", "/b");
        assertMatch(tenantId, index.match(tenantId, "/#"), "/", "/a", "/b");

        assertMatch(tenantId, index.match(tenantId, "a/+"), "a/", "a/b");
        assertMatch(tenantId, index.match(tenantId, "a/#"), "a", "a/", "a/b", "a/b/c");

        assertMatch(tenantId, index.match(tenantId, "$a/+"), "$a/", "$a/b");
        assertMatch(tenantId, index.match(tenantId, "$a/+/#"), "$a/", "$a/b");
        assertMatch(tenantId, index.match(tenantId, "$a/#"), "$a", "$a/", "$a/b");

        assertMatch(tenantId, index.match("tenantB", "#"));
    }

    @Test
    public void testFindAll() {
        String tenantId = "tenantA";
        add(tenantId, "/", "/a", "/b", "a", "a/", "a/b", "a/b/c", "$a", "$a/", "$a/b").join();
        assertMatch(tenantId, index.findAll(), "/", "/a", "/b", "a", "a/", "a/b", "a/b/c", "$a", "$a/", "$a/b");
    }

    @Test
    public void testRemove() {
        String tenantId = "tenantA";
        add("/", "/a", "/b", "a", "a/", "a/b", "a/b/c", "$a", "$a/", "$a/b").join();
        index.remove(tenantId, "/");
        assertMatch(tenantId, index.match(tenantId, "/"));
        index.remove(tenantId, "/a");
        assertMatch(tenantId, index.match(tenantId, "/a"));
        index.remove(tenantId, "/b");
        assertMatch(tenantId, index.match(tenantId, "/b"));
        index.remove(tenantId, "a");
        assertMatch(tenantId, index.match(tenantId, "a"));
        index.remove(tenantId, "a/");
        assertMatch(tenantId, index.match(tenantId, "a/"));
        index.remove(tenantId, "a/b");
        assertMatch(tenantId, index.match(tenantId, "a/b"));
        index.remove(tenantId, "a/b/c");
        assertMatch(tenantId, index.match(tenantId, "a/b/c"));
        index.remove(tenantId, "$a");
        assertMatch(tenantId, index.match(tenantId, "$a"));
        index.remove(tenantId, "$a/");
        assertMatch(tenantId, index.match(tenantId, "$a/"));
        index.remove(tenantId, "$a/b");
        assertMatch(tenantId, index.match(tenantId, "$a/b"));
        assertMatch(tenantId, index.match(tenantId, "#"));
    }


    @Test
    public void testEdgeCases() {
        String tenantId = "tenantA";
        add(tenantId, "/", "/").join();
        assertMatch(tenantId, index.match(tenantId, "#"), "/");
    }

    private CompletableFuture<Void> add(String tenantId, String... topics) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String topic : topics) {
            futures.add(CompletableFuture.runAsync(() -> index.add(tenantId, topic, System.nanoTime(), 1)));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    private void assertMatch(String tenantId, Set<RetainedMsgInfo> matches, String... expected) {
        assertEquals(new HashSet<>(matches),
            Set.of(expected).stream().map(topic -> new RetainedMsgInfo(tenantId, topic, 0, 0))
                .collect(Collectors.toSet()));
    }
}
