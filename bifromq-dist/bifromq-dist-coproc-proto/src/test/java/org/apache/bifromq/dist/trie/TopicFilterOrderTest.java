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

package org.apache.bifromq.dist.trie;

import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.tenantRouteStartKey;
import static org.apache.bifromq.dist.worker.schema.RouteKeyOrder.tenantIdOrderKey;
import static org.testng.Assert.assertEquals;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import org.apache.bifromq.dist.TestUtil;
import org.apache.bifromq.util.TopicUtil;
import org.testng.annotations.Test;

public class TopicFilterOrderTest {
    @Test
    public void iterationAndSeeksMatchEncodedOrderAcrossPoolReuse() {
        for (int round = 0; round < 6; round++) {
            verifyOrder(true);
            verifyOrder(false);
            verifyOrder(true);
        }
    }

    private void verifyOrder(boolean global) {
        Comparator<List<String>> order = (left, right) -> ByteString.unsignedLexicographicalComparator().compare(
            global ? tenantRouteStartKey(left.get(0).substring(2), left.subList(1, left.size())) : tenantRouteStartKey("t", left),
            global ? tenantRouteStartKey(right.get(0).substring(2), right.subList(1, right.size())) : tenantRouteStartKey("t", right));
        NavigableSet<List<String>> expected = new TreeSet<>(order);
        TopicTrieNode.Builder<String> builder = TopicTrieNode.builder(global);
        List<String> tenants = global ? List.of("", "t", "aa", "zz", "bbb", "中", "\uD800\uDC00", "#", "+") : List.of("");
        for (String tenant : tenants) {
            for (String topic : List.of("a", "a//x", "/a", "a/", "$SYS/x", "\uE000/x", "\uD800\uDC00/x")) {
                builder.addTopic(global ? TopicUtil.parse(tenantIdOrderKey(tenant), topic, false)
                    : TopicUtil.parse(topic, false), topic);
                for (String filter : TestUtil.expand(topic)) {
                    List<String> levels = new ArrayList<>(TopicUtil.parse(filter, true));
                    if (global) {
                        levels.add(0, tenantIdOrderKey(tenant));
                    }
                    expected.add(levels);
                }
            }
        }
        try (TopicFilterIterator<String> iterator = new TopicFilterIterator<>()) {
            iterator.init(builder.build());
            List<List<String>> actual = new ArrayList<>();
            while (iterator.isValid()) {
                actual.add(iterator.key());
                iterator.next();
            }
            assertEquals(actual, new ArrayList<>(expected));
            List<List<String>> probes = new ArrayList<>(expected);
            for (String tenant : tenants) {
                List<String> probe = new ArrayList<>(List.of("b", "x"));
                if (global) {
                    probe.add(0, tenantIdOrderKey(tenant));
                }
                probes.add(probe);
            }
            probes.add(global ? List.of(tenantIdOrderKey("aaaa"), "a") : List.of("\uFFFF", "a"));
            for (List<String> probe : probes) {
                iterator.seek(probe);
                assertEquals(iterator.isValid() ? iterator.key() : null, expected.ceiling(probe));
                iterator.seekPrev(probe);
                assertEquals(iterator.isValid() ? iterator.key() : null, expected.lower(probe));
            }
            List<String> last = expected.last();
            iterator.seek(last);
            actual.clear();
            while (iterator.isValid()) {
                actual.add(iterator.key());
                iterator.prev();
            }
            Collections.reverse(actual);
            assertEquals(actual, new ArrayList<>(expected));
        }
    }
}
