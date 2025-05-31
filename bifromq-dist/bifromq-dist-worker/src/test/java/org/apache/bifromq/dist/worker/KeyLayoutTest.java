/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.dist.worker;

import static org.apache.bifromq.util.TopicUtil.fastJoin;
import static org.apache.bifromq.util.TopicUtil.parse;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.dist.rpc.proto.MatchRoute;
import org.apache.bifromq.dist.trie.TopicFilterIterator;
import org.apache.bifromq.dist.trie.TopicTrieNode;
import org.apache.bifromq.dist.worker.schema.KVSchemaUtil;
import org.apache.bifromq.util.TopicUtil;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.testng.annotations.Test;

public class KeyLayoutTest {
    @Test
    public void layout() {
        List<String> topics = List.of("$", "b", "a/b", "b/c", "a/b/c", "b/c/d");
        TopicTrieNode.Builder<String> trieNodeBuilder = TopicTrieNode.builder(false);
        topics.forEach(t -> trieNodeBuilder.addTopic(parse(t, false), t));
        TopicFilterIterator<String> itr = new TopicFilterIterator<>(trieNodeBuilder.build());
        List<String> generated = new ArrayList<>();
        for (; itr.isValid(); itr.next()) {
            generated.add(fastJoin("/", itr.key()));
        }
        Map<String, List<MatchRoute>> routes = new LinkedHashMap<>();
        generated.forEach(tf -> {
            for (int i = 0; i < 10; i++) {
                routes.computeIfAbsent(tf, k -> new ArrayList<>())
                    .add(MatchRoute.newBuilder()
                        .setMatcher(TopicUtil.from(tf))
                        .setReceiverId(UUID.randomUUID().toString())
                        .setDelivererKey(UUID.randomUUID().toString())
                        .setIncarnation(System.nanoTime())
                        .setBrokerId(ThreadLocalRandom.current().nextInt())
                        .build());
            }
        });
        SortedSet<ByteString> keys = new TreeSet<>(BoundaryUtil::compare);
        routes.forEach((tf, routeList) ->
            routeList.forEach(
                route -> keys.add(
                    KVSchemaUtil.toNormalRouteKey("t", TopicUtil.from(tf), KVSchemaUtil.toReceiverUrl(route)))));
        LinkedHashSet<String> parsed = new LinkedHashSet<>();
        keys.forEach(k -> parsed.add(KVSchemaUtil.parseRouteDetail(k).matcher().getMqttTopicFilter()));
        assertEquals(generated, new ArrayList<>(parsed));
    }
}
