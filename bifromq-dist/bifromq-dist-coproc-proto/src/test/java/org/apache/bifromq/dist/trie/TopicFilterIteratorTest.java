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

import static org.apache.bifromq.dist.TestUtil.randomTopic;
import static org.apache.bifromq.dist.TestUtil.randomTopicFilter;
import static org.apache.bifromq.util.TopicConst.NUL;
import static org.apache.bifromq.util.TopicUtil.escape;
import static org.apache.bifromq.util.TopicUtil.fastJoin;
import static org.apache.bifromq.util.TopicUtil.parse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.dist.TestUtil;
import org.apache.bifromq.util.TopicConst;
import org.apache.bifromq.util.TopicUtil;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class TopicFilterIteratorTest {
    @Test
    public void expandRandomLocalTopic() {
        String topic = randomTopic();
        TopicTrieNode<String> topicTrie =
            TopicTrieNode.<String>builder(false).addTopic(parse(topic, false), "v1").build();
        TopicFilterIterator<String> itr = new TopicFilterIterator<>(topicTrie);
        List<String> generated = new ArrayList<>();
        for (; itr.isValid(); itr.next()) {
            generated.add(fastJoin(NUL, itr.key()));
        }
        List<String> allFilters = TestUtil.expand(topic);
        assertEquals(generated, allFilters);
    }

    @Test
    public void expandGlobalTopics() {
        expandTopics(Fixtures.GlobalTopicToFilters, true);
    }

    @Test
    public void expandLocalTopics() {
        expandTopics(Fixtures.LocalTopicToFilters, false);
    }

    @Test
    public void seekExistAndIteration() {
        TopicTrieNode.Builder<String> builder = TopicTrieNode.builder(false);
        builder.addTopic(parse(randomTopic(), false), "_");
        builder.addTopic(parse(randomTopic(), false), "_");
        builder.addTopic(parse(randomTopic(), false), "_");
        TopicFilterIterator<String> itr = new TopicFilterIterator<>(builder.build());
        List<String> generated = new ArrayList<>();
        for (; itr.isValid(); itr.next()) {
            generated.add(fastJoin(NUL, itr.key()));
        }
        List<String> generatedAfterSeek = new ArrayList<>();
        int seekIdx = ThreadLocalRandom.current().nextInt(generated.size());
        String seekTopicFilter = generated.get(seekIdx);
        for (itr.seek(parse(seekTopicFilter, true)); itr.isValid(); itr.next()) {
            String current = fastJoin(NUL, itr.key());
            generatedAfterSeek.add(current);
            assertTrue(seekTopicFilter.compareTo(current) <= 0);
        }
        assertEquals(generated.subList(seekIdx, generated.size()), generatedAfterSeek);
    }

    @Test
    public void randomSeekAndIteration() {
        TopicTrieNode.Builder<String> builder = TopicTrieNode.builder(false);
        builder.addTopic(parse(randomTopic(), false), "_");
        builder.addTopic(parse(randomTopic(), false), "_");
        builder.addTopic(parse(randomTopic(), false), "_");
        TopicFilterIterator<String> itr = new TopicFilterIterator<>(builder.build());
        List<String> generated = new ArrayList<>();
        for (; itr.isValid(); itr.next()) {
            generated.add(fastJoin(NUL, itr.key()));
        }
        List<String> generatedAfterSeek = new ArrayList<>();
        String seekTopicFilter = escape(randomTopicFilter());
        for (itr.seek(parse(seekTopicFilter, true));
             itr.isValid(); itr.next()) {
            generatedAfterSeek.add(fastJoin(NUL, itr.key()));
        }
        for (int i = 0; i < generatedAfterSeek.size(); i++) {
            assertEquals(generated.get(generated.size() - 1 - i),
                generatedAfterSeek.get(generatedAfterSeek.size() - 1 - i));
        }
    }

    @Test
    public void seekExistAndBackwardIteration() {
        TopicTrieNode.Builder<String> builder = TopicTrieNode.builder(false);
        builder.addTopic(parse(randomTopic(), false), "_");
        builder.addTopic(parse(randomTopic(), false), "_");
        builder.addTopic(parse(randomTopic(), false), "_");
        TopicFilterIterator<String> itr = new TopicFilterIterator<>(builder.build());
        List<String> generated = new ArrayList<>();
        for (; itr.isValid(); itr.next()) {
            generated.add(fastJoin(NUL, itr.key()));
        }
        List<String> generatedAfterSeek = new ArrayList<>();
        int seekIdx = ThreadLocalRandom.current().nextInt(generated.size());
        String seekTopicFilter = generated.get(seekIdx);
        for (itr.seek(parse(seekTopicFilter, true)); itr.isValid(); itr.prev()) {
            String current = fastJoin(NUL, itr.key());
            generatedAfterSeek.add(current);
            assertTrue(seekTopicFilter.compareTo(current) >= 0);
        }
        for (int i = 0; i < generatedAfterSeek.size(); i++) {
            int j = generatedAfterSeek.size() - i - 1;
            assertEquals(generated.get(j), generatedAfterSeek.get(i));
        }
    }

    @Test
    public void randomSeekAndBackwardIteration() {
        TopicTrieNode.Builder<String> builder = TopicTrieNode.builder(false);
        builder.addTopic(parse(randomTopic(), false), "_");
        builder.addTopic(parse(randomTopic(), false), "_");
        builder.addTopic(parse(randomTopic(), false), "_");
        TopicFilterIterator<String> itr = new TopicFilterIterator<>(builder.build());
        List<String> generated = new ArrayList<>();
        for (; itr.isValid(); itr.next()) {
            generated.add(fastJoin(NUL, itr.key()));
        }
        List<String> generatedAfterSeek = new ArrayList<>();
        String seekTopicFilter = escape(randomTopicFilter());
        int k = 0;
        for (itr.seek(parse(seekTopicFilter, true)); itr.isValid(); itr.prev()) {
            String current = fastJoin(NUL, itr.key());
            generatedAfterSeek.add(current);
            if (k++ == 0) {
                assertTrue(seekTopicFilter.compareTo(current) <= 0);
            } else {
                assertTrue(seekTopicFilter.compareTo(current) > 0);
            }
        }
        for (int i = 0; i < generatedAfterSeek.size(); i++) {
            int j = generatedAfterSeek.size() - i - 1;
            assertEquals(generated.get(j), generatedAfterSeek.get(i));
        }
    }

    @Test
    public void seekPrevExistAndIteration() {
        TopicTrieNode.Builder<String> builder = TopicTrieNode.builder(false);
        builder.addTopic(parse(randomTopic(), false), "_");
        builder.addTopic(parse(randomTopic(), false), "_");
        builder.addTopic(parse(randomTopic(), false), "_");
        TopicFilterIterator<String> itr = new TopicFilterIterator<>(builder.build());
        List<String> generated = new ArrayList<>();
        for (; itr.isValid(); itr.next()) {
            generated.add(fastJoin(NUL, itr.key()));
        }
        List<String> generatedAfterSeek = new ArrayList<>();
        int seekIdx = ThreadLocalRandom.current().nextInt(1, generated.size());
        String seekTopicFilter = generated.get(seekIdx);
        int i = 0;
        for (itr.seekPrev(parse(seekTopicFilter, true)); itr.isValid(); itr.next()) {
            String current = fastJoin(NUL, itr.key());
            generatedAfterSeek.add(current);
            if (i++ == 0) {
                assertTrue(seekTopicFilter.compareTo(current) > 0);
            } else {
                assertTrue(seekTopicFilter.compareTo(current) <= 0);
            }
        }
        assertEquals(generated.subList(seekIdx - 1, generated.size()), generatedAfterSeek);
    }

    @Test
    public void randomSeekPrevAndIteration() {
        TopicTrieNode.Builder<String> builder = TopicTrieNode.builder(false);
        builder.addTopic(parse(randomTopic(), false), "_");
        builder.addTopic(parse(randomTopic(), false), "_");
        builder.addTopic(parse(randomTopic(), false), "_");
        TopicFilterIterator<String> itr = new TopicFilterIterator<>(builder.build());
        List<String> generated = new ArrayList<>();
        for (; itr.isValid(); itr.next()) {
            generated.add(fastJoin(NUL, itr.key()));
        }
        List<String> generatedAfterSeek = new ArrayList<>();
        String seekTopicFilter = escape(randomTopicFilter());
        int k = 0;
        for (itr.seekPrev(parse(seekTopicFilter, true)); itr.isValid(); itr.next()) {
            String current = fastJoin(NUL, itr.key());
            generatedAfterSeek.add(current);
            if (k++ == 0) {
                assertTrue(seekTopicFilter.compareTo(current) > 0);
            } else {
                assertTrue(seekTopicFilter.compareTo(current) <= 0);
            }
        }
        for (int i = 0; i < generatedAfterSeek.size(); i++) {
            assertEquals(generated.get(generated.size() - 1 - i),
                generatedAfterSeek.get(generatedAfterSeek.size() - 1 - i));
        }
    }

    @Test
    public void seekPrevExistAndBackwardIteration() {
        TopicTrieNode.Builder<String> builder = TopicTrieNode.builder(false);
        builder.addTopic(parse(randomTopic(), false), "_");
        builder.addTopic(parse(randomTopic(), false), "_");
        builder.addTopic(parse(randomTopic(), false), "_");
        TopicFilterIterator<String> itr = new TopicFilterIterator<>(builder.build());
        List<String> generated = new ArrayList<>();
        for (; itr.isValid(); itr.next()) {
            generated.add(fastJoin(NUL, itr.key()));
        }
        List<String> generatedAfterSeek = new ArrayList<>();
        int seekIdx = ThreadLocalRandom.current().nextInt(1, generated.size());
        String seekTopicFilter = generated.get(seekIdx);
        for (itr.seekPrev(parse(seekTopicFilter, true)); itr.isValid(); itr.prev()) {
            String current = fastJoin(NUL, itr.key());
            generatedAfterSeek.add(current);
            assertTrue(seekTopicFilter.compareTo(current) > 0);
        }
        for (int i = 0; i < generatedAfterSeek.size(); i++) {
            int j = generatedAfterSeek.size() - i - 1;
            assertEquals(generated.get(j), generatedAfterSeek.get(i));
        }
    }

    @Test
    public void randomSeekPrevAndBackwardIteration() {
        TopicTrieNode.Builder<String> builder = TopicTrieNode.builder(false);
        builder.addTopic(parse(randomTopic(), false), "_");
        builder.addTopic(parse(randomTopic(), false), "_");
        builder.addTopic(parse(randomTopic(), false), "_");
        TopicFilterIterator<String> itr = new TopicFilterIterator<>(builder.build());
        List<String> generated = new ArrayList<>();
        for (; itr.isValid(); itr.next()) {
            generated.add(fastJoin(NUL, itr.key()));
        }
        List<String> generatedAfterSeek = new ArrayList<>();
        String seekTopicFilter = escape(randomTopicFilter());
        for (itr.seekPrev(parse(seekTopicFilter, true)); itr.isValid(); itr.prev()) {
            String current = fastJoin(NUL, itr.key());
            generatedAfterSeek.add(current);
            assertTrue(seekTopicFilter.compareTo(current) > 0);
        }
        for (int i = 0; i < generatedAfterSeek.size(); i++) {
            int j = generatedAfterSeek.size() - i - 1;
            assertEquals(generated.get(j), generatedAfterSeek.get(i));
        }
    }

    @Test
    public void associatedValues() {
        TopicTrieNode.Builder<String> builder = TopicTrieNode.builder(false);
        builder.addTopic(parse("a", false), "v1");
        builder.addTopic(parse("a/b", false), "v2");
        builder.addTopic(parse("c", false), "v3");
        TopicFilterIterator<String> itr = new TopicFilterIterator<>(builder.build());
        itr.seek(List.of("#"));
        assertEquals(itr.value().size(), 3);
        assertEquals(itr.value().get(List.of("a")), Set.of("v1"));
        assertEquals(itr.value().get(List.of("a", "b")), Set.of("v2"));
        assertEquals(itr.value().get(List.of("c")), Set.of("v3"));

        itr.seek(List.of("+"));
        assertEquals(itr.value().size(), 2);
        assertEquals(itr.value().get(List.of("a")), Set.of("v1"));
        assertEquals(itr.value().get(List.of("c")), Set.of("v3"));

        itr.seek(List.of("a", "#"));
        assertEquals(itr.key(), List.of("a", "#"));
        assertEquals(itr.value().size(), 2);
        assertEquals(itr.value().get(List.of("a")), Set.of("v1"));
        assertEquals(itr.value().get(List.of("a", "b")), Set.of("v2"));

        itr.seek(List.of("a", "+"));
        assertEquals(itr.key(), List.of("a", "+"));
        assertEquals(itr.value().get(List.of("a", "b")), Set.of("v2"));

        itr.seek(List.of("a", "+", "#"));
        assertEquals(itr.key(), List.of("a", "+", "#"));
        assertEquals(itr.value().get(List.of("a", "b")), Set.of("v2"));
    }

    @Test
    public void localSysTopicMatch() {
        TopicTrieNode.Builder<String> builder = TopicTrieNode.builder(false);
        builder.addTopic(parse("$sys/a", false), "v1");
        builder.addTopic(parse("a/b", false), "v2");
        builder.addTopic(parse("c", false), "v3");
        TopicFilterIterator<String> itr = new TopicFilterIterator<>(builder.build());
        itr.seek(List.of("#"));
        assertEquals(itr.value().size(), 2);
        assertEquals(itr.value().get(List.of("a", "b")), Set.of("v2"));
        assertEquals(itr.value().get(List.of("c")), Set.of("v3"));
    }

    @Test
    public void globalSysTopicMatch() {
        TopicTrieNode.Builder<String> builder = TopicTrieNode.builder(true);
        builder.addTopic(parse("tenant/$sys/a", false), "v1");
        builder.addTopic(parse("tenant/a/b", false), "v2");
        builder.addTopic(parse("tenant/c", false), "v3");
        TopicFilterIterator<String> itr = new TopicFilterIterator<>(builder.build());
        itr.seek(List.of("tenant", "#"));
        assertEquals(itr.value().size(), 2);
        assertEquals(itr.value().get(List.of("tenant", "a", "b")), Set.of("v2"));
        assertEquals(itr.value().get(List.of("tenant", "c")), Set.of("v3"));
    }

    private void expandTopics(Map<String, List<String>> topicToFilters, boolean isGlobal) {
        TopicTrieNode.Builder<String> topicTrieBuilder = TopicTrieNode.builder(isGlobal);
        Set<String> allTopicFilters = new HashSet<>();
        for (String topic : topicToFilters.keySet()) {
            topicTrieBuilder.addTopic(TopicUtil.parse(topic, false), topic);
            allTopicFilters.addAll(topicToFilters.get(topic));
        }
        List<String> sortedTopicFilters = allTopicFilters.stream().map(TopicUtil::escape).sorted().toList();
        TopicFilterIterator<String> iterator = new TopicFilterIterator<>(topicTrieBuilder.build());
        List<String> generated = new ArrayList<>();
        for (; iterator.isValid(); iterator.next()) {
            generated.add(fastJoin(NUL, iterator.key()));
            log.info("{}-{}", fastJoin(TopicConst.DELIMITER, iterator.key()), iterator.value().values());
        }
        assertEquals(sortedTopicFilters, generated);
    }
}
