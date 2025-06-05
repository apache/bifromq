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

package org.apache.bifromq.dist.trie.benchmark;

import org.apache.bifromq.dist.TestUtil;
import org.apache.bifromq.dist.trie.TopicTrieNode;
import org.apache.bifromq.util.TopicUtil;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@Slf4j
@State(Scope.Thread)
public class TopicTrieBuilderBenchmarkState {
    public TopicTrieNode.Builder<String> topicTrieBuilder;

    @Setup(Level.Iteration)
    public void setup() {
        topicTrieBuilder = TopicTrieNode.builder(false);
    }

    /*
     * And, check the benchmark went fine afterwards:
     */

    @TearDown(Level.Iteration)
    public void tearDown() {
    }

    public List<String> randomTopic() {
        return TopicUtil.parse(TestUtil.randomTopic(), true);
    }
}
