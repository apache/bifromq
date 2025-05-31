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

package org.apache.bifromq.mqtt.handler.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class ReceiverTopicAliasManagerTest {
    @Test
    public void case1() {
        ReceiverTopicAliasManager topicAliasMap = new ReceiverTopicAliasManager();
        topicAliasMap.setAlias("topic1", 1);

        assertTrue(topicAliasMap.getAlias("topic1").isPresent());
        assertEquals(topicAliasMap.getAlias("topic1").get(), 1);

        assertTrue(topicAliasMap.getTopic(1).isPresent());
        assertEquals(topicAliasMap.getTopic(1).get(), "topic1");
    }

    @Test
    public void cass2() {
        ReceiverTopicAliasManager topicAliasMap = new ReceiverTopicAliasManager();
        topicAliasMap.setAlias("topic1", 1);
        topicAliasMap.setAlias("topic2", 1);

        assertTrue(topicAliasMap.getTopic(1).isPresent());
        assertEquals(topicAliasMap.getTopic(1).get(), "topic2");

        assertTrue(topicAliasMap.getAlias("topic1").isEmpty());
    }

    @Test
    public void case3() {
        ReceiverTopicAliasManager topicAliasMap = new ReceiverTopicAliasManager();
        topicAliasMap.setAlias("topic1", 1);
        topicAliasMap.setAlias("topic1", 2);

        assertTrue(topicAliasMap.getAlias("topic1").isPresent());
        assertEquals(topicAliasMap.getAlias("topic1").get(), 2);

        assertTrue(topicAliasMap.getTopic(2).isPresent());
        assertEquals(topicAliasMap.getTopic(2).get(), "topic1");
    }
}
