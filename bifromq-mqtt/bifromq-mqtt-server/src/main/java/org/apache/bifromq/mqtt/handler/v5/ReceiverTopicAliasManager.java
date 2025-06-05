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

package org.apache.bifromq.mqtt.handler.v5;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ReceiverTopicAliasManager {
    private final Map<String, Integer> topicToAlias = new HashMap<>();
    private final Map<Integer, String> aliasToTopic = new HashMap<>();

    public Optional<String> getTopic(int alias) {
        return Optional.ofNullable(aliasToTopic.get(alias));
    }

    public Optional<Integer> getAlias(String topic) {
        return Optional.ofNullable(topicToAlias.get(topic));
    }

    public void setAlias(String topic, int alias) {
        Integer oldAlias = topicToAlias.put(topic, alias);
        if (oldAlias != null) {
            aliasToTopic.remove(oldAlias);
        }
        String oldTopic = aliasToTopic.put(alias, topic);
        if (oldTopic != null) {
            topicToAlias.remove(oldTopic);
        }
    }
}
