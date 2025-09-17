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

package org.apache.bifromq.dist.worker.schema;

import static org.apache.bifromq.util.TopicConst.DELIMITER;
import static org.apache.bifromq.util.TopicConst.ORDERED_SHARE;
import static org.apache.bifromq.util.TopicConst.UNORDERED_SHARE;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.bifromq.type.RouteMatcher;

/**
 * Test Helper for creating NormalMatching & GroupMatching directly.
 */
public final class Matchings {
    private Matchings() {
    }

    public static NormalMatching normalMatching(String tenantId,
                                                String mqttTopicFilter,
                                                int subBrokerId,
                                                String receiverId,
                                                String delivererKey,
                                                long incarnation) {
        RouteMatcher matcher = RouteMatcher.newBuilder()
            .setType(RouteMatcher.Type.Normal)
            .addAllFilterLevel(split(mqttTopicFilter))
            .setMqttTopicFilter(mqttTopicFilter)
            .build();
        return new NormalMatching(tenantId,
            matcher,
            KVSchemaUtil.toReceiverUrl(subBrokerId, receiverId, delivererKey),
            incarnation);
    }

    public static GroupMatching unorderedGroupMatching(String tenantId,
                                                       String sharedTopicFilter,
                                                       String group,
                                                       Map<String, Long> members) {
        RouteMatcher matcher = RouteMatcher.newBuilder()
            .setType(RouteMatcher.Type.UnorderedShare)
            .addAllFilterLevel(split(sharedTopicFilter))
            .setGroup(group)
            .setMqttTopicFilter(UNORDERED_SHARE + DELIMITER + group + DELIMITER + sharedTopicFilter)
            .build();
        return new GroupMatching(tenantId, matcher, members);
    }

    public static GroupMatching orderedGroupMatching(String tenantId,
                                                     String sharedTopicFilter,
                                                     String group,
                                                     Map<String, Long> members) {
        RouteMatcher matcher = RouteMatcher.newBuilder()
            .setType(RouteMatcher.Type.OrderedShare)
            .addAllFilterLevel(split(sharedTopicFilter))
            .setGroup(group)
            .setMqttTopicFilter(ORDERED_SHARE + DELIMITER + group + DELIMITER + sharedTopicFilter)
            .build();
        return new GroupMatching(tenantId, matcher, members);
    }

    public static String receiverUrl(int subBrokerId, String receiverId, String delivererKey) {
        return KVSchemaUtil.toReceiverUrl(subBrokerId, receiverId, delivererKey);
    }

    private static List<String> split(String topicFilter) {
        return Arrays.asList(topicFilter.split("/", -1));
    }
}
