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

package org.apache.bifromq.mqtt.session;

import org.apache.bifromq.type.TopicMessagePack;
import java.util.Set;

/**
 * The interface of transient session.
 */
public interface IMQTTTransientSession extends IMQTTSession {
    String NAME = "MQTTTransientSession";

    /**
     * Publish message to the topic filters, and return the topic filters that are not subscribed.
     *
     * @param messagePack         The message pack to publish.
     * @param matchedTopicFilters The topic filters to publish.
     * @return The topic filters that are not subscribed.
     */
    Set<MatchedTopicFilter> publish(TopicMessagePack messagePack, Set<MatchedTopicFilter> matchedTopicFilters);

    boolean hasSubscribed(String topicFilter);

    /**
     * The matched topic filter.
     *
     * @param topicFilter The topic filter.
     * @param incarnation The incarnation.
     */
    record MatchedTopicFilter(String topicFilter, long incarnation) {
    }
}
