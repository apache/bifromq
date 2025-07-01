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

package org.apache.bifromq.mqtt.handler;

import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicFilterOption;

/**
 * Represents a message that has been routed to a specific topic and client.
 * It contains the original message, the topic it was routed to, the publisher's information,
 * and various options related to the routing.
 */
@Accessors(fluent = true)
@Getter
public class RoutedMessage {
    private final String topic;
    private final Message message;
    private final ClientInfo publisher;
    private final String topicFilter;
    private final TopicFilterOption option;
    private final int bytesSize;
    private final boolean permissionGranted;
    private final boolean isDup; // if duplicated because of internal retry, should be dropped before send
    private final long hlc;
    private final long inboxPos; // used in persistent session, the position in inbox

    public RoutedMessage(String topic,
                         Message message,
                         ClientInfo publisher,
                         String topicFilter,
                         TopicFilterOption option,
                         long hlc,
                         boolean permissionGranted,
                         boolean isDup) {
        this(topic, message, publisher, topicFilter, option, hlc, permissionGranted, isDup, 0);
    }

    public RoutedMessage(String topic,
                         Message message,
                         ClientInfo publisher,
                         String topicFilter,
                         TopicFilterOption option,
                         long hlc,
                         boolean permissionGranted,
                         boolean isDup,
                         long inboxPos) {
        this.topic = topic;
        this.message = message;
        this.publisher = publisher;
        this.topicFilter = topicFilter;
        this.option = option;
        this.hlc = hlc;
        this.permissionGranted = permissionGranted;
        this.isDup = isDup;
        this.bytesSize = topic.length() + topicFilter.length() + message.getPayload().size();
        this.inboxPos = inboxPos;
    }

    public boolean isRetain() {
        return message.getIsRetained() || option.getRetainAsPublished() && message.getIsRetain();
    }

    public QoS qos() {
        return QoS.forNumber(Math.min(message.getPubQoS().getNumber(), option.getQos().getNumber()));
    }

    public int estBytes() {
        return bytesSize;
    }
}
