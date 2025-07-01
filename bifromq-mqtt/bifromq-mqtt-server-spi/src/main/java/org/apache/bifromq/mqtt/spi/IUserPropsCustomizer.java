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

package org.apache.bifromq.mqtt.spi;

import com.google.protobuf.ByteString;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicFilterOption;

/**
 * Interface for customizing UserProperties for inbound and outbound MQTT messages.
 * Implementations can provide extra properties that will be included in the UserProperties of the messages.
 * <br>
 * <strong>NOTE:</strong>
 * <ol>
 *   <li>User properties are subject to the maximum packet size, so customization should not exceed the limits.</li>
 *   <li>The implementation should be thread-safe as it may be called concurrently for different messages.</li>
 * </ol>
 */
public interface IUserPropsCustomizer {
    /**
     * Customize the extra UserProperties for an inbound message.
     *
     * @param topic The topic of the message.
     * @param pubQoS The QoS of the message.
     * @param payload The payload of the message.
     * @param publisher The client information of the publisher.
     * @param hlc The HLC timestamp of the message when inbound.
     * @return Customized the additional UserProperties for the inbound message.
     */
    Iterable<UserProperty> inbound(String topic, QoS pubQoS, ByteString payload, ClientInfo publisher, long hlc);

    /**
     * Customize the extra UserProperties for an outbound message.
     * Note.
     *
     * @param topic The topic of the message.
     * @param message The message being sent.
     * @param publisher The client information of the publisher.
     * @param topicFilter The topic filter used for subscription.
     * @param option The topic filter option.
     * @param subscriber The client information of the subscriber.
     * @param hlc The HLC timestamp of the message when outbound.
     * @return Customized the additional UserProperties for the outbound message.
     */
    Iterable<UserProperty> outbound(String topic,
                                    Message message,
                                    ClientInfo publisher,
                                    String topicFilter,
                                    TopicFilterOption option,
                                    ClientInfo subscriber,
                                    long hlc);
}
