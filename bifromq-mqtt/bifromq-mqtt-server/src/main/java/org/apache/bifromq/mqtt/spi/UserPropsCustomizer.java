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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicFilterOption;

@Slf4j
class UserPropsCustomizer implements IUserPropsCustomizer {
    private final IUserPropsCustomizer delegate;

    UserPropsCustomizer(List<IUserPropsCustomizer> customizers) {
        if (customizers.isEmpty()) {
            this.delegate = new DummyUserPropsCustomizer();
        } else {
            this.delegate = new AggregatedUserPropsCustomizer(customizers);
        }
    }

    @Override
    public Iterable<UserProperty> inbound(String topic,
                                          QoS pubQoS,
                                          ByteString payload,
                                          ClientInfo publisher,
                                          long hlc) {
        return delegate.inbound(topic, pubQoS, payload, publisher, hlc);
    }

    @Override
    public Iterable<UserProperty> outbound(String topic,
                                           Message message,
                                           ClientInfo publisher,
                                           String topicFilter,
                                           TopicFilterOption option,
                                           ClientInfo subscriber,
                                           long hlc) {
        return delegate.outbound(topic, message, publisher, topicFilter, option, subscriber, hlc);
    }

    private static class DummyUserPropsCustomizer implements IUserPropsCustomizer {
        @Override
        public Iterable<UserProperty> inbound(String topic, QoS pubQoS, ByteString payload,
                                              ClientInfo publisher, long hlc) {
            return Collections.emptyList();
        }

        @Override
        public Iterable<UserProperty> outbound(String topic, Message message, ClientInfo publisher,
                                               String topicFilter, TopicFilterOption option,
                                               ClientInfo subscriber, long hlc) {
            return Collections.emptyList();
        }
    }

    private static class AggregatedUserPropsCustomizer implements IUserPropsCustomizer {
        private final List<IUserPropsCustomizer> customizers;

        private AggregatedUserPropsCustomizer(List<IUserPropsCustomizer> customizers) {
            this.customizers = customizers;
        }

        @Override
        public Iterable<UserProperty> inbound(String topic, QoS pubQoS, ByteString payload,
                                              ClientInfo publisher, long hlc) {
            return Iterables.concat(Lists.transform(customizers, customizer -> {
                try {
                    return customizer.inbound(topic, pubQoS, payload, publisher, hlc);
                } catch (Throwable e) {
                    log.error("Error customizing inbound user properties for topic: {}", topic, e);
                    return Collections.emptyList();
                }
            }));
        }

        @Override
        public Iterable<UserProperty> outbound(String topic, Message message, ClientInfo publisher,
                                               String topicFilter, TopicFilterOption option,
                                               ClientInfo subscriber, long hlc) {
            return Iterables.concat(Lists.transform(customizers, customizer -> {
                try {
                    return customizer.outbound(topic, message, publisher, topicFilter, option,
                        subscriber, hlc);
                } catch (Throwable e) {
                    log.error("Error customizing outbound user properties for topic: {}", topic, e);
                    return Collections.emptyList();
                }
            }));
        }
    }
}
