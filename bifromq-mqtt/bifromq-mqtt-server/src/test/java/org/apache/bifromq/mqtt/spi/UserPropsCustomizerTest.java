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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.List;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicFilterOption;
import org.testng.annotations.Test;


public class UserPropsCustomizerTest {

    @Test
    void testDummyUserPropsCustomizer() {
        UserPropsCustomizer customizer = new UserPropsCustomizer(Collections.emptyList());
        Iterable<UserProperty> inbound = customizer.inbound("topic", QoS.AT_MOST_ONCE,
            ByteString.EMPTY, ClientInfo.getDefaultInstance(), System.currentTimeMillis());
        assertFalse(inbound.iterator().hasNext());

        Iterable<UserProperty> outbound = customizer.outbound("topic",
            Message.getDefaultInstance(), ClientInfo.getDefaultInstance(), "filter",
            TopicFilterOption.getDefaultInstance(), ClientInfo.getDefaultInstance(), System.currentTimeMillis());
        assertFalse(outbound.iterator().hasNext());
    }

    @Test
    void testAggregatedUserPropsCustomizer() {
        IUserPropsCustomizer first = new IUserPropsCustomizer() {
            @Override
            public Iterable<UserProperty> inbound(String topic, QoS pubQoS, ByteString payload,
                                                  ClientInfo publisher, long hlc) {
                return List.of(new UserProperty("k1", "v1"));
            }

            @Override
            public Iterable<UserProperty> outbound(String topic, Message message, ClientInfo publisher,
                                                   String topicFilter, TopicFilterOption option,
                                                   ClientInfo subscriber, long hlc) {
                return List.of(new UserProperty("k2", "v2"));
            }
        };

        IUserPropsCustomizer second = new IUserPropsCustomizer() {
            @Override
            public Iterable<UserProperty> inbound(String topic, QoS pubQoS, ByteString payload,
                                                  ClientInfo publisher, long hlc) {
                return List.of(new UserProperty("k3", "v3"));
            }

            @Override
            public Iterable<UserProperty> outbound(String topic, Message message, ClientInfo publisher,
                                                   String topicFilter, TopicFilterOption option,
                                                   ClientInfo subscriber, long hlc) {
                return List.of(new UserProperty("k4", "v4"));
            }
        };

        UserPropsCustomizer customizer = new UserPropsCustomizer(List.of(first, second));

        List<UserProperty> inbound = Lists.newArrayList(customizer.inbound("topic", QoS.AT_MOST_ONCE,
            ByteString.EMPTY, ClientInfo.getDefaultInstance(), System.currentTimeMillis()));
        assertEquals(inbound.size(), 2);

        List<UserProperty> outbound = Lists.newArrayList(customizer.outbound("topic",
            Message.getDefaultInstance(), ClientInfo.getDefaultInstance(), "filter",
            TopicFilterOption.getDefaultInstance(), ClientInfo.getDefaultInstance(), System.currentTimeMillis()));
        assertEquals(outbound.size(), 2);
    }
}
