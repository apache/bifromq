/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.inbox.server;

import static org.apache.bifromq.inbox.util.InboxServiceUtil.receiverId;

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.rpc.proto.SendRequest;
import org.apache.bifromq.plugin.subbroker.DeliveryPack;
import org.apache.bifromq.plugin.subbroker.DeliveryPackage;
import org.apache.bifromq.plugin.subbroker.DeliveryRequest;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.StringPair;
import org.apache.bifromq.type.TopicMessagePack;
import org.apache.bifromq.type.UserProperties;
import org.apache.bifromq.util.TopicUtil;
import com.google.protobuf.ByteString;

public class Fixtures {

    static SendRequest sendRequest() {
        Message message = Message.newBuilder()
            .setMessageId(1L)
            .setPubQoS(QoS.AT_MOST_ONCE)
            .setPayload(ByteString.EMPTY)
            .setTimestamp(HLC.INST.get())
            .setIsRetain(false)
            .setIsRetained(false)
            .setIsUTF8String(false)
            .setUserProperties(UserProperties.newBuilder()
                .addUserProperties(StringPair.newBuilder()
                    .setKey("foo_key")
                    .setValue("foo_val")
                    .build())
                .build())
            .build();
        TopicMessagePack.PublisherPack publisherPack = TopicMessagePack.PublisherPack.newBuilder()
            .setPublisher(ClientInfo
                .newBuilder()
                .setTenantId("iot_bar")
                .setType("type")
                .build())
            .addMessage(message)
            .build();
        TopicMessagePack topicMessagePack = TopicMessagePack.newBuilder()
            .setTopic("/foo/bar/baz")
            .addMessage(publisherPack)
            .build();
        DeliveryPack deliveryPack = DeliveryPack.newBuilder()
            .addMatchInfo(matchInfo())
            .setMessagePack(topicMessagePack)
            .build();
        DeliveryRequest deliveryRequest = DeliveryRequest.newBuilder()
            .putPackage("_", DeliveryPackage.newBuilder()
                .addPack(deliveryPack)
                .build())
            .build();

        return SendRequest.newBuilder()
            .setRequest(deliveryRequest)
            .build();
    }

    static MatchInfo matchInfo() {
        return MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from("/foo/+"))
            .setIncarnation(1L)
            .setReceiverId(receiverId("foo", 1L))
            .build();
    }

}