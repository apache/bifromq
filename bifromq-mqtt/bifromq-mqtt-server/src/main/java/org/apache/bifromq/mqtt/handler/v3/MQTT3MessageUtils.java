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

package org.apache.bifromq.mqtt.handler.v3;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.mqtt.spi.IUserPropsCustomizer;
import org.apache.bifromq.mqtt.spi.UserProperty;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.StringPair;
import org.apache.bifromq.type.UserProperties;

public final class MQTT3MessageUtils {
    static LWT toWillMessage(MqttConnectMessage connMsg,
                             ClientInfo publisher,
                             IUserPropsCustomizer userPropsCustomizer) {
        String willTopic = connMsg.payload().willTopic();
        QoS willQoS = QoS.forNumber(connMsg.variableHeader().willQos());
        ByteString willPayload = UnsafeByteOperations.unsafeWrap(connMsg.payload().willMessageInBytes());
        long now = HLC.INST.get();
        Iterable<UserProperty> extraUserProps = userPropsCustomizer.inbound(
            willTopic,
            willQoS,
            willPayload,
            publisher,
            now
        );
        Message willMsg = toMessage(0, willQoS, connMsg.variableHeader().isWillRetain(),
            willPayload, now, extraUserProps);
        return LWT.newBuilder()
            .setTopic(willTopic)
            .setMessage(willMsg)
            .build();
    }

    static Message toMessage(MqttPublishMessage pubMsg,
                             ClientInfo publisher,
                             IUserPropsCustomizer userPropsCustomizer) {
        String topic = pubMsg.variableHeader().topicName();
        QoS pubQoS = QoS.forNumber(pubMsg.fixedHeader().qosLevel().value());
        ByteString payload = ByteString.copyFrom(pubMsg.payload().nioBuffer());
        long now = HLC.INST.get();
        Iterable<UserProperty> extraUserProps = userPropsCustomizer.inbound(
            topic,
            pubQoS,
            payload,
            publisher,
            now);
        long pubMsgId = pubMsg.variableHeader().packetId();
        boolean isRetain = pubMsg.fixedHeader().isRetain();
        return toMessage(pubMsgId, pubQoS, isRetain, payload, now, extraUserProps);
    }

    static Message toMessage(long packetId,
                             QoS pubQoS,
                             boolean isRetain,
                             ByteString payload,
                             long hlc,
                             Iterable<UserProperty> extraUserProps) {
        UserProperties.Builder userPropsBuilder = UserProperties.newBuilder();
        for (UserProperty userProp : extraUserProps) {
            userPropsBuilder.addUserProperties(StringPair.newBuilder()
                .setKey(userProp.key())
                .setValue(userProp.value())
                .build());
        }
        return Message.newBuilder()
            .setMessageId(packetId)
            .setPubQoS(pubQoS)
            .setPayload(payload)
            .setTimestamp(hlc)
            // MessageExpiryInterval
            .setExpiryInterval(Integer.MAX_VALUE)
            .setIsRetain(isRetain)
            .setUserProperties(userPropsBuilder.build())
            .build();
    }
}
