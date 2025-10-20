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

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.AUTHENTICATION_DATA;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.AUTHENTICATION_METHOD;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.CONTENT_TYPE;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.CORRELATION_DATA;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.PAYLOAD_FORMAT_INDICATOR;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.REASON_STRING;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.RESPONSE_TOPIC;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.mqtt.spi.IUserPropsCustomizer;
import org.apache.bifromq.mqtt.spi.UserProperty;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.StringPair;
import org.apache.bifromq.type.UserProperties;

public class MQTT5MessageUtils {
    public static MqttPropertiesBuilder mqttProps() {
        return new MqttPropertiesBuilder();
    }

    public static MqttProperties.UserProperties toMqttUserProps(UserProperties userProperties) {
        MqttProperties.UserProperties userProps = new MqttProperties.UserProperties();
        for (StringPair stringPair : userProperties.getUserPropertiesList()) {
            userProps.add(stringPair.getKey(), stringPair.getValue());
        }
        return userProps;
    }

    public static UserProperties toUserProperties(MqttProperties mqttProperties) {
        return toUserPropertiesBuilder(mqttProperties).build();
    }

    @SuppressWarnings("unchecked")
    private static UserProperties.Builder toUserPropertiesBuilder(MqttProperties mqttProperties) {
        UserProperties.Builder userPropsBuilder = UserProperties.newBuilder();
        List<MqttProperties.UserProperty> userPropertyList = (List<MqttProperties.UserProperty>) mqttProperties
            .getProperties(MqttProperties.MqttPropertyType.USER_PROPERTY.value());
        if (!userPropertyList.isEmpty()) {
            userPropertyList.forEach(up -> userPropsBuilder.addUserProperties(
                StringPair.newBuilder().setKey(up.value().key).setValue(up.value().value).build()));
        }
        return userPropsBuilder;
    }

    public static boolean isUTF8Payload(MqttProperties mqttProperties) {
        return packetFormatIndicator(mqttProperties).map(i -> i == 1).orElse(false);
    }

    public static Optional<Integer> receiveMaximum(MqttProperties mqttProperties) {
        return integerMqttProperty(mqttProperties, MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM);
    }

    public static Optional<Integer> topicAliasMaximum(MqttProperties mqttProperties) {
        return integerMqttProperty(mqttProperties, MqttProperties.MqttPropertyType.TOPIC_ALIAS_MAXIMUM);
    }

    public static Optional<Integer> subscriptionIdentifier(MqttProperties mqttProperties) {
        return integerMqttProperty(mqttProperties, MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER);
    }

    public static Optional<Integer> maximumPacketSize(MqttProperties mqttProperties) {
        return integerMqttProperty(mqttProperties, MqttProperties.MqttPropertyType.MAXIMUM_PACKET_SIZE);
    }

    public static Optional<Integer> topicAlias(MqttProperties mqttProperties) {
        return integerMqttProperty(mqttProperties, MqttProperties.MqttPropertyType.TOPIC_ALIAS);
    }

    static Optional<Integer> packetFormatIndicator(MqttProperties mqttProperties) {
        return integerMqttProperty(mqttProperties, PAYLOAD_FORMAT_INDICATOR);
    }

    public static Optional<Integer> messageExpiryInterval(MqttProperties mqttProperties) {
        return integerMqttProperty(mqttProperties, MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL);
    }

    public static Optional<String> contentType(MqttProperties mqttProperties) {
        return stringMqttProperty(mqttProperties, CONTENT_TYPE);
    }

    public static Optional<String> responseTopic(MqttProperties mqttProperties) {
        return stringMqttProperty(mqttProperties, RESPONSE_TOPIC);
    }

    public static Optional<String> authMethod(MqttProperties mqttProperties) {
        return stringMqttProperty(mqttProperties, AUTHENTICATION_METHOD);
    }

    public static Optional<ByteString> authData(MqttProperties mqttProperties) {
        return binaryMqttProperty(mqttProperties, AUTHENTICATION_DATA);
    }

    public static boolean requestResponseInformation(MqttProperties mqttProperties) {
        return integerMqttProperty(mqttProperties, MqttProperties.MqttPropertyType.REQUEST_RESPONSE_INFORMATION)
            .orElse(0) == 1;
    }

    public static boolean requestProblemInformation(MqttProperties mqttProperties) {
        return integerMqttProperty(mqttProperties, MqttProperties.MqttPropertyType.REQUEST_PROBLEM_INFORMATION)
            .orElse(1) == 1;
    }

    public static Optional<String> reasonString(MqttProperties mqttProperties) {
        return stringMqttProperty(mqttProperties, REASON_STRING);
    }

    static Optional<Integer> integerMqttProperty(MqttProperties mqttProperties, MqttProperties.MqttPropertyType type) {
        return Optional.ofNullable((MqttProperties.IntegerProperty) mqttProperties.getProperty(type.value()))
            .map(MqttProperties.MqttProperty::value);
    }

    static Optional<String> stringMqttProperty(MqttProperties mqttProperties, MqttProperties.MqttPropertyType type) {
        return Optional.ofNullable((MqttProperties.StringProperty) mqttProperties.getProperty(type.value()))
            .map(MqttProperties.MqttProperty::value);
    }

    static Optional<ByteString> binaryMqttProperty(MqttProperties mqttProperties,
                                                   MqttProperties.MqttPropertyType type) {
        return Optional.ofNullable((MqttProperties.BinaryProperty) mqttProperties.getProperty(type.value()))
            .map(MqttProperties.MqttProperty::value)
            .map(UnsafeByteOperations::unsafeWrap);
    }

    static LWT toWillMessage(MqttConnectMessage connMsg,
                             ClientInfo clientInfo,
                             IUserPropsCustomizer userPropsCustomizer) {
        String willTopic = connMsg.payload().willTopic();
        QoS willQoS = QoS.forNumber(connMsg.variableHeader().willQos());
        ByteString willPayload = UnsafeByteOperations.unsafeWrap(connMsg.payload().willMessageInBytes());
        long now = HLC.INST.get();
        Iterable<UserProperty> extraUserProps = userPropsCustomizer.inbound(
            willTopic,
            willQoS,
            willPayload,
            clientInfo,
            now);
        LWT.Builder lwtBuilder = LWT.newBuilder()
            .setTopic(connMsg.payload().willTopic())
            .setDelaySeconds(integerMqttProperty(connMsg.payload().willProperties(),
                MqttProperties.MqttPropertyType.WILL_DELAY_INTERVAL).orElse(0));
        Message willMsg = toMessage(0,
            QoS.forNumber(connMsg.variableHeader().willQos()),
            connMsg.variableHeader().isWillRetain(),
            connMsg.payload().willProperties(),
            UnsafeByteOperations.unsafeWrap(connMsg.payload().willMessageInBytes()),
            now,
            extraUserProps);
        return lwtBuilder.setMessage(willMsg).build();
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
        MqttProperties mqttProperties = pubMsg.variableHeader().properties();
        return toMessage(pubMsgId, pubQoS, isRetain, mqttProperties, payload, now, extraUserProps);
    }

    static Message toMessage(long packetId,
                             QoS pubQoS,
                             boolean isRetain,
                             MqttProperties mqttProperties,
                             ByteString payload,
                             long hlc,
                             Iterable<UserProperty> extraUserProps) {
        Message.Builder msgBuilder = Message.newBuilder()
            .setMessageId(packetId)
            .setPubQoS(pubQoS)
            .setPayload(payload)
            .setTimestamp(hlc)
            // If absent, the Application Message does not expire, we use Integer.MAX_VALUE to represent this.
            .setExpiryInterval(messageExpiryInterval(mqttProperties).orElse(Integer.MAX_VALUE))
            .setIsRetain(isRetain);
        // PacketFormatIndicator
        packetFormatIndicator(mqttProperties).ifPresent(integer -> msgBuilder.setIsUTF8String(integer == 1));
        // ContentType
        contentType(mqttProperties).ifPresent(msgBuilder::setContentType);
        // ResponseTopic
        responseTopic(mqttProperties).ifPresent(msgBuilder::setResponseTopic);
        // CorrelationData
        Optional<ByteString> correlationData = binaryMqttProperty(mqttProperties, CORRELATION_DATA);
        correlationData.ifPresent(msgBuilder::setCorrelationData);
        // UserProperty
        UserProperties.Builder userPropertiesBuilder = toUserPropertiesBuilder(mqttProperties);
        extraUserProps.forEach(pair -> userPropertiesBuilder.addUserProperties(StringPair.newBuilder()
            .setKey(pair.key())
            .setValue(pair.value())
            .build()));
        UserProperties userProperties = userPropertiesBuilder.build();
        if (userProperties.getUserPropertiesCount() > 0) {
            msgBuilder.setUserProperties(userProperties);
        }
        return msgBuilder.build();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class MqttPropertiesBuilder {
        private final MqttProperties mqttProperties = new MqttProperties();

        public MqttPropertiesBuilder addSubscriptionIdentifier(int value) {
            mqttProperties.add(
                new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER.value(),
                    value));
            return this;
        }

        public MqttPropertiesBuilder addReceiveMaximum(int value) {
            mqttProperties.add(
                new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value(), value));
            return this;
        }

        public MqttPropertiesBuilder addTopicAliasMaximum(int value) {
            mqttProperties.add(
                new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.TOPIC_ALIAS_MAXIMUM.value(), value));
            return this;
        }

        public MqttPropertiesBuilder addMaximumPacketSize(int value) {
            mqttProperties.add(
                new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.MAXIMUM_PACKET_SIZE.value(), value));
            return this;
        }

        public MqttPropertiesBuilder addSessionExpiryInterval(int value) {
            mqttProperties.add(
                new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value(),
                    value));
            return this;
        }

        public MqttPropertiesBuilder addServerKeepAlive(int value) {
            mqttProperties.add(
                new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.SERVER_KEEP_ALIVE.value(),
                    value));
            return this;
        }

        public MqttPropertiesBuilder addServerReference(String value) {
            mqttProperties.add(
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.SERVER_REFERENCE.value(),
                    value));
            return this;
        }

        public MqttPropertiesBuilder addTopicAlias(int value) {
            mqttProperties.add(
                new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.TOPIC_ALIAS.value(), value));
            return this;
        }

        public MqttPropertiesBuilder addUserProperty(String key, String value) {
            mqttProperties.add(new MqttProperties.UserProperty(key, value));
            return this;
        }

        public MqttPropertiesBuilder addUserProperties(UserProperties userProperties) {
            userProperties.getUserPropertiesList().forEach(pair -> addUserProperty(pair.getKey(), pair.getValue()));
            return this;
        }

        public MqttPropertiesBuilder addPayloadFormatIndicator(int value) {
            mqttProperties.add(new MqttProperties.IntegerProperty(PAYLOAD_FORMAT_INDICATOR.value(), value));
            return this;
        }

        public MqttPropertiesBuilder addContentType(String value) {
            mqttProperties.add(new MqttProperties.StringProperty(CONTENT_TYPE.value(), value));
            return this;
        }

        public MqttPropertiesBuilder addResponseTopic(String value) {
            mqttProperties.add(new MqttProperties.StringProperty(RESPONSE_TOPIC.value(), value));
            return this;
        }

        public MqttPropertiesBuilder addCorrelationData(ByteString value) {
            mqttProperties.add(new MqttProperties.BinaryProperty(CORRELATION_DATA.value(), value.toByteArray()));
            return this;
        }

        public MqttPropertiesBuilder addReasonString(String value) {
            mqttProperties.add(new MqttProperties.StringProperty(REASON_STRING.value(), value));
            return this;
        }

        public MqttPropertiesBuilder addAuthMethod(String value) {
            mqttProperties.add(new MqttProperties.StringProperty(AUTHENTICATION_METHOD.value(), value));
            return this;
        }

        public MqttPropertiesBuilder addAuthData(ByteString value) {
            mqttProperties.add(new MqttProperties.BinaryProperty(AUTHENTICATION_DATA.value(), value.toByteArray()));
            return this;
        }

        public MqttPropertiesBuilder addRequestResponseInformation(boolean value) {
            mqttProperties.add(
                new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.REQUEST_RESPONSE_INFORMATION.value(),
                    value ? 1 : 0));
            return this;
        }

        public MqttPropertiesBuilder addRequestProblemInformation(boolean value) {
            mqttProperties.add(
                new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.REQUEST_PROBLEM_INFORMATION.value(),
                    value ? 1 : 0));
            return this;
        }

        public MqttPropertiesBuilder addMessageExpiryInterval(int value) {
            mqttProperties.add(
                new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL.value(),
                    value));
            return this;
        }

        public MqttProperties build() {
            return mqttProperties;
        }
    }
}
