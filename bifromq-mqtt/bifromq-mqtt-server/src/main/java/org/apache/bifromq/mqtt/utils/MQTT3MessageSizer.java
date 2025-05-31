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

package org.apache.bifromq.mqtt.utils;

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribePayload;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MQTT3MessageSizer implements IMQTTMessageSizer {
    public static final IMQTTMessageSizer INSTANCE = new MQTT3MessageSizer();

    public IMQTTMessageSizer.MqttMessageSize sizeOf(MqttMessage message) {
        switch (message.fixedHeader().messageType()) {
            case CONNECT -> {
                MqttConnectMessage connMsg = ((MqttConnectMessage) message);
                return new MqttMessageSize(10, sizeConnPayload(connMsg));
            }
            case PUBLISH -> {
                MqttPublishMessage pubMsg = (MqttPublishMessage) message;
                return new MqttMessageSize(sizePubVarHeader(pubMsg.variableHeader()), pubMsg.payload().readableBytes());
            }
            case SUBSCRIBE -> {
                MqttSubscribeMessage subMsg = (MqttSubscribeMessage) message;
                return new MqttMessageSize(2, sizeSubPayload(subMsg.payload()));
            }
            case SUBACK -> {
                MqttSubAckMessage subAckMsg = (MqttSubAckMessage) message;
                return new MqttMessageSize(2, sizeSubAckPayload(subAckMsg.payload()));
            }
            case UNSUBSCRIBE -> {
                MqttUnsubscribeMessage unsubMsg = (MqttUnsubscribeMessage) message;
                return new MqttMessageSize(2, sizeUnsubPayload(unsubMsg.payload()));
            }
            case CONNACK, UNSUBACK, PUBACK, PUBREC, PUBREL, PUBCOMP -> {
                return TWO_BYTES_REMAINING_LENGTH;
            }
            case DISCONNECT, PINGREQ, PINGRESP -> {
                return ZERO_BYTES_REMAINING_LENGTH;
            }
            default -> {
                log.error("Unknown message type for sizing: {}", message.fixedHeader().messageType());
                return ZERO_BYTES_REMAINING_LENGTH;
            }
        }
    }

    @Override
    public int lastWillSize(MqttConnectMessage message) {
        MqttConnectPayload payload = message.payload();
        int size = 0;
        if (message.variableHeader().isWillFlag()) {
            size += IMQTTMessageSizer.sizeUTF8EncodedString(payload.willTopic());
            size += IMQTTMessageSizer.sizeBinary(payload.willMessageInBytes());
        }
        return size;
    }

    private int sizeConnPayload(MqttConnectMessage message) {
        MqttConnectPayload payload = message.payload();
        int clientIdBytes = IMQTTMessageSizer.sizeUTF8EncodedString(payload.clientIdentifier());
        int usernameBytes =
            message.variableHeader().hasUserName() ? IMQTTMessageSizer.sizeUTF8EncodedString(payload.userName()) : 0;
        int passwordBytes =
            message.variableHeader().hasPassword() ? IMQTTMessageSizer.sizeBinary(payload.passwordInBytes()) : 0;
        int payloadSize = clientIdBytes + usernameBytes + passwordBytes;
        payloadSize += lastWillSize(message);
        return payloadSize;
    }

    private int sizePubVarHeader(MqttPublishVariableHeader header) {
        int topicNameBytes = IMQTTMessageSizer.sizeUTF8EncodedString(header.topicName());
        // A PUBLISH packet MUST NOT contain a Packet Identifier if its QoS value is set to 0 [MQTT5-2.2.1-2]
        int packetIdBytes = header.packetId() == 0 ? 0 : 2;
        return topicNameBytes + packetIdBytes;
    }

    private int sizeSubPayload(MqttSubscribePayload payload) {
        int totalBytes = 0;
        for (MqttTopicSubscription sub : payload.topicSubscriptions()) {
            // 1 byte for encoding sub qos
            totalBytes += 1 + IMQTTMessageSizer.sizeUTF8EncodedString(sub.topicFilter());
        }
        return totalBytes;
    }

    private int sizeUnsubPayload(MqttUnsubscribePayload payload) {
        int totalBytes = 0;
        for (String topicFilter : payload.topics()) {
            totalBytes += IMQTTMessageSizer.sizeUTF8EncodedString(topicFilter);
        }
        return totalBytes;
    }

    private int sizeSubAckPayload(MqttSubAckPayload payload) {
        // 1 byte for each reason code
        return payload.reasonCodes().size();
    }

    private record MqttMessageSize(int varHeaderBytes, int payloadBytes)
        implements IMQTTMessageSizer.MqttMessageSize {
        public int encodedBytes() {
            return encodedBytes(true, true);
        }

        public int encodedBytes(boolean includeUserProps, boolean includeReasonString) {
            return 1 + IMQTTMessageSizer.varIntBytes(varHeaderBytes + payloadBytes) + varHeaderBytes + payloadBytes;
        }
    }
}
