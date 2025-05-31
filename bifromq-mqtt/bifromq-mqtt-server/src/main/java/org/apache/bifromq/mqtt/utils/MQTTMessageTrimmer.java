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
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckPayload;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;

public class MQTTMessageTrimmer {
    public static MqttMessage trim(MqttMessage message, boolean trimReasonString, boolean trimUserProps) {
        return switch (message.fixedHeader().messageType()) {
            case CONNECT -> {
                MqttConnectMessage connMsg = (MqttConnectMessage) message;
                yield new MqttConnectMessage(connMsg.fixedHeader(),
                    trimVarHeader(connMsg.variableHeader(), trimReasonString, trimUserProps), connMsg.payload());
            }
            case SUBSCRIBE -> {
                MqttSubscribeMessage subMsg = (MqttSubscribeMessage) message;
                if (subMsg.variableHeader() instanceof MqttMessageIdAndPropertiesVariableHeader header) {
                    yield new MqttSubscribeMessage(subMsg.fixedHeader(),
                        trimVarHeader(header, trimReasonString, trimUserProps), subMsg.payload());
                }
                yield subMsg;
            }
            case UNSUBSCRIBE -> {
                MqttUnsubscribeMessage unsubMsg = (MqttUnsubscribeMessage) message;
                if (unsubMsg.variableHeader() instanceof MqttMessageIdAndPropertiesVariableHeader header) {
                    yield new MqttUnsubscribeMessage(unsubMsg.fixedHeader(),
                        trimVarHeader(header, trimReasonString, trimUserProps), unsubMsg.payload());
                }
                yield unsubMsg;
            }
            case SUBACK -> {
                MqttSubAckMessage subAckMsg = (MqttSubAckMessage) message;
                if (subAckMsg.variableHeader() instanceof MqttMessageIdAndPropertiesVariableHeader header) {
                    yield new MqttSubAckMessage(subAckMsg.fixedHeader(),
                        trimVarHeader(header, trimReasonString, trimUserProps), subAckMsg.payload());
                }
                yield subAckMsg;
            }
            case UNSUBACK -> {
                if (message.variableHeader() instanceof MqttMessageIdAndPropertiesVariableHeader header) {
                    yield new MqttUnsubAckMessage(
                        message.fixedHeader(),
                        trimVarHeader(header, trimReasonString, trimUserProps),
                        (MqttUnsubAckPayload) message.payload());
                }
                yield message;
            }
            case PUBACK, PUBREC, PUBREL, PUBCOMP -> {
                if (message.variableHeader() instanceof MqttPubReplyMessageVariableHeader) {
                    yield new MqttMessage(message.fixedHeader(),
                        trimVarHeader((MqttPubReplyMessageVariableHeader) message.variableHeader(),
                            trimReasonString,
                            trimUserProps));
                }
                yield message;
            }
            case AUTH -> new MqttMessage(message.fixedHeader(),
                trimVarHeader((MqttReasonCodeAndPropertiesVariableHeader) message.variableHeader(), trimReasonString,
                    trimUserProps));
            default -> message; // don't trim ConnAck/Disconnect/Publish
        };
    }

    private static MqttConnectVariableHeader trimVarHeader(MqttConnectVariableHeader header,
                                                           boolean trimReasonString,
                                                           boolean trimUserProps) {
        return new MqttConnectVariableHeader(
            header.name(),
            header.version(),
            header.hasUserName(),
            header.hasPassword(),
            header.isWillRetain(),
            header.willQos(),
            header.isWillFlag(),
            header.isCleanSession(),
            header.keepAliveTimeSeconds(),
            trimMqttProps(header.properties(), trimReasonString, trimUserProps));
    }

    private static MqttPubReplyMessageVariableHeader trimVarHeader(MqttPubReplyMessageVariableHeader header,
                                                                   boolean trimReasonString,
                                                                   boolean trimUserProps) {
        return new MqttPubReplyMessageVariableHeader(
            header.messageId(),
            header.reasonCode(),
            trimMqttProps(header.properties(), trimReasonString, trimUserProps));
    }

    private static MqttMessageIdAndPropertiesVariableHeader trimVarHeader(
        MqttMessageIdAndPropertiesVariableHeader header,
        boolean trimReasonString,
        boolean trimUserProps) {
        return new MqttMessageIdAndPropertiesVariableHeader(
            header.messageId(),
            trimMqttProps(header.properties(), trimReasonString, trimUserProps));
    }

    private static MqttReasonCodeAndPropertiesVariableHeader trimVarHeader(
        MqttReasonCodeAndPropertiesVariableHeader header,
        boolean trimReasonString,
        boolean trimUserProps) {
        return new MqttReasonCodeAndPropertiesVariableHeader(
            header.reasonCode(),
            trimMqttProps(header.properties(), trimReasonString, trimUserProps));

    }

    private static MqttProperties trimMqttProps(MqttProperties props,
                                                boolean trimReasonString,
                                                boolean trimUserProps) {
        MqttProperties trimmed = new MqttProperties();
        for (MqttProperties.MqttProperty<?> prop : props.listAll()) {
            switch (MqttProperties.MqttPropertyType.valueOf(prop.propertyId())) {
                case REASON_STRING -> {
                    if (!trimReasonString) {
                        trimmed.add(prop);
                    }
                }
                case USER_PROPERTY -> {
                    if (!trimUserProps) {
                        trimmed.add(prop);
                    }
                }
                default -> trimmed.add(prop);
            }
        }
        return trimmed;
    }
}
