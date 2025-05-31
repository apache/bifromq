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

package org.apache.bifromq.mqtt.handler.v5.reason;

public enum MQTT5DisconnectReasonCode {
    Normal((byte) 0),
    DisconnectWithWillMessage((byte) 0x04),
    UnspecifiedError((byte) 0x80),
    MalformedPacket((byte) 0x81),
    ProtocolError((byte) 0x82),
    ImplementationSpecificError((byte) 0x83),
    NotAuthorized((byte) 0x87),
    ServerBusy((byte) 0x89),
    ServerShuttingDown((byte) 0x8B),
    KeepAliveTimeout((byte) 0x8D),
    SessionTakenOver((byte) 0x8E),
    TopicFilterInvalid((byte) 0x8F),
    TopicNameInvalid((byte) 0x90),
    ReceiveMaximumExceeded((byte) 0x93),
    TopicAliasInvalid((byte) 0x94),
    PacketTooLarge((byte) 0x95),
    MessageRateToHigh((byte) 0x96),
    QuotaExceeded((byte) 0x97),
    AdministrativeAction((byte) 0x98),
    PayloadFormatInvalid((byte) 0x99),
    RetainNotSupported((byte) 0x9A),
    QoSNotSupported((byte) 0x9B),
    UseAnotherServer((byte) 0x9C),
    ServerMoved((byte) 0x9D),
    SharedSubscriptionsNotSupported((byte) 0x9E),
    ConnectionRateExceeded((byte) 0x9F),
    MaximumConnectTime((byte) 0xA0),
    SubscriptionIdentifierNotSupported((byte) 0xA1),
    WildcardSubscriptionsNotSupported((byte) 0xA2);
    private final byte value;

    MQTT5DisconnectReasonCode(byte value) {
        this.value = value;
    }

    public byte value() {
        return value;
    }

    public static MQTT5DisconnectReasonCode valueOf(byte value) {
        return switch (value) {
            case (byte) 0 -> Normal;
            case (byte) 0x04 -> DisconnectWithWillMessage;
            case (byte) 0x80 -> UnspecifiedError;
            case (byte) 0x81 -> MalformedPacket;
            case (byte) 0x82 -> ProtocolError;
            case (byte) 0x83 -> ImplementationSpecificError;
            case (byte) 0x87 -> NotAuthorized;
            case (byte) 0x89 -> ServerBusy;
            case (byte) 0x8B -> ServerShuttingDown;
            case (byte) 0x8D -> KeepAliveTimeout;
            case (byte) 0x8E -> SessionTakenOver;
            case (byte) 0x8F -> TopicFilterInvalid;
            case (byte) 0x90 -> TopicNameInvalid;
            case (byte) 0x93 -> ReceiveMaximumExceeded;
            case (byte) 0x94 -> TopicAliasInvalid;
            case (byte) 0x95 -> PacketTooLarge;
            case (byte) 0x96 -> MessageRateToHigh;
            case (byte) 0x97 -> QuotaExceeded;
            case (byte) 0x98 -> AdministrativeAction;
            case (byte) 0x99 -> PayloadFormatInvalid;
            case (byte) 0x9A -> RetainNotSupported;
            case (byte) 0x9B -> QoSNotSupported;
            case (byte) 0x9C -> UseAnotherServer;
            case (byte) 0x9D -> ServerMoved;
            case (byte) 0x9E -> SharedSubscriptionsNotSupported;
            case (byte) 0x9F -> ConnectionRateExceeded;
            case (byte) 0xA0 -> MaximumConnectTime;
            case (byte) 0xA1 -> SubscriptionIdentifierNotSupported;
            case (byte) 0xA2 -> WildcardSubscriptionsNotSupported;
            default -> throw new IllegalArgumentException("Invalid Disconnect ReasonCode: " + value);
        };
    }
}
