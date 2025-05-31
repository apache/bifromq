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

public enum MQTT5SubAckReasonCode {
    GrantedQoS0(0),
    GrantedQoS1(1),
    GrantedQoS2(2),
    UnspecifiedError(0x80),
    ImplementationSpecificError(0x83),
    NotAuthorized(0x87),
    TopicFilterInvalid(0x8F),
    PacketIdentifierInUse(0x91),
    QuotaExceeded(0x97),
    SharedSubscriptionsNotSupported(0x9E),
    SubscriptionIdentifierNotSupported(0xA1),
    WildcardSubscriptionsNotSupported(0xA2);
    private final int value;

    MQTT5SubAckReasonCode(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public static MQTT5SubAckReasonCode valueOf(int value) {
        return switch (value) {
            case 0 -> GrantedQoS0;
            case 1 -> GrantedQoS1;
            case 2 -> GrantedQoS2;
            case 0x80 -> UnspecifiedError;
            case 0x83 -> ImplementationSpecificError;
            case 0x87 -> NotAuthorized;
            case 0x8F -> TopicFilterInvalid;
            case 0x91 -> PacketIdentifierInUse;
            case 0x97 -> QuotaExceeded;
            case 0x9E -> SharedSubscriptionsNotSupported;
            case 0xA1 -> SubscriptionIdentifierNotSupported;
            case 0xA2 -> WildcardSubscriptionsNotSupported;
            default -> throw new IllegalArgumentException("Invalid SubAck ReasonCode: " + value);
        };
    }

}
