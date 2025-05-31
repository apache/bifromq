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

public enum MQTT5UnsubAckReasonCode {
    Success(0x00),
    NoSubscriptionExisted(0x11),
    UnspecifiedError(0x80),
    ImplementationSpecificError(0x83),
    NotAuthorized(0x87),
    TopicFilterInvalid(0x8F),
    PacketIdentifierInUse(0x91);
    private final short value;

    MQTT5UnsubAckReasonCode(int value) {
        this.value = (short) value;
    }

    public short value() {
        return value;
    }

    public static MQTT5UnsubAckReasonCode valueOf(short value) {
        return switch (value) {
            case 0x00 -> Success;
            case 0x11 -> NoSubscriptionExisted;
            case 0x80 -> UnspecifiedError;
            case 0x83 -> ImplementationSpecificError;
            case 0x87 -> NotAuthorized;
            case 0x8F -> TopicFilterInvalid;
            case 0x91 -> PacketIdentifierInUse;
            default -> throw new IllegalArgumentException("Invalid UnsubAck ReasonCode: " + value);
        };
    }

}
