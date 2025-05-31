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

public enum MQTT5PubRecReasonCode {
    Success((byte) 0x00),
    NoMatchingSubscribers((byte) 0x10),
    UnspecifiedError((byte) 0x80),
    ImplementationSpecificError((byte) 0x83),
    NotAuthorized((byte) 0x87),
    TopicNameInvalid((byte) 0x90),
    PacketIdentifierInUse((byte) 0x91),
    QuotaExceeded((byte) 0x97),
    PayloadFormatInvalid((byte) 0x99);

    private final byte value;

    MQTT5PubRecReasonCode(byte value) {
        this.value = value;
    }

    public byte value() {
        return value;
    }

    public static MQTT5PubRecReasonCode valueOf(byte value) {
        return switch (value) {
            case (byte) 0x00 -> Success;
            case (byte) 0x10 -> NoMatchingSubscribers;
            case (byte) 0x80 -> UnspecifiedError;
            case (byte) 0x83 -> ImplementationSpecificError;
            case (byte) 0x87 -> NotAuthorized;
            case (byte) 0x90 -> TopicNameInvalid;
            case (byte) 0x91 -> PacketIdentifierInUse;
            case (byte) 0x97 -> QuotaExceeded;
            case (byte) 0x99 -> PayloadFormatInvalid;
            default -> throw new IllegalArgumentException("Invalid PubRec ReasonCode: " + value);
        };
    }
}
