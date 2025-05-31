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

public enum MQTT5PubRelReasonCode {
    Success((byte) 0x00),
    PacketIdentifierNotFound((byte) 0x92);

    private final byte value;

    MQTT5PubRelReasonCode(byte value) {
        this.value = value;
    }

    public byte value() {
        return value;
    }

    public static MQTT5PubRelReasonCode valueOf(byte value) {
        return switch (value) {
            case (byte) 0x00 -> Success;
            case (byte) 0x92 -> PacketIdentifierNotFound;
            default -> throw new IllegalArgumentException("Invalid PubRel ReasonCode: " + value);
        };
    }
}
