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

package org.apache.bifromq.mqtt.utils;

import static io.netty.buffer.ByteBufUtil.utf8Bytes;

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;

/**
 * The interface for calculating the size of the mqtt message.
 */
public interface IMQTTMessageSizer {
    int MIN_CONTROL_PACKET_SIZE = 16;
    IMQTTMessageSizer.MqttMessageSize TWO_BYTES_REMAINING_LENGTH =
        new IMQTTMessageSizer.MqttMessageSize() {
            @Override
            public int encodedBytes() {
                return 4;
            }

            @Override
            public int encodedBytes(boolean includeUserProps, boolean includeReasonString) {
                return 4;
            }
        };
    IMQTTMessageSizer.MqttMessageSize ZERO_BYTES_REMAINING_LENGTH =
        new IMQTTMessageSizer.MqttMessageSize() {
            @Override
            public int encodedBytes() {
                return 2;
            }

            @Override
            public int encodedBytes(boolean includeUserProps, boolean includeReasonString) {
                return 2;
            }
        };

    static IMQTTMessageSizer mqtt3() {
        return MQTT3MessageSizer.INSTANCE;
    }

    static IMQTTMessageSizer mqtt5() {
        return MQTT5MessageSizer.INSTANCE;
    }

    static int sizeBinary(byte[] binary) {
        // 2 bytes for encoding size prefix in Binary Data, [MQTT5-1.5.6]
        return 2 + binary.length;
    }

    static int sizeUTF8EncodedString(String s) {
        int rsBytes = utf8Bytes(s);
        // 2 bytes for encoding size prefix in UTF-8 Encoded String, [MQTT5-1.5.7]
        return 2 + rsBytes;
    }

    static int varIntBytes(int i) {
        int bytes = 0;
        do {
            int digit = i % 128;
            i /= 128;
            if (i > 0) {
                digit |= 0x80;
            }
            bytes++;
        } while (i > 0);
        return bytes;
    }

    MqttMessageSize sizeOf(MqttMessage message);

    int lastWillSize(MqttConnectMessage message);

    /**
     * Calculate the size of the message by the fixed header from decoded mqtt message.
     *
     * @param header the fixed header of the message
     * @return the size of the message
     */
    default int sizeByHeader(MqttFixedHeader header) {
        int remainingLength = header.remainingLength();
        int fixedHeaderSize = 1;
        int remainingLengthSize = 1; // at least one byte
        if (remainingLength > 127) {
            remainingLengthSize = 2;
            if (remainingLength > 16383) {
                remainingLengthSize = 3;
                if (remainingLength > 2097151) {
                    remainingLengthSize = 4;
                }
            }
        }
        return fixedHeaderSize + remainingLengthSize + remainingLength;
    }

    interface MqttMessageSize {
        int encodedBytes();

        int encodedBytes(boolean includeUserProps, boolean includeReasonString);
    }
}
