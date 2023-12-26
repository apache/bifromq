/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.mqtt.handler.v5;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import java.util.List;

public class MQTT5MessageUtils {

    static MqttSubAckMessage toMqttSubAckMessage(int packetId, List<Integer> ackedQoSs) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK,
            false,
            MqttQoS.AT_MOST_ONCE,
            false,
            0);
        MqttMessageIdVariableHeader mqttMessageIdVariableHeader = MqttMessageIdVariableHeader.from(packetId);
        io.netty.handler.codec.mqtt.MqttSubAckPayload mqttSubAckPayload = new MqttSubAckPayload(ackedQoSs);
        return new MqttSubAckMessage(mqttFixedHeader,
            mqttMessageIdVariableHeader,
            mqttSubAckPayload);
    }

    static MqttUnsubAckMessage toMqttUnsubAckMessage(int messageId) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK,
            false,
            MqttQoS.AT_MOST_ONCE,
            false,
            0);
        MqttMessageIdVariableHeader mqttMessageIdVariableHeader = MqttMessageIdVariableHeader.from(messageId);
        return new MqttUnsubAckMessage(mqttFixedHeader, mqttMessageIdVariableHeader);
    }
}
