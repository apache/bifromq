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

package org.apache.bifromq.mqtt.integration.v5.client;

import com.google.protobuf.ByteString;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

public class MqttMsg {
    public final int id;
    public final int qos;
    public final String topic;
    public final ByteString payload;
    public final boolean isRetain;
    public final boolean isDup;
    public final MqttProperties mqttProperties;

    MqttMsg(String topic, MqttMessage message) {
        id = message.getId();
        this.topic = topic;
        payload = ByteString.copyFrom(message.getPayload());
        isDup = message.isDuplicate();
        isRetain = message.isRetained();
        qos = message.getQos();
        mqttProperties = message.getProperties();
    }
}
