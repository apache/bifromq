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

package org.apache.bifromq.sessiondict.server;

import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;

import org.apache.bifromq.type.ClientInfo;

record MqttClientKey(String userId, String clientId) {
    static MqttClientKey from(ClientInfo clientInfo) {
        String userId = clientInfo.getMetadataOrDefault(MQTT_USER_ID_KEY, "");
        String mqttClientId = clientInfo.getMetadataOrDefault(MQTT_CLIENT_ID_KEY, "");
        return new MqttClientKey(userId, mqttClientId);
    }
}
