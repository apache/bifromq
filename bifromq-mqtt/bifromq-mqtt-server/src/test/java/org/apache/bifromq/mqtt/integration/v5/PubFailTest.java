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

package org.apache.bifromq.mqtt.integration.v5;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5DisconnectReasonCode;
import org.apache.bifromq.mqtt.integration.MQTTTest;
import org.apache.bifromq.mqtt.integration.v5.client.MqttTestClient;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Granted;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthResult;
import org.apache.bifromq.plugin.authprovider.type.Ok;
import org.apache.bifromq.plugin.authprovider.type.Success;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.testng.annotations.Test;

public class PubFailTest extends MQTTTest {
    private final String userId = "userId";

    protected void doSetup(Method method) {
        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId(tenantId)
                    .setUserId(userId)
                    .build())
                .build()));
        when(authProvider.auth(any(MQTT5AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT5AuthResult.newBuilder()
                .setSuccess(Success.newBuilder()
                    .setTenantId(tenantId)
                    .setUserId(userId)
                    .build())
                .build()));
        when(authProvider.checkPermission(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(CheckResult.newBuilder()
                .setGranted(Granted.newBuilder().build())
                .build()));
    }

    @Test(groups = "integration")
    public void pubDupQoS0() {
        String topicFilter = "abc";
        MqttConnectionOptions connOpts = new MqttConnectionOptions();
        connOpts.setCleanStart(true);
        connOpts.setSessionExpiryInterval(0L);
        connOpts.setUserName(tenantId + "/" + userId);

        MqttTestClient client = new MqttTestClient(BROKER_URI);
        client.connect(connOpts);
        MqttMessage message = new MqttMessage();
        // qos0 and dup set to true is prohibited by MQTT 5.0
        message.setQos(0);
        message.setDuplicate(true);
        message.setPayload("hello".getBytes());
        client.publish(topicFilter, message);
        MqttDisconnectResponse disconnectResponse = client.onDisconnect().blockingGet();
        assertEquals((byte) disconnectResponse.getReturnCode(), MQTT5DisconnectReasonCode.ProtocolError.value());
        client.close();
    }

    @Test(groups = "integration")
    public void invalidPacketFormat() {

        String topicFilter = "abc";
        MqttConnectionOptions connOpts = new MqttConnectionOptions();
        connOpts.setCleanStart(true);
        connOpts.setSessionExpiryInterval(0L);
        connOpts.setUserName(tenantId + "/" + userId);
        MqttTestClient client = new MqttTestClient(BROKER_URI);
        client.connect(connOpts);

        MqttMessage message = new MqttMessage();
        message.setQos(0);
        message.setProperties(new MqttProperties() {{
            setPayloadFormat(true);
        }});
        message.setPayload(new byte[8]);
        client.publish(topicFilter, message);
        MqttDisconnectResponse disconnectResponse = client.onDisconnect().blockingGet();
        assertEquals((byte) disconnectResponse.getReturnCode(), MQTT5DisconnectReasonCode.PayloadFormatInvalid.value());
        client.close();
    }
}
