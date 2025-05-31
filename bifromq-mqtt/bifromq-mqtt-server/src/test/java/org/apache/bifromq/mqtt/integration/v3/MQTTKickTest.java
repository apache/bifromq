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

package org.apache.bifromq.mqtt.integration.v3;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.mqtt.integration.MQTTTest;
import org.apache.bifromq.mqtt.integration.v3.client.MqttTestClient;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Granted;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.apache.bifromq.plugin.authprovider.type.Ok;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.Kicked;
import java.util.concurrent.CompletableFuture;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.testng.annotations.Test;

public class MQTTKickTest extends MQTTTest {

    @Test(groups = "integration")
    public void emptyClientIdNoKick() {
        String deviceKey = "testDevice";
        String clientId = "";

        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId(tenantId)
                    .setUserId(deviceKey)
                    .build())
                .build()));

        when(authProvider.checkPermission(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(
                CheckResult.newBuilder().setGranted(Granted.newBuilder().build()).build()));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setMqttVersion(4);
        connOpts.setCleanSession(true);
        connOpts.setUserName(tenantId + "/" + deviceKey);

        MqttTestClient client1 = new MqttTestClient(BROKER_URI, clientId);
        client1.connect(connOpts);
        assertTrue(client1.isConnected());

        MqttTestClient client2 = new MqttTestClient(BROKER_URI, clientId);
        client2.connect(connOpts);
        assertTrue(client2.isConnected());
        assertTrue(client1.isConnected());

        client1.disconnect();
        client2.disconnect();
        client1.close();
        client2.close();
    }

    @Test(groups = "integration")
    public void testKick() {
        String deviceKey = "testDevice";
        String clientId = "testClient1";

        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId(tenantId)
                    .setUserId(deviceKey)
                    .build())
                .build()));
        when(authProvider.checkPermission(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(CheckResult.newBuilder()
                .setGranted(Granted.getDefaultInstance())
                .build()));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setMqttVersion(4);
        connOpts.setCleanSession(true);
        connOpts.setUserName(tenantId + "/" + deviceKey);

        MqttTestClient client1 = new MqttTestClient(BROKER_URI, clientId);
        client1.connect(connOpts);
        assertTrue(client1.isConnected());

        MqttTestClient client2 = new MqttTestClient(BROKER_URI, clientId);
        client2.connect(connOpts);
        assertTrue(client2.isConnected());
        // waiting client1 to be kicked
        await().until(() -> !client1.isConnected());

        verify(eventCollector).report(argThat(event -> event instanceof Kicked));

        client2.disconnect();
        client1.close();
        client2.close();
    }
}
