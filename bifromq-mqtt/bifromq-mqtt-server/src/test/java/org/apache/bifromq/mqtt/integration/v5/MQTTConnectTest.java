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

package org.apache.bifromq.mqtt.integration.v5;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.mqtt.integration.MQTTTest;
import org.apache.bifromq.mqtt.integration.v5.client.MqttTestClient;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Granted;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthResult;
import org.apache.bifromq.plugin.authprovider.type.Success;
import org.apache.bifromq.plugin.settingprovider.Setting;
import com.google.common.base.Strings;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.testng.annotations.Test;

@Slf4j
public class MQTTConnectTest extends MQTTTest {

    @Test(groups = "integration")
    public void brokerAssignClientId() {
        when(authProvider.auth(any(MQTT5AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT5AuthResult.newBuilder()
                .setSuccess(Success.newBuilder()
                    .setTenantId("tenant")
                    .setUserId("testUser")
                    .build()).build()));
        when(authProvider.checkPermission(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(
                CheckResult.newBuilder().setGranted(Granted.newBuilder().build()).build()));

        MqttConnectionOptions connOpts = new MqttConnectionOptions();
        connOpts.setCleanStart(true);
        connOpts.setSessionExpiryInterval(0L);
        connOpts.setUserName("tenant/testUser");

        MqttTestClient client = new MqttTestClient(BROKER_URI, "");
        IMqttToken token = client.connect(connOpts);
        assertFalse(Strings.isNullOrEmpty(token.getResponseProperties().getAssignedClientIdentifier()));
    }

    @Test(groups = "integration")
    public void minSessionExpireIntervalEnforced() {
        when(authProvider.auth(any(MQTT5AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT5AuthResult.newBuilder()
                .setSuccess(Success.newBuilder()
                    .setTenantId("tenant")
                    .setUserId("testUser")
                    .build()).build()));
        when(authProvider.checkPermission(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(
                CheckResult.newBuilder().setGranted(Granted.newBuilder().build()).build()));
        when(settingProvider.provide(eq(Setting.ForceTransient), eq("tenant"))).thenReturn(false);

        MqttConnectionOptions connOpts = new MqttConnectionOptions();
        connOpts.setCleanStart(true);
        connOpts.setSessionExpiryInterval(10L);
        connOpts.setUserName("tenant/testUser");

        MqttTestClient client = new MqttTestClient(BROKER_URI, "");
        IMqttToken token = client.connect(connOpts);
        assertEquals(token.getResponseProperties().getSessionExpiryInterval(), 60L);
    }

    @Test(groups = "integration")
    public void maxSessionExpireIntervalEnforced() {
        when(authProvider.auth(any(MQTT5AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT5AuthResult.newBuilder()
                .setSuccess(Success.newBuilder()
                    .setTenantId("tenant")
                    .setUserId("testUser")
                    .build()).build()));
        when(authProvider.checkPermission(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(
                CheckResult.newBuilder().setGranted(Granted.newBuilder().build()).build()));
        when(settingProvider.provide(eq(Setting.ForceTransient), eq("tenant"))).thenReturn(false);
        when(settingProvider.provide(eq(Setting.MaxSessionExpirySeconds), eq("tenant"))).thenReturn(120);

        MqttConnectionOptions connOpts = new MqttConnectionOptions();
        connOpts.setCleanStart(true);
        connOpts.setSessionExpiryInterval(180L);
        connOpts.setUserName("tenant/testUser");

        MqttTestClient client = new MqttTestClient(BROKER_URI, "");
        IMqttToken token = client.connect(connOpts);
        assertEquals(token.getResponseProperties().getSessionExpiryInterval(), 120L);
    }

    @Test(groups = "integration")
    public void forceTransient() {
        when(authProvider.auth(any(MQTT5AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT5AuthResult.newBuilder()
                .setSuccess(Success.newBuilder()
                    .setTenantId("tenant")
                    .setUserId("testUser")
                    .build()).build()));
        when(authProvider.checkPermission(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(
                CheckResult.newBuilder().setGranted(Granted.newBuilder().build()).build()));
        when(settingProvider.provide(eq(Setting.ForceTransient), eq("tenant"))).thenReturn(true);

        MqttConnectionOptions connOpts = new MqttConnectionOptions();
        connOpts.setCleanStart(true);
        connOpts.setSessionExpiryInterval(180L);
        connOpts.setUserName("tenant/testUser");

        MqttTestClient client = new MqttTestClient(BROKER_URI, "");
        IMqttToken token = client.connect(connOpts);
        assertEquals(token.getResponseProperties().getSessionExpiryInterval(), 0L);
    }

    /**
     *  Test whether it can reconnect when cleanStart=true but the session expiration interval is not 0, when reconnecting.
     */
    @Test(groups = "integration")
    public void reconnectTest() {
        when(authProvider.auth(any(MQTT5AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT5AuthResult.newBuilder()
                .setSuccess(Success.newBuilder()
                    .setTenantId("tenant")
                    .setUserId("testUser")
                    .build()).build()));
        when(authProvider.checkPermission(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(
                CheckResult.newBuilder().setGranted(Granted.newBuilder().build()).build()));
        when(settingProvider.provide(eq(Setting.ForceTransient), eq("tenant"))).thenReturn(false);

        MqttConnectionOptions connOpts = new MqttConnectionOptions();
        connOpts.setCleanStart(true);
        connOpts.setSessionExpiryInterval(1800L);
        connOpts.setUserName("tenant/testUser");

        MqttTestClient client1 = new MqttTestClient(BROKER_URI, "client_id");
        IMqttToken token1 = client1.connect(connOpts);
        assertTrue(token1.isComplete());
        assertNull(token1.getException());
        client1.disconnect();

        // reconnect
        MqttTestClient client2 = new MqttTestClient(BROKER_URI, "client_id");
        IMqttToken token2 = client2.connect(connOpts);
        assertTrue(token2.isComplete());
        assertNull(token2.getException());
    }

}
