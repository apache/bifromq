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

package org.apache.bifromq.mqtt.integration.v3;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.observers.TestObserver;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.mqtt.integration.MQTTTest;
import org.apache.bifromq.mqtt.integration.v3.client.MqttMsg;
import org.apache.bifromq.mqtt.integration.v3.client.MqttTestClient;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Granted;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.apache.bifromq.plugin.authprovider.type.Ok;
import org.apache.bifromq.plugin.eventcollector.Event;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.testng.annotations.Test;

@Slf4j
public class MQTTLastWillTest extends MQTTTest {
    @Test(groups = "integration")
    public void lastWillQoS1() {
        String deviceKey = "testDevice";
        String userName = tenantId + "/" + deviceKey;
        String willTopic = "willTopicQoS1";
        ByteString willPayload = ByteString.copyFromUtf8("bye");
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

        MqttConnectOptions lwtPubConnOpts = new MqttConnectOptions();
        lwtPubConnOpts.setCleanSession(true);
        lwtPubConnOpts.setWill(willTopic, willPayload.toByteArray(), 1, false);
        lwtPubConnOpts.setUserName(userName);
        MqttTestClient lwtPubClient = new MqttTestClient(BROKER_URI, "lwtPubclient");
        lwtPubClient.connect(lwtPubConnOpts);

        MqttConnectOptions lwtSubConnOpts = new MqttConnectOptions();
        lwtSubConnOpts.setCleanSession(true);
        lwtSubConnOpts.setUserName(userName);

        MqttTestClient lwtSubClient = new MqttTestClient(BROKER_URI, "lwtSubClient");
        lwtSubClient.connect(lwtSubConnOpts);
        TestObserver<MqttMsg> topicSub = lwtSubClient.subscribe(willTopic, 1).test();
        await().until(() -> {
            // make sure route is ready
            lwtPubClient.publish(willTopic, 1, ByteString.copyFromUtf8("test"), false);
            return !topicSub.values().isEmpty();
        });

        log.info("Kill client");
        kill(deviceKey, "lwtPubclient").join();

        await().until(() -> {
            MqttMsg msg = topicSub.values().get(topicSub.values().size() - 1);
            try {
                assertEquals(msg.topic, willTopic);
                assertEquals(msg.qos, 1);
                assertEquals(msg.payload, willPayload);
                assertFalse(msg.isRetain);
                return true;
            } catch (Throwable e) {
                return false;
            }
        });
    }

    @Test(groups = "integration")
    public void lastWillQoS1Retained() {
        String deviceKey = "testDevice";
        String userName = tenantId + "/" + deviceKey;
        String willTopic = "willTopicQoS1Retained";
        ByteString willPayload = ByteString.copyFromUtf8("bye");
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


        doAnswer(invocationOnMock -> null).when(eventCollector).report(any(Event.class));

        // Publisher with retained will
        MqttConnectOptions lwtPubConnOpts = new MqttConnectOptions();
        lwtPubConnOpts.setCleanSession(true);
        lwtPubConnOpts.setWill(willTopic, willPayload.toByteArray(), 1, true);
        lwtPubConnOpts.setUserName(userName);
        MqttTestClient lwtPubClient = new MqttTestClient(BROKER_URI, "lwtPubclient");
        lwtPubClient.connect(lwtPubConnOpts);

        // Warm route: pre-subscribe and ensure path is active
        MqttConnectOptions warmSubConnOpts = new MqttConnectOptions();
        warmSubConnOpts.setCleanSession(true);
        warmSubConnOpts.setUserName(userName);
        MqttTestClient warmSubClient = new MqttTestClient(BROKER_URI, "lwtWarmSubClient");
        warmSubClient.connect(warmSubConnOpts);
        TestObserver<MqttMsg> warmSub = warmSubClient.subscribe(willTopic, 1).test();
        await().until(() -> {
            lwtPubClient.publish(willTopic, 1, ByteString.copyFromUtf8("test"), false);
            return !warmSub.values().isEmpty();
        });

        // Trigger will publication and retained storage
        kill(deviceKey, "lwtPubclient").join();

        // Fresh subscriber to receive retained snapshot (retain flag true)
        MqttConnectOptions lwtSubConnOpts = new MqttConnectOptions();
        lwtSubConnOpts.setCleanSession(true);
        lwtSubConnOpts.setUserName(userName);
        MqttTestClient lwtSubClient = new MqttTestClient(BROKER_URI, "lwtSubClient");
        lwtSubClient.connect(lwtSubConnOpts);
        Observable<MqttMsg> topicSub = lwtSubClient.subscribe(willTopic, 1);

        MqttMsg msg = topicSub
            .firstElement()
            .timeout(30, java.util.concurrent.TimeUnit.SECONDS)
            .blockingGet();
        assertEquals(msg.topic, willTopic);
        assertEquals(msg.qos, 1);
        assertEquals(msg.payload, willPayload);
        assertTrue(msg.isRetain);

        // clear the retained will message
        lwtSubClient.publish(willTopic, 1, ByteString.EMPTY, true);
        lwtSubClient.disconnect();
        warmSubClient.disconnect();
    }

    @Test(groups = "integration", dependsOnMethods = "lastWillQoS1Retained")
    public void lastWillQoS2() {
        String deviceKey = "testDevice";
        String userName = tenantId + "/" + deviceKey;
        String willTopic = "willTopicQoS2";
        ByteString willPayload = ByteString.copyFromUtf8("bye");
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


        doAnswer(invocationOnMock -> {
            Event event = invocationOnMock.getArgument(0);
            log.info("event: {}", event);
            return null;
        }).when(eventCollector).report(any(Event.class));

        MqttConnectOptions lwtPubConnOpts = new MqttConnectOptions();
        lwtPubConnOpts.setCleanSession(true);
        lwtPubConnOpts.setWill(willTopic, willPayload.toByteArray(), 2, false);
        lwtPubConnOpts.setUserName(userName);
        MqttTestClient lwtPubClient = new MqttTestClient(BROKER_URI, "lwtPubclient");
        lwtPubClient.connect(lwtPubConnOpts);

        MqttConnectOptions lwtSubConnOpts = new MqttConnectOptions();
        lwtSubConnOpts.setCleanSession(true);
        lwtSubConnOpts.setUserName(userName);

        MqttTestClient lwtSubClient = new MqttTestClient(BROKER_URI, "lwtSubClient");
        lwtSubClient.connect(lwtSubConnOpts);
        TestObserver<MqttMsg> topicSub = lwtSubClient.subscribe(willTopic, 2).test();
        await().until(() -> {
            // make sure route is ready
            lwtPubClient.publish(willTopic, 2, ByteString.copyFromUtf8("test"), false);
            return !topicSub.values().isEmpty();
        });

        kill(deviceKey, "lwtPubclient").join();

        topicSub.awaitCount(2);
        MqttMsg msg = topicSub.values().get(topicSub.values().size() - 1);
        assertEquals(msg.topic, willTopic);
        assertEquals(msg.qos, 2);
        assertEquals(msg.payload, willPayload);
        assertFalse(msg.isRetain);
    }

    @Test(groups = "integration")
    public void lastWillQoS2Retained() {
        String deviceKey = "testDevice";
        String userName = tenantId + "/" + deviceKey;
        String willTopic = "willTopicQoS2Retained";
        ByteString willPayload = ByteString.copyFromUtf8("bye");
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

        // Publisher with retained will
        MqttConnectOptions lwtPubConnOpts = new MqttConnectOptions();
        lwtPubConnOpts.setCleanSession(true);
        lwtPubConnOpts.setWill(willTopic, willPayload.toByteArray(), 2, true);
        lwtPubConnOpts.setUserName(userName);
        MqttTestClient lwtPubClient = new MqttTestClient(BROKER_URI, "lwtPubclient");
        lwtPubClient.connect(lwtPubConnOpts);

        // Warm route: pre-subscribe and ensure path is active
        MqttConnectOptions warmSubConnOpts = new MqttConnectOptions();
        warmSubConnOpts.setCleanSession(true);
        warmSubConnOpts.setUserName(userName);
        MqttTestClient warmSubClient = new MqttTestClient(BROKER_URI, "lwtWarmSubClientQoS2");
        warmSubClient.connect(warmSubConnOpts);
        io.reactivex.rxjava3.observers.TestObserver<MqttMsg> warmSub = warmSubClient.subscribe(willTopic, 2).test();
        await().until(() -> {
            lwtPubClient.publish(willTopic, 2, ByteString.copyFromUtf8("test"), false);
            return !warmSub.values().isEmpty();
        });

        // Trigger will publication and retained storage
        kill(deviceKey, "lwtPubclient").join();

        // Fresh subscriber to receive retained snapshot (retain flag true)
        MqttConnectOptions lwtSubConnOpts = new MqttConnectOptions();
        lwtSubConnOpts.setCleanSession(true);
        lwtSubConnOpts.setUserName(userName);
        MqttTestClient lwtSubClient = new MqttTestClient(BROKER_URI, "lwtSubClient");
        lwtSubClient.connect(lwtSubConnOpts);
        Observable<MqttMsg> topicSub = lwtSubClient.subscribe(willTopic, 2);

        MqttMsg msg = topicSub
            .firstElement()
            .timeout(30, java.util.concurrent.TimeUnit.SECONDS)
            .blockingGet();
        assertEquals(msg.topic, willTopic);
        assertEquals(msg.qos, 2);
        assertEquals(msg.payload, willPayload);
        assertTrue(msg.isRetain);

        // clear the retained will message
        lwtSubClient.publish(willTopic, 2, ByteString.EMPTY, true);
        lwtSubClient.disconnect();
        warmSubClient.disconnect();
    }
}
