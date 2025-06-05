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
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import org.apache.bifromq.mqtt.integration.MQTTTest;
import org.apache.bifromq.mqtt.integration.v3.client.MqttMsg;
import org.apache.bifromq.mqtt.integration.v3.client.MqttTestAsyncClient;
import org.apache.bifromq.mqtt.integration.v3.client.MqttTestClient;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Granted;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.apache.bifromq.plugin.authprovider.type.Ok;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.observers.TestObserver;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.testng.annotations.Test;

@Slf4j
public class MQTTPubSubTest extends MQTTTest {
    private final String deviceKey = "testDevice";

    protected void doSetup(Method method) {
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
    }

    @Test(groups = "integration")
    @SneakyThrows
    public void pubQoS0AndDisconnectQuickly() {
        String topic = "greeting";
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(tenantId + "/" + deviceKey);

        MqttTestAsyncClient pubClient = new MqttTestAsyncClient(BROKER_URI, MqttClient.generateClientId());
        pubClient.connect(connOpts);

        MqttTestClient subClient = new MqttTestClient(BROKER_URI, MqttClient.generateClientId());
        subClient.connect(connOpts);
        Observable<MqttMsg> topicSub = subClient.subscribe(topic, 1);

        // publish qos0 and quick disconnect
        CompletableFuture<CheckResult> checkFuture = new CompletableFuture<>();
        when(authProvider.checkPermission(any(), any())).thenReturn(checkFuture);
        pubClient.publish(topic, 0, ByteString.copyFromUtf8("hello"), false);
        pubClient.disconnect().join();
        pubClient.close();
        Thread.sleep(100); // delay a little bit
        checkFuture.complete(CheckResult.newBuilder()
            .setGranted(Granted.getDefaultInstance())
            .build());

        subClient.unsubscribe(topic);
        subClient.disconnect();
        subClient.close();
        TestObserver<MqttMsg> msgObserver = topicSub.test();
        msgObserver.awaitDone(100, TimeUnit.MILLISECONDS);
        msgObserver.assertNoValues();
    }

    @Test(groups = "integration")
    public void pubSubCleanSessionTrue() {
        pubSub("/topic/0/0", 0, "/topic/0/0", 0);
        pubSub("/topic/0/1", 0, "/topic/0/1", 1);
        pubSub("/topic/0/2", 0, "/topic/0/2", 2);
        pubSub("/topic/1/0", 1, "/topic/1/0", 0);
        pubSub("/topic/1/1", 1, "/topic/1/1", 1);
        pubSub("/topic/1/2", 1, "/topic/1/2", 2);
        pubSub("/topic/2/0", 2, "/topic/2/0", 0);
        pubSub("/topic/2/1", 2, "/topic/2/1", 1);
        pubSub("/topic/2/2", 2, "/topic/2/2", 2);
        pubSub("/topic1/0/0", 0, "#", 0);
        pubSub("/topic1/0/1", 0, "#", 1);
        pubSub("/topic1/0/2", 0, "#", 2);
        pubSub("/topic1/1/0", 1, "#", 0);
        pubSub("/topic1/1/1", 1, "#", 1);
        pubSub("/topic1/1/2", 1, "#", 2);
        pubSub("/topic1/2/0", 2, "#", 0);
        pubSub("/topic1/2/1", 2, "#", 1);
        pubSub("/topic1/2/2", 2, "#", 2);
    }

    @Test(groups = "integration")
    public void pubSubCleanSessionFalse() {
        pubSub("/topic/0/0", 0, "/topic/0/0", 0, false);
        pubSub("/topic/0/1", 0, "/topic/0/1", 1, false);
        pubSub("/topic/0/2", 0, "/topic/0/2", 2, false);
        pubSub("/topic/1/0", 1, "/topic/1/0", 0, false);
        pubSub("/topic/1/1", 1, "/topic/1/1", 1, false);
        pubSub("/topic/2/0", 2, "/topic/2/0", 0, false);
        pubSub("/topic/2/1", 2, "/topic/2/1", 1, false);
        pubSub("/topic/2/2", 2, "/topic/2/2", 2, false);
        pubSub("/topic1/0/0", 0, "#", 0, false);
        pubSub("/topic1/0/1", 0, "#", 1, false);
        pubSub("/topic1/0/2", 0, "#", 2, false);
        pubSub("/topic1/1/0", 1, "#", 0, false);
        pubSub("/topic1/1/1", 1, "#", 1, false);
        pubSub("/topic1/1/2", 1, "#", 2, false);
        pubSub("/topic1/2/0", 2, "#", 0, false);
        pubSub("/topic1/2/1", 2, "#", 1, false);
        pubSub("/topic1/2/2", 2, "#", 2, false);
    }

    @Test(groups = "integration")
    public void pubSubCleanSessionFalseQoS2() {
        pubSub("/topic/1/2", 1, "/topic/1/2", 2, false);
    }

    @Test(groups = "integration")
    public void receiveOfflineMessage11() {
        receiveOfflineMessage(1, 1);
    }

    @Test(groups = "integration")
    public void receiveOfflineMessageQoS21() {
        receiveOfflineMessage(2, 1);
    }

    @Test(groups = "integration")
    public void receiveOfflineMessageQoS12() {
        receiveOfflineMessage(1, 2);
    }

//    @Test(groups = "integration")
//    public void receiveOfflineMessageQoS22() {
//     paho client having some issue to run this case
//        receiveOfflineMessage(2, 2);
//    }

    @Test(groups = "integration")
    public void pubSubInOrder_0_0_true() {
        pubSubInOrder(0, 0, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_0_1_true() {
        pubSubInOrder(0, 1, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_0_2_true() {
        pubSubInOrder(0, 2, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_1_0_true() {
        pubSubInOrder(1, 0, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_1_1_true() {
        pubSubInOrder(1, 1, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_1_2_true() {
        pubSubInOrder(1, 2, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_2_0_true() {
        pubSubInOrder(2, 0, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_2_1_true() {
        pubSubInOrder(2, 1, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_2_2_true() {
        pubSubInOrder(2, 2, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_0_0_false() {
        pubSubInOrder(0, 0, false);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_0_1_false() {
        pubSubInOrder(0, 1, false);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_0_2_false() {
        pubSubInOrder(0, 2, false);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_1_0_false() {
        pubSubInOrder(1, 0, false);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_1_1_false() {
        pubSubInOrder(1, 1, false);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_1_2_false() {
        pubSubInOrder(1, 2, false);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_2_0_false() {
        pubSubInOrder(2, 0, false);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_2_1_false() {
        pubSubInOrder(2, 1, false);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_2_2_false() {
        pubSubInOrder(2, 2, false);
    }

    @SneakyThrows
    private void pubSubInOrder(int pubQoS, int subQoS, boolean cleanSession) {
        String topic = "a/b";
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(cleanSession);
        connOpts.setUserName(tenantId + "/" + deviceKey);

        MqttTestClient pubClient = new MqttTestClient(BROKER_URI, MqttClient.generateClientId());
        pubClient.connect(connOpts);

        MqttTestClient subClient = new MqttTestClient(BROKER_URI, MqttClient.generateClientId());
        subClient.connect(connOpts);
        TestObserver<MqttMsg> topicSub = subClient.subscribe(topic, subQoS).test();
        await().until(() -> {
            pubClient.publish(topic, pubQoS, ByteString.EMPTY, false);
            return !topicSub.values().isEmpty();
        });

        List<ByteString> pubMsgList = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            ByteString msg = ByteString.copyFromUtf8(Integer.toUnsignedString(i));
            pubMsgList.add(msg);
            pubClient.publish(topic, pubQoS, msg, false);
        }
        await().until(() -> {
            List<ByteString> mqttMsgs = topicSub.values().stream().map(msg -> msg.payload).toList();
            return mqttMsgs.size() >= 100
                && mqttMsgs.subList(mqttMsgs.size() - 100, mqttMsgs.size()).equals(pubMsgList);
        });
        pubClient.disconnect();
        pubClient.close();
        subClient.unsubscribe(topic);
        subClient.disconnect();
        subClient.close();
    }

    private void receiveOfflineMessage(int pubQoS, int subQoS) {
        String topic = "topic/" + pubQoS + "/" + subQoS;
        MqttConnectOptions subClientOpts = new MqttConnectOptions();
        subClientOpts.setCleanSession(false);
        subClientOpts.setUserName(tenantId + "/subClient");

        log.info("Connect sub client");
        // make a offline subscription
        String clientId = MqttClient.generateClientId();
        MqttTestClient subClient = new MqttTestClient(BROKER_URI, clientId);
        subClient.connect(subClientOpts);
        subClient.subscribe(topic, subQoS);
        log.info("Disconnect sub client");
        subClient.disconnect();
        subClient.close();

        log.info("Connect pub client and pub some message");
        MqttConnectOptions pubClientOpts = new MqttConnectOptions();
        pubClientOpts.setCleanSession(true);
        pubClientOpts.setUserName(tenantId + "/pubClient");
        MqttTestClient pubClient = new MqttTestClient(BROKER_URI, MqttClient.generateClientId());
        pubClient.connect(pubClientOpts);
        pubClient.publish(topic, pubQoS, ByteString.copyFromUtf8("hello"), false);

        log.info("Reconnect sub client");
        subClient = new MqttTestClient(BROKER_URI, clientId);
        TestObserver<MqttMsg> msgObserver = subClient.messageArrived().test();
        subClient.connect(subClientOpts);

        msgObserver.awaitCount(1);
        MqttMsg msg = msgObserver.values().get(0);
        assertEquals(msg.topic, topic);
        assertEquals(msg.qos, Math.min(pubQoS, subQoS));
        assertFalse(msg.isDup);
        assertFalse(msg.isRetain);
        assertEquals(msg.payload, ByteString.copyFromUtf8("hello"));

        pubClient.disconnect();
        pubClient.close();
//        subClient.unsubscribe(topic);
        subClient.disconnect();
        subClient.close();
    }

    private void pubSub(String topic, int pubQoS, String topicFilter, int subQoS) {
        pubSub(topic, pubQoS, topicFilter, subQoS, true);
    }

    private void pubSub(String topic, int pubQoS, String topicFilter, int subQoS, boolean cleanSession) {

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(cleanSession);
        connOpts.setUserName(tenantId + "/" + deviceKey);

        MqttTestClient client = new MqttTestClient(BROKER_URI, MqttClient.generateClientId());
        client.connect(connOpts);
        TestObserver<MqttMsg> topicSub = client.subscribe(topicFilter, subQoS).test();
        await().until(() -> {
            client.publish(topic, pubQoS, ByteString.copyFromUtf8("hello"), false);
            return !topicSub.values().isEmpty();
        });
        MqttMsg msg = topicSub.values().get(0);
        assertEquals(msg.topic, topic);
        assertEquals(msg.qos, Math.min(pubQoS, subQoS));
        assertFalse(msg.isDup);
        assertFalse(msg.isRetain);
        assertEquals(msg.payload, ByteString.copyFromUtf8("hello"));
        client.unsubscribe(topicFilter);
        client.disconnect();
        client.close();
    }
}
