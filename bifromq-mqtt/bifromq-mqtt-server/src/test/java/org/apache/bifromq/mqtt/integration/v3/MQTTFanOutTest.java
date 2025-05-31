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
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import org.apache.bifromq.mqtt.integration.MQTTTest;
import org.apache.bifromq.mqtt.integration.v3.client.MqttMsg;
import org.apache.bifromq.mqtt.integration.v3.client.MqttTestClient;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Granted;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.apache.bifromq.plugin.authprovider.type.Ok;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.observers.TestObserver;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.testng.annotations.Test;

@Slf4j
public class MQTTFanOutTest extends MQTTTest {
    private final String deviceKey = "testDevice";

    @Override
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
    public void fanout() {
        fanout(0);
        fanout(1);
        fanout(2);
    }

    public void fanout(int pubQoS) {
        String topic = "/a/" + pubQoS;
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(tenantId + "/" + deviceKey);

        MqttTestClient pubClient = new MqttTestClient(BROKER_URI, "pubClient");
        pubClient.connect(connOpts);

        MqttTestClient subClient1 = new MqttTestClient(BROKER_URI, "subClient1");
        subClient1.connect(connOpts);
        MqttTestClient subClient2 = new MqttTestClient(BROKER_URI, "subClient2");
        subClient2.connect(connOpts);
        MqttTestClient subClient3 = new MqttTestClient(BROKER_URI, "subClient3");
        subClient3.connect(connOpts);

        TestObserver<MqttMsg> topicSub1 = subClient1.subscribe("#", 0).test();
        TestObserver<MqttMsg> topicSub2 = subClient2.subscribe(topic, 1).test();
        TestObserver<MqttMsg> topicSub3 = subClient3.subscribe("/a/+", 2).test();

        await().until(() -> {
            pubClient.publish(topic, pubQoS, ByteString.copyFromUtf8("hello"), false);
            return !topicSub1.values().isEmpty() && !topicSub2.values().isEmpty() && !topicSub3.values().isEmpty();
        });

        MqttMsg msg1 = topicSub1.values().get(0);
        assertEquals(msg1.topic, topic);
        assertEquals(msg1.qos, Math.min(0, pubQoS));
        assertFalse(msg1.isDup);
        assertFalse(msg1.isRetain);
        assertEquals(msg1.payload, ByteString.copyFromUtf8("hello"));

        MqttMsg msg2 = topicSub2.values().get(0);
        assertEquals(msg2.topic, topic);
        assertEquals(msg2.qos, Math.min(1, pubQoS));
        assertFalse(msg2.isDup);
        assertFalse(msg2.isRetain);
        assertEquals(msg2.payload, ByteString.copyFromUtf8("hello"));

        MqttMsg msg3 = topicSub3.values().get(0);
        assertEquals(msg3.topic, topic);
        assertEquals(msg3.qos, Math.min(2, pubQoS));
        assertFalse(msg3.isDup);
        assertFalse(msg3.isRetain);
        assertEquals(msg3.payload, ByteString.copyFromUtf8("hello"));

        // TODO: verify event collected

        pubClient.disconnect();
        subClient1.disconnect();
        subClient2.disconnect();
        subClient3.disconnect();

        pubClient.close();
        subClient1.close();
        subClient2.close();
        subClient3.close();
    }
}
