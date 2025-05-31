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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.testng.annotations.Test;

@Slf4j
public class MQTTFanInTest extends MQTTTest {
    private final String tenantId = "testFanInTraffic";
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
    public void fanin() {
        fanin(0);
        fanin(1);
        fanin(2);
    }

    public void fanin(int subQoS) {
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(tenantId + "/" + deviceKey);

        MqttTestClient pubClient1 = new MqttTestClient(BROKER_URI, "pubClient1");
        pubClient1.connect(connOpts);

        MqttTestClient pubClient2 = new MqttTestClient(BROKER_URI, "pubClient2");
        pubClient2.connect(connOpts);

        MqttTestClient pubClient3 = new MqttTestClient(BROKER_URI, "pubClient3");
        pubClient3.connect(connOpts);

        MqttTestClient subClient = new MqttTestClient(BROKER_URI, "subClient");
        subClient.connect(connOpts);

        TestObserver<MqttMsg> testObserver = subClient.subscribe("#", subQoS).test();

        await().atMost(Duration.ofSeconds(10)).until(() -> {
            pubClient1.publish("/" + subQoS, 0, ByteString.copyFromUtf8("hello"), false);
            pubClient2.publish("/a/" + subQoS, 1, ByteString.copyFromUtf8("world"), false);
            pubClient3.publish("/a/b" + subQoS, 2, ByteString.copyFromUtf8("greeting"), false);
            return testObserver.values().size() >= 3;
        });
        List<MqttMsg> msgs = new ArrayList<>(testObserver.values());
        for (MqttMsg m : msgs) {
            if (m.topic.equals("/" + subQoS)) {
                assertEquals(m.qos, Math.min(0, subQoS));
                assertFalse(m.isDup);
                assertFalse(m.isRetain);
                assertEquals(m.payload, ByteString.copyFromUtf8("hello"));
            }
            if (m.topic.equals("/a/" + subQoS)) {
                assertEquals(m.qos, Math.min(1, subQoS));
                assertFalse(m.isDup);
                assertFalse(m.isRetain);
                assertEquals(m.payload, ByteString.copyFromUtf8("world"));
            }
            if (m.topic.equals("/a/b" + subQoS)) {
                assertEquals(m.qos, Math.min(2, subQoS));
                assertFalse(m.isDup);
                assertFalse(m.isRetain);
                assertEquals(m.payload, ByteString.copyFromUtf8("greeting"));
            }
        }

        // TODO: verify event collected

        pubClient1.disconnect();
        pubClient2.disconnect();
        pubClient3.disconnect();
        subClient.disconnect();

        pubClient1.close();
        pubClient2.close();
        pubClient3.close();
        subClient.close();
    }
}
