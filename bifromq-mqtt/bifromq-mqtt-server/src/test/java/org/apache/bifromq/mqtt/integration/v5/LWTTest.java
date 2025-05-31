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

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;

import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Granted;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthResult;
import org.apache.bifromq.plugin.authprovider.type.Ok;
import org.apache.bifromq.plugin.authprovider.type.Success;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.observers.TestObserver;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.mqtt.integration.MQTTTest;
import org.apache.bifromq.mqtt.integration.v5.client.MqttMsg;
import org.apache.bifromq.mqtt.integration.v5.client.MqttTestClient;
import org.apache.bifromq.sessiondict.rpc.proto.KillReply;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.testng.annotations.Test;

@Slf4j
public class LWTTest extends MQTTTest {
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
    public void lastWillTest() {
        lastWillTest("wt000", 0, 0, 0);
        lastWillTest("wt001", 0, 0, 1);
        lastWillTest("wt002", 0, 0, 2);
        lastWillTest("wt020", 0, 2, 0);
        lastWillTest("wt021", 0, 2, 1);
        lastWillTest("wt022", 0, 2, 2);
        lastWillTest("wt200", 2, 0, 0);
        lastWillTest("wt201", 2, 0, 1);
        lastWillTest("wt202", 2, 0, 2);
        lastWillTest("wt130", 1, 3, 0);
        lastWillTest("wt131", 1, 3, 1);
        lastWillTest("wt132", 1, 3, 2);
        lastWillTest("wt310", 3, 1, 0);
        lastWillTest("wt311", 3, 1, 1);
        lastWillTest("wt312", 3, 1, 2);
    }

    @Test(groups = "integration")
    public void willRetain() {
        lastWillTest("000", 0, 0, 0, true);
        lastWillTest("001", 0, 0, 1, true);
        lastWillTest("002", 0, 0, 2, true);
        lastWillTest("010", 0, 1, 0, true);
        lastWillTest("011", 0, 1, 1, true);
        lastWillTest("012", 0, 1, 2, true);
        lastWillTest("100", 1, 0, 0, true);
        lastWillTest("101", 1, 0, 1, true);
        lastWillTest("102", 1, 0, 2, true);
        lastWillTest("110", 1, 1, 0, true);
        lastWillTest("111", 1, 1, 1, true);
        lastWillTest("112", 1, 1, 2, true);
    }

    private void lastWillTest(String willTopic,
                              int sessionExpiryInterval,
                              int willDelayInterval,
                              int willQoS) {
        lastWillTest(willTopic, sessionExpiryInterval, willDelayInterval, willQoS, false);
    }

    private void lastWillTest(String willTopic,
                              int sessionExpiryInterval,
                              int willDelayInterval,
                              int willQoS,
                              boolean willRetain) {
        ByteString willPayload = ByteString.copyFromUtf8("bye");
        MqttConnectionOptions lwtPubConnOpts = new MqttConnectionOptions();
        lwtPubConnOpts.setCleanStart(true);
        lwtPubConnOpts.setSessionExpiryInterval((long) sessionExpiryInterval);
        lwtPubConnOpts.setWill(willTopic, new MqttMessage(willPayload.toByteArray()) {
            {
                setQos(willQoS);
                setRetained(willRetain);
                setProperties(new MqttProperties() {
                    {
                        setWillDelayInterval((long) willDelayInterval);
                    }
                });
            }
        });
        lwtPubConnOpts.setUserName(userId);
        MqttTestClient lwtPubClient = new MqttTestClient(BROKER_URI, "lwtPubClient" + willTopic);
        lwtPubClient.connect(lwtPubConnOpts);

        MqttConnectionOptions lwtSubConnOpts = new MqttConnectionOptions();
        lwtSubConnOpts.setCleanStart(true);
        lwtSubConnOpts.setUserName(userId);
        MqttTestClient lwtSubClient = new MqttTestClient(BROKER_URI, "lwtSubClient" + willTopic);
        lwtSubClient.connect(lwtSubConnOpts);
        // Subscribe to the will topic
        TestObserver<MqttMsg> topicSub = lwtSubClient.subscribe(willTopic, willQoS).test();
        // make sure the subscription is active
        await().until(() -> {
            lwtPubClient.publish(willTopic, 1, ByteString.EMPTY, false);
            return !topicSub.values().isEmpty();
        });

        log.info("Kill client");
        assertSame(kill(userId, "lwtPubClient" + willTopic).join(), KillReply.Result.OK);

        await().atMost(Duration.ofSeconds(30)).until(() -> topicSub.values().size() >= 2);
        MqttMsg msg = topicSub.values().get(topicSub.values().size() - 1);
        assertEquals(msg.topic, willTopic);
        assertEquals(msg.qos, willQoS);
        assertEquals(msg.payload, willPayload);
        assertFalse(msg.isRetain);
        lwtSubClient.disconnect();
        lwtSubClient.close();
    }
}
