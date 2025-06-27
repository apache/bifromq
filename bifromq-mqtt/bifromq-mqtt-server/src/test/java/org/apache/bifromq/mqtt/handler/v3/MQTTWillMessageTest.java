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

package org.apache.bifromq.mqtt.handler.v3;


import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static org.apache.bifromq.plugin.eventcollector.EventType.CLIENT_CONNECTED;
import static org.apache.bifromq.plugin.eventcollector.EventType.IDLE;
import static org.apache.bifromq.plugin.eventcollector.EventType.KICKED;
import static org.apache.bifromq.plugin.eventcollector.EventType.MQTT_SESSION_START;
import static org.apache.bifromq.plugin.eventcollector.EventType.MQTT_SESSION_STOP;
import static org.apache.bifromq.plugin.eventcollector.EventType.MSG_RETAINED;
import static org.apache.bifromq.plugin.eventcollector.EventType.MSG_RETAINED_ERROR;
import static org.apache.bifromq.plugin.eventcollector.EventType.PUB_ACTION_DISALLOW;
import static org.apache.bifromq.plugin.eventcollector.EventType.RETAIN_MSG_CLEARED;
import static org.apache.bifromq.plugin.eventcollector.EventType.WILL_DISTED;
import static org.apache.bifromq.plugin.eventcollector.EventType.WILL_DIST_ERROR;
import static org.apache.bifromq.retain.rpc.proto.RetainReply.Result.CLEARED;
import static org.apache.bifromq.retain.rpc.proto.RetainReply.Result.ERROR;
import static org.apache.bifromq.retain.rpc.proto.RetainReply.Result.RETAINED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.mqtt.utils.MQTTMessageUtils;
import org.apache.bifromq.plugin.eventcollector.EventType;
import org.apache.bifromq.sessiondict.rpc.proto.ServerRedirection;
import org.apache.bifromq.type.ClientInfo;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class MQTTWillMessageTest extends BaseMQTTTest {

    @Test
    public void willWhenIdle() {
        setupTransientSessionWithLWT(false);
        mockAuthCheck(true);
        mockDistDist(true);
        channel.advanceTimeBy(100, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, IDLE, MQTT_SESSION_STOP, WILL_DISTED);
    }

//    @Test
//    public void willWhenSelfKick() {
//        connectAndVerify(true, false, 30, true, false);
//        mockAuthCheck(CheckResult.Type.ALLOW);
//        mockDistDist(true);
//        kickSubject.onNext(
//            Quit.newBuilder()
//                .setKiller(
//                    ClientInfo.newBuilder()
//                        .setTenantId(trafficId)
//                        .setUserId(userId)
//                        .setMqtt3ClientInfo(
//                            MQTT3ClientInfo.newBuilder()
//                                .setClientId(clientId)
//                                .build()
//                        )
//                        .build()
//                )
//                .build()
//        );
//        channel.runPendingTasks();
//        Assert.assertFalse(channel.isActive());
//        verifyEvent(3, ClientConnected, Kicked, WillDisted);
//    }

    @Test
    public void willWhenNotSelfKick() {
        setupTransientSessionWithLWT(false);
        mockAuthCheck(true);
        mockDistDist(true);
        onKill.get().onKill(ClientInfo.newBuilder()
            .setTenantId("sys")
            .putMetadata("agent", "sys")
            .putMetadata("clientId", clientId)
            .build(), ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build());
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, KICKED, MQTT_SESSION_STOP, WILL_DISTED);
    }

    @Test
    public void willAuthCheckFailed() {
        setupTransientSessionWithLWT(false);
        mockAuthCheck(false);
        channel.advanceTimeBy(50, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, IDLE, PUB_ACTION_DISALLOW, MQTT_SESSION_STOP);
        verify(distClient, times(0)).pub(anyLong(), anyString(), any(), any(ClientInfo.class));
    }

    @Test
    public void willDistError() {
        setupTransientSessionWithLWT(false);
        mockAuthCheck(true);
        mockDistDist(false);
        channel.advanceTimeBy(50, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, IDLE, MQTT_SESSION_STOP, WILL_DIST_ERROR);
    }

    @Test
    public void willDistDrop() {
        setupTransientSessionWithLWT(false);
        mockAuthCheck(true);
        mockDistBackPressure();
        channel.advanceTimeBy(50, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, IDLE, MQTT_SESSION_STOP, WILL_DIST_ERROR);
    }


    @Test
    public void willAndRetain() {
        setupTransientSessionWithLWT(true);
        mockAuthCheck(true);
        mockDistDist(true);
        mockRetainPipeline(RETAINED);
        channel.advanceTimeBy(50, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, IDLE, MQTT_SESSION_STOP, MSG_RETAINED, WILL_DISTED);
    }


    @Test
    public void willAndRetainClear() {
        setupTransientSessionWithLWT(true);
        mockAuthCheck(true);
        mockDistDist(true);
        mockRetainPipeline(CLEARED);
        channel.advanceTimeBy(50, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, IDLE, MQTT_SESSION_STOP, RETAIN_MSG_CLEARED, WILL_DISTED);
    }

    @Test
    public void willAndRetainError() {
        setupTransientSessionWithLWT(true);
        mockAuthCheck(true);
        mockDistDist(true);
        mockRetainPipeline(ERROR);
        channel.advanceTimeBy(50, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, IDLE, MQTT_SESSION_STOP, MSG_RETAINED_ERROR, WILL_DISTED);
    }

    protected void setupTransientSessionWithLWT(boolean willRetain) {
        mockAuthPass();
        mockSessionReg();
        mockInboxExist(false);
        MqttConnectMessage connectMessage;
        if (!willRetain) {
            connectMessage = MQTTMessageUtils.qoSWillMqttConnectMessage(1, true);
        } else {
            connectMessage = MQTTMessageUtils.willRetainMqttConnectMessage(1, true);
        }
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);
        verifyEvent(MQTT_SESSION_START, EventType.CLIENT_CONNECTED);
    }
}
