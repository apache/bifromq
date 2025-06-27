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


import static org.apache.bifromq.plugin.eventcollector.EventType.BAD_PACKET;
import static org.apache.bifromq.plugin.eventcollector.EventType.BY_CLIENT;
import static org.apache.bifromq.plugin.eventcollector.EventType.BY_SERVER;
import static org.apache.bifromq.plugin.eventcollector.EventType.CLIENT_CONNECTED;
import static org.apache.bifromq.plugin.eventcollector.EventType.IDLE;
import static org.apache.bifromq.plugin.eventcollector.EventType.MQTT_SESSION_START;
import static org.apache.bifromq.plugin.eventcollector.EventType.MQTT_SESSION_STOP;
import static org.apache.bifromq.plugin.eventcollector.EventType.PROTOCOL_VIOLATION;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.inbox.rpc.proto.DetachReply;
import org.apache.bifromq.mqtt.utils.MQTTMessageUtils;
import org.apache.bifromq.plugin.settingprovider.Setting;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class MQTTDisconnectTest extends BaseMQTTTest {

    @Test
    public void transientSession() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExist(true);
        mockInboxDetach(DetachReply.Code.OK);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);

        channel.writeInbound(MQTTMessageUtils.disconnectMessage());

        assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, BY_CLIENT, MQTT_SESSION_STOP);
    }

    @Test
    public void persistentSession() {
        mockAuthPass();
        mockSessionReg();
        mockInboxReader();
        mockInboxExist(true);
        mockInboxAttach(0, 0);
        mockDetach(DetachReply.Code.OK);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(false);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);

        channel.writeInbound(MQTTMessageUtils.disconnectMessage());
        assertFalse(channel.isActive());
        verifyEvent(CLIENT_CONNECTED, BY_CLIENT);
    }

    @Test
    public void idle() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExist(false);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true, "testClientId", 60);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();

        channel.advanceTimeBy(100, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(100, TimeUnit.SECONDS);
        channel.runPendingTasks();
        assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, IDLE, MQTT_SESSION_STOP);
    }

    @Test
    public void noKeepAlive() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExist(false);

        // keepalive = 0
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true, "abc", 0);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();

        int keepAlive = settingProvider.provide(Setting.MinKeepAliveSeconds, tenantId);
        channel.advanceTimeBy(keepAlive * 2L, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(keepAlive * 2L, TimeUnit.SECONDS);
        channel.runPendingTasks();
        assertTrue(channel.isActive());
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED);
    }

    @Test
    public void enforceMinKeepAlive() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExist(false);

        // keepalive too short, least is 60s
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true, "abc", 1);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();

        channel.advanceTimeBy(2, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(2, TimeUnit.SECONDS);
        channel.runPendingTasks();
        assertTrue(channel.isActive());
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED);
    }

    @Test
    public void connectTwice() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExist(false);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();

        connectMessage = MQTTMessageUtils.mqttConnectMessage(true, clientId, 0);
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(5, TimeUnit.SECONDS);
        channel.runPendingTasks();
        assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, PROTOCOL_VIOLATION, MQTT_SESSION_STOP);
    }

    @Test
    public void disconnectByServer() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExist(true);
        mockInboxDetach(DetachReply.Code.NO_INBOX);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();

        sessionRegistry.disconnectAll(1000);
        channel.runPendingTasks();
        assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, BY_SERVER, MQTT_SESSION_STOP);
    }

    @Test
    public void badPacketAfterConnected() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExist(false);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();

        MqttMessage mqttMessage = MQTTMessageUtils.failedToDecodeMessage();
        channel.writeInbound(mqttMessage);
        channel.advanceTimeBy(5, TimeUnit.SECONDS);
        channel.runPendingTasks();
        assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, BAD_PACKET, MQTT_SESSION_STOP);
    }
}
