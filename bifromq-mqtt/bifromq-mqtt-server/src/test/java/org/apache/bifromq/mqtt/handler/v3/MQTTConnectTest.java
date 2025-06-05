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


import static org.apache.bifromq.plugin.eventcollector.EventType.CLIENT_CONNECTED;
import static org.apache.bifromq.plugin.eventcollector.EventType.INBOX_TRANSIENT_ERROR;
import static org.apache.bifromq.plugin.eventcollector.EventType.MQTT_SESSION_START;
import static org.apache.bifromq.plugin.eventcollector.EventType.PING_REQ;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.inbox.rpc.proto.AttachReply;
import org.apache.bifromq.inbox.rpc.proto.DetachReply;
import org.apache.bifromq.mqtt.utils.MQTTMessageUtils;
import org.apache.bifromq.plugin.authprovider.type.Reject;
import org.apache.bifromq.plugin.eventcollector.Event;
import org.apache.bifromq.plugin.eventcollector.EventType;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientconnected.ClientConnected;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class MQTTConnectTest extends BaseMQTTTest {

    @Test
    public void transientSessionWithoutInbox() {
        mockAuthPass();
        mockSessionReg();
        mockInboxDetach(DetachReply.Code.NO_INBOX);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED);
    }

    @Test
    public void transientSessionWithInbox() {
        mockAuthPass();
        mockSessionReg();
        mockInboxDetach(DetachReply.Code.OK);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED);
    }

    @Test
    public void cleanSessionExpireExistingError() {
        // clear failed
        mockAuthPass();
        mockSessionReg();
        mockInboxDetach(DetachReply.Code.ERROR);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        // verifications
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_REFUSED_SERVER_UNAVAILABLE);
        verifyEvent(INBOX_TRANSIENT_ERROR);
    }

    @Test
    public void attachToExistingSession() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExist(true);
        mockInboxReader();
        mockInboxAttach(1, 1);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(false);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);
        assertTrue(ackMessage.variableHeader().isSessionPresent());
        verifyEvent(CLIENT_CONNECTED);
    }

    @Test
    public void attachToExistingSessionError() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExist(true);
        mockInboxAttach(AttachReply.Code.ERROR);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(false);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        assertNull(channel.readOutbound());
        verifyEvent(INBOX_TRANSIENT_ERROR);
    }

    @Test
    public void createNewPersistentSession() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExist(false);
        mockInboxReader();
        mockInboxAttach(0, 0);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(false);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);
        verifyEvent(CLIENT_CONNECTED);
    }

    @Test
    public void createPersistentSessionError() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExist(false);
        mockInboxReader();
        mockInboxAttach(AttachReply.Code.ERROR);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(false);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        assertNull(channel.readOutbound());
        verifyEvent(INBOX_TRANSIENT_ERROR);
    }

    @Test
    public void authWithCustomAttrs() {
        String attrKey = "attrKey";
        String attrVal = "attrVal";
        mockAuthPass(attrKey, attrVal);
        mockSessionReg();
        mockInboxDetach(DetachReply.Code.OK);

        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        // verifications
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);
        ArgumentCaptor<ClientConnected> eventArgumentCaptor = ArgumentCaptor.forClass(ClientConnected.class);
        verify(eventCollector).report(eventArgumentCaptor.capture());
        ClientConnected clientConnected = eventArgumentCaptor.getValue();
        assertTrue(clientConnected.clientInfo().containsMetadata(attrKey));
        assertEquals(clientConnected.clientInfo().getMetadataMap().get(attrKey), attrVal);
    }

    @Test
    public void reservedMetadataNotOverridable() {
        String attrKey = MQTT_PROTOCOL_VER_KEY;
        String attrVal = "attrVal";
        mockAuthPass(attrKey, attrVal);
        mockSessionReg();
        mockInboxDetach(DetachReply.Code.OK);

        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        // verifications
        Assert.assertEquals(CONNECTION_ACCEPTED, ackMessage.variableHeader().connectReturnCode());
        ArgumentCaptor<ClientConnected> eventArgumentCaptor = ArgumentCaptor.forClass(ClientConnected.class);
        verify(eventCollector).report(eventArgumentCaptor.capture());
        ClientConnected clientConnected = eventArgumentCaptor.getValue();
        assertTrue(clientConnected.clientInfo().containsMetadata(attrKey));
        assertNotEquals(clientConnected.clientInfo().getMetadataMap().get(attrKey), attrVal);
    }

    @Test
    public void authBanned() {
        mockAuthReject(Reject.Code.NotAuthorized, "");
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        // verifications
        Assert.assertEquals(CONNECTION_REFUSED_NOT_AUTHORIZED, ackMessage.variableHeader().connectReturnCode());
        verifyEvent(EventType.NOT_AUTHORIZED_CLIENT);
    }

    @Test
    public void authNotPass() {
        mockAuthReject(Reject.Code.BadPass, "");
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        // verifications
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        Assert.assertEquals(CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD,
            ackMessage.variableHeader().connectReturnCode());
        verifyEvent(EventType.UNAUTHENTICATED_CLIENT);
    }

    @Test
    public void authError() {
        mockAuthReject(Reject.Code.Error, "");
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        // verifications
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_REFUSED_SERVER_UNAVAILABLE);
        ArgumentCaptor<Event> eventArgumentCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventCollector, times(1)).report(eventArgumentCaptor.capture());
        verifyEvent(EventType.AUTH_ERROR);
    }

    @Test
    public void validWillTopic() {
        mockAuthPass();
        mockSessionReg();
        mockInboxDetach(DetachReply.Code.OK);
        MqttConnectMessage connectMessage = MQTTMessageUtils.qoSWillMqttConnectMessage(1, true);
        channel.writeInbound(connectMessage);
        MqttConnAckMessage ackMessage = channel.readOutbound();
        // verifications
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED);
    }

    @Test
    public void pingAndPingResp() {
        mockAuthPass();
        mockSessionReg();
        mockInboxDetach(DetachReply.Code.OK);
        MqttConnectMessage connectMessage = MQTTMessageUtils.qoSWillMqttConnectMessage(1, true);
        channel.writeInbound(connectMessage);
        MqttConnAckMessage ackMessage = channel.readOutbound();
        // verifications
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);

        channel.writeInbound(MqttMessage.PINGREQ);
        MqttMessage pingResp = channel.readOutbound();
        assertEquals(pingResp, MqttMessage.PINGRESP);
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, PING_REQ);
    }
}
