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

package org.apache.bifromq.mqtt.handler.v3;


import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED;
import static org.testng.Assert.assertNull;

import org.apache.bifromq.mqtt.utils.MQTTMessageUtils;
import org.apache.bifromq.plugin.eventcollector.EventType;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class MQTTBadConnectTest extends BaseMQTTTest {

    @Test
    public void unacceptableProtocolVersion() {
        MqttMessage connectMessage = MQTTMessageUtils.connectMessageWithMqttUnacceptableProtocolVersion();
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertNull(ackMessage);
        verifyEvent(EventType.UNACCEPTED_PROTOCOL_VER);
    }

    @Test
    public void mqttIdentifierRejected() {
        MqttMessage connectMessage = MQTTMessageUtils.connectMessageWithMqttIdentifierRejected();
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        Assert.assertEquals(CONNECTION_REFUSED_IDENTIFIER_REJECTED, ackMessage.variableHeader().connectReturnCode());
        verifyEvent(EventType.IDENTIFIER_REJECTED);
    }

    @Test
    public void badMqttPacket() {
        MqttMessage connectMessage = MQTTMessageUtils.failedToDecodeMessage();
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertNull(ackMessage);
        verifyEvent(EventType.PROTOCOL_ERROR);
    }

    @Test
    public void persistentSessionWithoutClientId() {
        MqttMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(false, null, 10);
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        Assert.assertEquals(CONNECTION_REFUSED_IDENTIFIER_REJECTED, ackMessage.variableHeader().connectReturnCode());
        verifyEvent(EventType.IDENTIFIER_REJECTED);
    }

    @Test
    public void clientIdContainsNullCharacter() {
        MqttMessage connectMessage = MQTTMessageUtils.connectMessage("hello\u0000", null, null);
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        verifyEvent(EventType.MALFORMED_CLIENT_IDENTIFIER);
    }

    @Test
    public void userNameContainsNullCharacter() {
        MqttMessage connectMessage = MQTTMessageUtils.connectMessage("hello", null, "user\u0000");
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        verifyEvent(EventType.MALFORMED_USERNAME);
    }

    @Test
    public void willTopicContainsNullCharacter() {
        MqttMessage connectMessage = MQTTMessageUtils.connectMessage("hello", "hello\u0000", null);
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        verifyEvent(EventType.MALFORMED_WILL_TOPIC);
    }


    @Test
    public void firstPacketNotConnect() {
        MqttMessage connectMessage = MQTTMessageUtils.subscribeMessageWithWildCard();
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertNull(ackMessage);
        verifyEvent(EventType.PROTOCOL_ERROR);
    }

    @Test
    public void connectTimeout() {
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(EventType.CONNECT_TIMEOUT);
    }

    @Test
    public void invalidWillTopic() {
        mockAuthPass();
        MqttConnectMessage connectMessage = MQTTMessageUtils.badWillTopicMqttConnectMessage();
        channel.writeInbound(connectMessage);
        // verifications
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertNull(ackMessage);
        verifyEvent(EventType.INVALID_TOPIC);
    }
}
