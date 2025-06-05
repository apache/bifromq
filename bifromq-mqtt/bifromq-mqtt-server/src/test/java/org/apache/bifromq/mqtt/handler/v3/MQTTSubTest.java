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
import static org.apache.bifromq.plugin.eventcollector.EventType.INVALID_TOPIC_FILTER;
import static org.apache.bifromq.plugin.eventcollector.EventType.MALFORMED_TOPIC_FILTER;
import static org.apache.bifromq.plugin.eventcollector.EventType.MQTT_SESSION_START;
import static org.apache.bifromq.plugin.eventcollector.EventType.PROTOCOL_VIOLATION;
import static org.apache.bifromq.plugin.eventcollector.EventType.SUB_ACKED;
import static org.apache.bifromq.plugin.eventcollector.EventType.SUB_ACTION_DISALLOW;
import static org.apache.bifromq.plugin.eventcollector.EventType.TOO_LARGE_SUBSCRIPTION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.mqtt.utils.MQTTMessageUtils;
import org.apache.bifromq.type.QoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Slf4j
public class MQTTSubTest extends BaseMQTTTest {

    private boolean shouldCleanSubs = false;

    @AfterMethod
    public void clean() {
        if (shouldCleanSubs) {
            when(distClient.removeRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(),
                anyLong()))
                .thenReturn(CompletableFuture.completedFuture(null));
            channel.close();
            verify(distClient, atLeast(1))
                .removeRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong());
        } else {
            channel.close();
        }
        shouldCleanSubs = false;
    }

    @SneakyThrows
    @Test
    public void transientQoS0Sub() {
        setupTransientSession();

        mockAuthCheck(true);
        mockDistMatch(QoS.AT_MOST_ONCE, true);
        mockRetainMatch();
        int[] qos = {0, 0, 0};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, qos);
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, SUB_ACKED);
        shouldCleanSubs = true;
    }

    @Test
    public void transientQoS1Sub() {
        setupTransientSession();

        mockAuthCheck(true);
        mockDistMatch(QoS.AT_LEAST_ONCE, true);
        mockRetainMatch();
        int[] qos = {1, 1, 1};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, qos);
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, SUB_ACKED);
        shouldCleanSubs = true;
    }

    @Test
    public void transientQoS2Sub() {
        setupTransientSession();

        mockAuthCheck(true);
        mockDistMatch(QoS.EXACTLY_ONCE, true);
        mockRetainMatch();
        int[] qos = {2, 2, 2};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, qos);
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, SUB_ACKED);
        shouldCleanSubs = true;
    }

    @Test
    public void transientMixedSub() {
        setupTransientSession();

        mockAuthCheck(true);
        mockDistMatch(QoS.AT_MOST_ONCE, true);
        mockDistMatch(QoS.AT_LEAST_ONCE, true);
        mockDistMatch(QoS.EXACTLY_ONCE, true);
        mockRetainMatch();
        int[] qos = {0, 1, 2};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, qos);
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, SUB_ACKED);
        shouldCleanSubs = true;
    }

    @Test
    public void transientMixedSubWithDistSubFailed() {
        setupTransientSession();

        mockAuthCheck(true);
        mockDistMatch("testTopic0", true);
        mockDistMatch("testTopic1", true);
        mockDistMatch("testTopic2", false);
        mockRetainMatch();
        int[] qos = {0, 1, 2};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, new int[] {0, 1, 128});
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, SUB_ACKED);
        shouldCleanSubs = true;
    }

    @Test
    public void persistentQoS0Sub() {
        setupPersistentSession();

        mockAuthCheck(true);
        mockInboxSub(QoS.AT_MOST_ONCE, true);
        mockRetainMatch();
        int[] qos = {0, 0, 0};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, qos);
        verifyEvent(CLIENT_CONNECTED, SUB_ACKED);
    }

    @Test
    public void persistentQoS1Sub() {
        setupPersistentSession();

        mockAuthCheck(true);
        mockInboxSub(QoS.AT_LEAST_ONCE, true);
        mockRetainMatch();
        int[] qos = {1, 1, 1};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, qos);
        verifyEvent(CLIENT_CONNECTED, SUB_ACKED);
    }

    @Test
    public void persistentQoS2Sub() {
        setupPersistentSession();

        mockAuthCheck(true);
        mockInboxSub(QoS.EXACTLY_ONCE, true);
        mockRetainMatch();
        int[] qos = {2, 2, 2};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, qos);
        verifyEvent(CLIENT_CONNECTED, SUB_ACKED);
    }

    @Test
    public void persistentMixedSub() {
        setupPersistentSession();

        mockAuthCheck(true);
        mockInboxSub(QoS.AT_MOST_ONCE, true);
        mockInboxSub(QoS.AT_LEAST_ONCE, true);
        mockInboxSub(QoS.EXACTLY_ONCE, true);
        mockRetainMatch();
        int[] qos = {0, 1, 2};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, qos);
        verifyEvent(CLIENT_CONNECTED, SUB_ACKED);
    }

    @Test
    public void subWithEmptyTopicList() {
        setupTransientSession();

        MqttSubscribeMessage subMessage = MQTTMessageUtils.badQoS0MqttSubMessageWithoutTopic();
        channel.writeInbound(subMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.writeInbound();
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, PROTOCOL_VIOLATION);
    }

    @Test
    public void subWithTooLargeTopicList() {
        setupTransientSession();

        int[] qos = new int[100];
        Arrays.fill(qos, 1);
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.writeInbound();
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, TOO_LARGE_SUBSCRIPTION);
    }

    @Test
    public void subWithMalformedTopic() {
        setupTransientSession();

        MqttSubscribeMessage subMessage = MQTTMessageUtils.topicMqttSubMessages("/topic\u0000");
        channel.writeInbound(subMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.writeInbound();
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, MALFORMED_TOPIC_FILTER, SUB_ACKED);
    }

    @Test
    public void subWithInvalidTopic() {
        setupTransientSession();

        MqttSubscribeMessage subMessage = MQTTMessageUtils.badTopicMqttSubMessages();
        channel.writeInbound(subMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.writeInbound();
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, INVALID_TOPIC_FILTER, SUB_ACKED);
    }

    @Test
    public void subWithAuthFailed() {
        setupTransientSession();

        mockAuthCheck(false);
        int[] qos = {1, 1, 1};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, new int[] {128, 128, 128});
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, SUB_ACTION_DISALLOW, SUB_ACTION_DISALLOW, SUB_ACTION_DISALLOW,
            SUB_ACKED);
    }

    private void verifySubAck(MqttSubAckMessage subAckMessage, int[] expectedQos) {
        assertEquals(subAckMessage.payload().grantedQoSLevels().size(), expectedQos.length);
        for (int i = 0; i < expectedQos.length; i++) {
            assertEquals(expectedQos[i], (int) subAckMessage.payload().grantedQoSLevels().get(i));
        }
    }
}
