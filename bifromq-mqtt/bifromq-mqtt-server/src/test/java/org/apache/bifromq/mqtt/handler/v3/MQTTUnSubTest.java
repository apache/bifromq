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


import static org.apache.bifromq.inbox.rpc.proto.UnsubReply.Code.ERROR;
import static org.apache.bifromq.inbox.rpc.proto.UnsubReply.Code.OK;
import static org.apache.bifromq.plugin.eventcollector.EventType.CLIENT_CONNECTED;
import static org.apache.bifromq.plugin.eventcollector.EventType.INVALID_TOPIC_FILTER;
import static org.apache.bifromq.plugin.eventcollector.EventType.MALFORMED_TOPIC_FILTER;
import static org.apache.bifromq.plugin.eventcollector.EventType.MQTT_SESSION_START;
import static org.apache.bifromq.plugin.eventcollector.EventType.PROTOCOL_VIOLATION;
import static org.apache.bifromq.plugin.eventcollector.EventType.TOO_LARGE_UNSUBSCRIPTION;
import static org.apache.bifromq.plugin.eventcollector.EventType.UNSUB_ACKED;
import static org.apache.bifromq.plugin.eventcollector.EventType.UNSUB_ACTION_DISALLOW;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.apache.bifromq.inbox.rpc.proto.UnsubReply;
import org.apache.bifromq.mqtt.utils.MQTTMessageUtils;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class MQTTUnSubTest extends BaseMQTTTest {

    @Test
    public void transientUnSub() {
        setupTransientSession();
        mockAuthCheck(true);
        mockDistUnmatch(true);
        channel.writeInbound(MQTTMessageUtils.qoSMqttUnSubMessages(3));
        MqttUnsubAckMessage unsubAckMessage = channel.readOutbound();
        Assert.assertNotNull(unsubAckMessage);
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, UNSUB_ACKED);
    }

    @Test
    public void transientMixedUnSubWithDistUnSubFailed() {
        setupTransientSession();
        mockAuthCheck(true);
        mockDistUnmatch(true, false, true);
        channel.writeInbound(MQTTMessageUtils.qoSMqttUnSubMessages(3));
        MqttUnsubAckMessage unsubAckMessage = channel.readOutbound();
        Assert.assertNotNull(unsubAckMessage);
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, UNSUB_ACKED);
    }

    @Test
    public void persistentUnSub() {
        setupPersistentSession();
        mockAuthCheck(true);
        when(inboxClient.unsub(any())).thenReturn(
            CompletableFuture.completedFuture(UnsubReply.newBuilder().setCode(OK).build()));
        channel.writeInbound(MQTTMessageUtils.qoSMqttUnSubMessages(3));
        MqttUnsubAckMessage unsubAckMessage = channel.readOutbound();
        Assert.assertNotNull(unsubAckMessage);
        verifyEvent(CLIENT_CONNECTED, UNSUB_ACKED);
    }

    @Test
    public void persistentMixedSubWithDistUnSubFailed() {
        setupPersistentSession();
        mockAuthCheck(true);
        mockDistUnmatch(true, false, true);
        when(inboxClient.unsub(any()))
            .thenReturn(CompletableFuture.completedFuture(UnsubReply.newBuilder().setCode(OK).build()))
            .thenReturn(CompletableFuture.completedFuture(UnsubReply.newBuilder().setCode(ERROR).build()))
            .thenReturn(CompletableFuture.completedFuture(UnsubReply.newBuilder().setCode(OK).build()));
        channel.writeInbound(MQTTMessageUtils.qoSMqttUnSubMessages(3));
        MqttUnsubAckMessage unsubAckMessage = channel.readOutbound();
        Assert.assertNotNull(unsubAckMessage);
        verifyEvent(CLIENT_CONNECTED, UNSUB_ACKED);
    }

    @Test
    public void unSubWithEmptyTopicList() {
        setupTransientSession();
        MqttUnsubscribeMessage unSubMessage = MQTTMessageUtils.badMqttUnSubMessageWithoutTopic();
        channel.writeInbound(unSubMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.writeInbound();
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, PROTOCOL_VIOLATION);
    }

    @Test
    public void unSubWithTooLargeTopicList() {
        setupTransientSession();
        MqttUnsubscribeMessage unSubMessage = MQTTMessageUtils.qoSMqttUnSubMessages(100);
        channel.writeInbound(unSubMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.writeInbound();
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, TOO_LARGE_UNSUBSCRIPTION);
    }

    @Test
    public void unSubWithMalformedTopic() {
        setupTransientSession();
        MqttUnsubscribeMessage unSubMessage = MQTTMessageUtils.topicMqttUnSubMessage("/topic\u0000");
        channel.writeInbound(unSubMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.writeInbound();
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, MALFORMED_TOPIC_FILTER);
    }

    @Test
    public void unSubWithInvalidTopic() {
        setupTransientSession();
        MqttUnsubscribeMessage unSubMessage = MQTTMessageUtils.invalidTopicMqttUnSubMessage();
        channel.writeInbound(unSubMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.writeInbound();
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, INVALID_TOPIC_FILTER, UNSUB_ACKED);
    }

    @Test
    public void unSubWithAuthFailed() {
        setupTransientSession();
        mockAuthCheck(false);
        channel.writeInbound(MQTTMessageUtils.qoSMqttUnSubMessages(3));
        MqttUnsubAckMessage unsubAckMessage = channel.readOutbound();
        Assert.assertNotNull(unsubAckMessage);
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, UNSUB_ACTION_DISALLOW, UNSUB_ACTION_DISALLOW,
                UNSUB_ACTION_DISALLOW,
            UNSUB_ACKED);
    }
}
