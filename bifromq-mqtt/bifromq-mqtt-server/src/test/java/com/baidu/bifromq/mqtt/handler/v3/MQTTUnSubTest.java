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

package com.baidu.bifromq.mqtt.handler.v3;


import static com.baidu.bifromq.plugin.eventcollector.EventType.CLIENT_CONNECTED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.INVALID_TOPIC_FILTER;
import static com.baidu.bifromq.plugin.eventcollector.EventType.MALFORMED_TOPIC_FILTER;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PROTOCOL_VIOLATION;
import static com.baidu.bifromq.plugin.eventcollector.EventType.TOO_LARGE_UNSUBSCRIPTION;
import static com.baidu.bifromq.plugin.eventcollector.EventType.UNSUB_ACKED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.UNSUB_ACTION_DISALLOW;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.inbox.client.InboxUnsubResult;
import com.baidu.bifromq.mqtt.handler.BaseMQTTTest;
import com.baidu.bifromq.mqtt.utils.MQTTMessageUtils;
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
        connectAndVerify(true);
        mockAuthCheck(true);
        mockDistUnmatch(true);
        channel.writeInbound(MQTTMessageUtils.qoSMqttUnSubMessages(3));
        MqttUnsubAckMessage unsubAckMessage = channel.readOutbound();
        Assert.assertNotNull(unsubAckMessage);
        verifyEvent(2, CLIENT_CONNECTED, UNSUB_ACKED);
    }

    @Test
    public void transientMixedUnSubWithDistUnSubFailed() {
        connectAndVerify(true);
        mockAuthCheck(true);
        mockDistUnmatch(true, false, true);
        channel.writeInbound(MQTTMessageUtils.qoSMqttUnSubMessages(3));
        MqttUnsubAckMessage unsubAckMessage = channel.readOutbound();
        Assert.assertNotNull(unsubAckMessage);
        verifyEvent(2, CLIENT_CONNECTED, UNSUB_ACKED);
    }

    @Test
    public void persistentUnSub() {
        connectAndVerify(false);
        mockAuthCheck(true);
        when(inboxClient.unsub(anyLong(), anyString(), anyString(), any())).thenReturn(
            CompletableFuture.completedFuture(InboxUnsubResult.OK));
        channel.writeInbound(MQTTMessageUtils.qoSMqttUnSubMessages(3));
        MqttUnsubAckMessage unsubAckMessage = channel.readOutbound();
        Assert.assertNotNull(unsubAckMessage);
        verifyEvent(2, CLIENT_CONNECTED, UNSUB_ACKED);
    }

    @Test
    public void persistentMixedSubWithDistUnSubFailed() {
        connectAndVerify(false);
        mockAuthCheck(true);
        mockDistUnmatch(true, false, true);
        when(inboxClient.unsub(anyLong(), anyString(), anyString(), any()))
            .thenReturn(CompletableFuture.completedFuture(InboxUnsubResult.OK))
            .thenReturn(CompletableFuture.completedFuture(InboxUnsubResult.ERROR))
            .thenReturn(CompletableFuture.completedFuture(InboxUnsubResult.OK));
        channel.writeInbound(MQTTMessageUtils.qoSMqttUnSubMessages(3));
        MqttUnsubAckMessage unsubAckMessage = channel.readOutbound();
        Assert.assertNotNull(unsubAckMessage);
        verifyEvent(2, CLIENT_CONNECTED, UNSUB_ACKED);
    }

    @Test
    public void unSubWithEmptyTopicList() {
        connectAndVerify(true);
        MqttUnsubscribeMessage unSubMessage = MQTTMessageUtils.badMqttUnSubMessageWithoutTopic();
        channel.writeInbound(unSubMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.writeInbound();
        verifyEvent(2, CLIENT_CONNECTED, PROTOCOL_VIOLATION);
    }

    @Test
    public void unSubWithTooLargeTopicList() {
        connectAndVerify(true);
        MqttUnsubscribeMessage unSubMessage = MQTTMessageUtils.qoSMqttUnSubMessages(100);
        channel.writeInbound(unSubMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.writeInbound();
        verifyEvent(2, CLIENT_CONNECTED, TOO_LARGE_UNSUBSCRIPTION);
    }

    @Test
    public void unSubWithMalformedTopic() {
        connectAndVerify(true);
        MqttUnsubscribeMessage unSubMessage = MQTTMessageUtils.topicMqttUnSubMessage("/topic\u0000");
        channel.writeInbound(unSubMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.writeInbound();
        verifyEvent(2, CLIENT_CONNECTED, MALFORMED_TOPIC_FILTER);
    }

    @Test
    public void unSubWithInvalidTopic() {
        connectAndVerify(true);
        MqttUnsubscribeMessage unSubMessage = MQTTMessageUtils.invalidTopicMqttUnSubMessage();
        channel.writeInbound(unSubMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.writeInbound();
        verifyEvent(2, CLIENT_CONNECTED, INVALID_TOPIC_FILTER);
    }

    @Test
    public void unSubWithAuthFailed() {
        connectAndVerify(true);
        mockAuthCheck(false);
        channel.writeInbound(MQTTMessageUtils.qoSMqttUnSubMessages(3));
        MqttUnsubAckMessage unsubAckMessage = channel.readOutbound();
        Assert.assertNotNull(unsubAckMessage);
        verifyEvent(5, CLIENT_CONNECTED, UNSUB_ACTION_DISALLOW, UNSUB_ACTION_DISALLOW, UNSUB_ACTION_DISALLOW,
            UNSUB_ACKED);
    }
}
