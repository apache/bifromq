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


import static org.apache.bifromq.plugin.eventcollector.EventType.CLIENT_CONNECTED;
import static org.apache.bifromq.plugin.eventcollector.EventType.MQTT_SESSION_START;
import static org.apache.bifromq.plugin.eventcollector.EventType.MSG_RETAINED;
import static org.apache.bifromq.plugin.eventcollector.EventType.MSG_RETAINED_ERROR;
import static org.apache.bifromq.plugin.eventcollector.EventType.PUB_ACKED;
import static org.apache.bifromq.plugin.eventcollector.EventType.PUB_RECED;
import static org.apache.bifromq.plugin.eventcollector.EventType.RETAIN_MSG_CLEARED;
import static org.apache.bifromq.retain.rpc.proto.RetainReply.Result.CLEARED;
import static org.apache.bifromq.retain.rpc.proto.RetainReply.Result.ERROR;
import static org.apache.bifromq.retain.rpc.proto.RetainReply.Result.RETAINED;

import org.apache.bifromq.mqtt.utils.MQTTMessageUtils;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class MQTTRetainPubTest extends BaseMQTTTest {

    @Test
    public void qoS1PubRetain() {
        setupTransientSession();

        mockAuthCheck(true);
        mockDistDist(true);
        mockRetainPipeline(RETAINED);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishRetainQoS1Message("testTopic", 123);
        channel.writeInbound(publishMessage);
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, MSG_RETAINED, PUB_ACKED);
    }

    @Test
    public void qoS1PubRetainClear() {
        setupTransientSession();

        mockAuthCheck(true);
        mockDistDist(true);
        mockRetainPipeline(CLEARED);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishRetainQoS1Message("testTopic", 123);
        channel.writeInbound(publishMessage);
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, RETAIN_MSG_CLEARED, PUB_ACKED);
    }

    @Test
    public void qoS1PubRetainFailed() {
        setupTransientSession();

        mockAuthCheck(true);
        mockDistDist(true);
        mockRetainPipeline(ERROR);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishRetainQoS1Message("testTopic", 123);
        channel.writeInbound(publishMessage);
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, MSG_RETAINED_ERROR, PUB_ACKED);
    }

    @Test
    public void qoS2PubRetainFailed() {
        setupTransientSession();

        mockAuthCheck(true);
        mockDistDist(true);
        mockRetainPipeline(ERROR);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishRetainQoS2Message("testTopic", 123);
        channel.writeInbound(publishMessage);
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, MSG_RETAINED_ERROR, PUB_RECED);
    }
}
