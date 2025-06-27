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
import static org.apache.bifromq.plugin.eventcollector.EventType.KICKED;
import static org.apache.bifromq.plugin.eventcollector.EventType.MQTT_SESSION_START;
import static org.apache.bifromq.plugin.eventcollector.EventType.MQTT_SESSION_STOP;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;

import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import org.apache.bifromq.mqtt.utils.MQTTMessageUtils;
import org.apache.bifromq.sessiondict.rpc.proto.ServerRedirection;
import org.apache.bifromq.type.ClientInfo;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MQTTKickTest extends BaseMQTTTest {

    @Test
    public void testKick() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExist(false);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        MqttConnAckMessage ackMessage = channel.readOutbound();
        Assert.assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);

        // kick

        onKill.get().onKill(ClientInfo.newBuilder().build(),
            ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build());

        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(MQTT_SESSION_START, CLIENT_CONNECTED, KICKED, MQTT_SESSION_STOP);
    }
}
