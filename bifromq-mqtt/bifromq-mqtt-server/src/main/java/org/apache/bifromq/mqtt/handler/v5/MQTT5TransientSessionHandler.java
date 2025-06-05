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

package org.apache.bifromq.mqtt.handler.v5;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import lombok.Builder;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.mqtt.handler.IMQTTProtocolHelper;
import org.apache.bifromq.mqtt.handler.MQTTTransientSessionHandler;
import org.apache.bifromq.mqtt.handler.TenantSettings;
import org.apache.bifromq.mqtt.handler.condition.Condition;
import org.apache.bifromq.type.ClientInfo;

public final class MQTT5TransientSessionHandler extends MQTTTransientSessionHandler {
    private final IMQTTProtocolHelper helper;
    private final IReAuthenticator reAuthenticator;

    @Builder
    public MQTT5TransientSessionHandler(MqttConnectMessage connMsg,
                                        TenantSettings settings,
                                        ITenantMeter tenantMeter,
                                        Condition oomCondition,
                                        String userSessionId,
                                        int keepAliveTimeSeconds,
                                        ClientInfo clientInfo,
                                        LWT willMessage, // nullable
                                        ChannelHandlerContext ctx) {
        super(settings, tenantMeter, oomCondition, userSessionId, keepAliveTimeSeconds, clientInfo, willMessage, ctx);
        this.helper = new MQTT5ProtocolHelper(connMsg, settings, clientInfo);
        this.reAuthenticator = IReAuthenticator.create(connMsg,
            authProvider,
            clientInfo,
            this::handleProtocolResponse,
            ctx.executor());
    }

    @Override
    protected IMQTTProtocolHelper helper() {
        return helper;
    }

    @Override
    protected void handleOther(MqttMessage message) {
        if (message.fixedHeader().messageType() == MqttMessageType.AUTH) {
            reAuthenticator.onAuth(message);
        }
    }
}
