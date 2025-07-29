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
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.mqtt.handler.IMQTTProtocolHelper;
import org.apache.bifromq.mqtt.handler.MQTTPersistentSessionHandler;
import org.apache.bifromq.mqtt.handler.TenantSettings;
import org.apache.bifromq.mqtt.handler.condition.Condition;
import org.apache.bifromq.type.ClientInfo;

public final class MQTT5PersistentSessionHandler extends MQTTPersistentSessionHandler {
    private final IMQTTProtocolHelper helper;
    private final IReAuthenticator reAuthenticator;

    @Builder
    public MQTT5PersistentSessionHandler(MqttConnectMessage connMsg,
                                         TenantSettings settings,
                                         ITenantMeter tenantMeter,
                                         Condition oomCondition,
                                         String userSessionId,
                                         int keepAliveTimeSeconds,
                                         int sessionExpirySeconds,
                                         ClientInfo clientInfo,
                                         InboxVersion inboxVersion,
                                         LWT noDelayLWT, // nullable
                                         ChannelHandlerContext ctx) {
        super(settings,
            tenantMeter,
            oomCondition,
            userSessionId,
            keepAliveTimeSeconds,
            sessionExpirySeconds,
            clientInfo,
            inboxVersion,
            noDelayLWT,
            ctx);
        this.helper = new MQTT5ProtocolHelper(connMsg, settings, clientInfo, sessionCtx.userPropsCustomizer);
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

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        super.handlerAdded(ctx);
        memUsage.addAndGet(estBaseMemSize());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        super.channelInactive(ctx);
        memUsage.addAndGet(-estBaseMemSize());
    }

    private int estBaseMemSize() {
        int s = 408; // base size from JOL
        s += userSessionId.length();
        s += clientInfo.getSerializedSize();
        if (willMessage() != null) {
            s += willMessage().getSerializedSize();
        }
        return s;
    }
}
