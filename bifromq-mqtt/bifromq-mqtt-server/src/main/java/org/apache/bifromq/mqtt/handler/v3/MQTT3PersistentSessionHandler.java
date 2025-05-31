/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.mqtt.handler.IMQTTProtocolHelper;
import org.apache.bifromq.mqtt.handler.MQTTPersistentSessionHandler;
import org.apache.bifromq.mqtt.handler.TenantSettings;
import org.apache.bifromq.mqtt.handler.condition.Condition;
import org.apache.bifromq.type.ClientInfo;
import io.netty.channel.ChannelHandlerContext;
import jakarta.annotation.Nullable;
import lombok.Builder;

public final class MQTT3PersistentSessionHandler extends MQTTPersistentSessionHandler {
    private final IMQTTProtocolHelper helper;

    @Builder
    public MQTT3PersistentSessionHandler(TenantSettings settings,
                                         ITenantMeter tenantMeter,
                                         Condition oomCondition,
                                         String userSessionId,
                                         int keepAliveTimeSeconds,
                                         int sessionExpirySeconds,
                                         ClientInfo clientInfo,
                                         InboxVersion inboxVersion,
                                         @Nullable LWT noDelayLWT,
                                         ChannelHandlerContext ctx) {
        super(settings,
            tenantMeter,
            oomCondition,
            userSessionId,
            keepAliveTimeSeconds,
            sessionExpirySeconds,
            clientInfo,
            inboxVersion,
            noDelayLWT, ctx);
        this.helper = new MQTT3ProtocolHelper(settings, clientInfo);
    }

    @Override
    protected IMQTTProtocolHelper helper() {
        return helper;
    }
}
