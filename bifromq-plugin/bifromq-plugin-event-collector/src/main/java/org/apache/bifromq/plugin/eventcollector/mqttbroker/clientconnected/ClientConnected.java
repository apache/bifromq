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

package org.apache.bifromq.plugin.eventcollector.mqttbroker.clientconnected;

import org.apache.bifromq.plugin.eventcollector.ClientEvent;
import org.apache.bifromq.plugin.eventcollector.EventType;
import org.apache.bifromq.type.QoS;
import java.nio.ByteBuffer;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true, chain = true)
@ToString(callSuper = true)
public final class ClientConnected extends ClientEvent<ClientConnected> {
    private String serverId;

    private String userSessionId;

    private int keepAliveTimeSeconds;

    private boolean cleanSession;

    private boolean sessionPresent;

    private WillInfo lastWill;

    @Override
    public EventType type() {
        return EventType.CLIENT_CONNECTED;
    }

    @Override
    public void clone(ClientConnected orig) {
        super.clone(orig);
        this.serverId = orig.serverId;
        this.userSessionId = orig.userSessionId;
        this.keepAliveTimeSeconds = orig.keepAliveTimeSeconds;
        this.cleanSession = orig.cleanSession;
        this.sessionPresent = orig.sessionPresent;
        this.lastWill = orig.lastWill;
    }

    @Getter
    @Setter
    @Accessors(fluent = true, chain = true)
    public static class WillInfo {
        private String topic;

        private QoS qos;

        private boolean isRetain;

        private ByteBuffer payload;
    }
}
