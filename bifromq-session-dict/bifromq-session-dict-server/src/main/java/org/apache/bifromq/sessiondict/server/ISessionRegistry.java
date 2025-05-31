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

package org.apache.bifromq.sessiondict.server;

import java.util.Optional;
import org.apache.bifromq.sessiondict.rpc.proto.ServerRedirection;
import org.apache.bifromq.type.ClientInfo;

/**
 * Registry for MQTT Sessions.
 */
interface ISessionRegistry {
    void add(ClientInfo sessionOwner, ISessionRegister register);

    void remove(ClientInfo sessionOwner, ISessionRegister register);

    Optional<ClientInfo> get(String tenantId, String userId, String mqttClientId);

    Optional<SessionRegistration> findRegistration(String tenantId, String userId, String mqttClientId);

    Iterable<SessionRegistration> findRegistrations(String tenantId, String userId);

    Iterable<SessionRegistration> findRegistrations(String tenantId);

    void close();

    record SessionRegistration(ClientInfo sessionOwner, ISessionRegister register) {
        void stop(ClientInfo kicker, ServerRedirection serverRedirection) {
            register.kick(sessionOwner.getTenantId(), sessionOwner, kicker, serverRedirection);
        }
    }
}
