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

package org.apache.bifromq.mqtt.handler.v5;

import static org.apache.bifromq.mqtt.handler.record.ProtocolResponse.farewell;
import static org.apache.bifromq.mqtt.handler.record.ProtocolResponse.farewellNow;
import static org.apache.bifromq.mqtt.handler.record.ProtocolResponse.response;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.authMethod;
import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;

import org.apache.bifromq.mqtt.handler.record.ProtocolResponse;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5AuthReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5DisconnectReasonCode;
import org.apache.bifromq.mqtt.utils.AuthUtil;
import org.apache.bifromq.plugin.authprovider.IAuthProvider;
import org.apache.bifromq.plugin.authprovider.type.Continue;
import org.apache.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthData;
import org.apache.bifromq.plugin.authprovider.type.Success;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ProtocolViolation;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ReAuthFailed;
import org.apache.bifromq.type.ClientInfo;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReAuthenticator implements IReAuthenticator {
    private final ClientInfo clientInfo;
    private final IAuthProvider authProvider;
    private final String origAuthMethod;
    private final Executor executor;
    private final Consumer<ProtocolResponse> responder;
    private boolean isStarting;
    private boolean isAuthing;

    public ReAuthenticator(ClientInfo clientInfo,
                           IAuthProvider authProvider,
                           String origAuthMethod,
                           Consumer<ProtocolResponse> responder,
                           Executor executor) {
        this.clientInfo = clientInfo;
        this.authProvider = authProvider;
        this.origAuthMethod = origAuthMethod;
        this.executor = executor;
        this.responder = responder;
    }

    public void onAuth(MqttMessage authMessage) {
        MqttReasonCodeAndPropertiesVariableHeader variableHeader =
            ((MqttReasonCodeAndPropertiesVariableHeader) authMessage.variableHeader());
        MQTT5AuthReasonCode reasonCode = MQTT5AuthReasonCode.valueOf(variableHeader.reasonCode());
        switch (reasonCode) {
            case Continue -> {
                if (!isStarting) {
                    responder.accept(farewell(
                        MQTT5MessageBuilders.disconnect()
                            .reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                            .reasonString("Re-auth not started")
                            .build(),
                        getLocal(ProtocolViolation.class)
                            .statement("Re-auth not started")
                            .clientInfo(clientInfo)));
                    return;
                }
                if (isAuthing) {
                    responder.accept(farewell(
                        MQTT5MessageBuilders.disconnect()
                            .reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                            .reasonString("Unexpected auth packet during re-auth")
                            .build(),
                        getLocal(ProtocolViolation.class)
                            .statement("Unexpected auth packet during re-auth")
                            .clientInfo(clientInfo)));
                    return;
                }
                Optional<String> authMethodOpt = authMethod(variableHeader.properties());
                if (authMethodOpt.isEmpty() || !authMethodOpt.get().equals(origAuthMethod)) {
                    responder.accept(farewell(
                        MQTT5MessageBuilders.disconnect()
                            .reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                            .reasonString("Invalid auth method: " + authMethodOpt.orElse(""))
                            .build(),
                        getLocal(ProtocolViolation.class)
                            .statement("Invalid auth method: " + authMethodOpt.orElse(""))
                            .clientInfo(clientInfo)));
                    return;
                }
                authenticate(AuthUtil.buildMQTT5ExtendedAuthData(authMessage, false));
            }
            case ReAuth -> {
                if (isStarting) {
                    responder.accept(farewell(
                        MQTT5MessageBuilders.disconnect()
                            .reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                            .reasonString("Re-auth in-progress")
                            .build(),
                        getLocal(ProtocolViolation.class)
                            .statement("Re-auth in-progress")
                            .clientInfo(clientInfo)));
                    return;
                }
                if (isAuthing) {
                    responder.accept(farewell(
                        MQTT5MessageBuilders.disconnect()
                            .reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                            .reasonString("Re-auth in-progress")
                            .build(),
                        getLocal(ProtocolViolation.class)
                            .statement("Re-auth in-progress")
                            .clientInfo(clientInfo)));
                    return;
                }
                Optional<String> authMethodOpt = authMethod(variableHeader.properties());
                if (authMethodOpt.isEmpty() || !authMethodOpt.get().equals(origAuthMethod)) {
                    responder.accept(farewell(
                        MQTT5MessageBuilders.disconnect()
                            .reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                            .reasonString("Invalid auth method: " + authMethodOpt.orElse(""))
                            .build(),
                        getLocal(ProtocolViolation.class)
                            .statement("Invalid auth method: " + authMethodOpt.orElse(""))
                            .clientInfo(clientInfo)));
                    return;
                }
                isStarting = true;
                authenticate(AuthUtil.buildMQTT5ExtendedAuthData(authMessage, true));
            }
            default -> responder.accept(farewell(
                MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                    .reasonString("Invalid reason code: " + reasonCode.value())
                    .build(),
                getLocal(ProtocolViolation.class)
                    .statement("Invalid reason code: " + reasonCode.value())
                    .clientInfo(clientInfo)));
        }
    }

    private void authenticate(MQTT5ExtendedAuthData authData) {
        isAuthing = true;
        authProvider.extendedAuth(authData)
            .thenAcceptAsync(authResult -> {
                isAuthing = false;
                switch (authResult.getTypeCase()) {
                    case SUCCESS -> {
                        isStarting = false;
                        Success success = authResult.getSuccess();
                        MQTT5MessageBuilders.AuthBuilder authBuilder = MQTT5MessageBuilders.auth(origAuthMethod)
                            .reasonCode(MQTT5AuthReasonCode.Success)
                            .userProperties(success.getUserProps());
                        if (success.hasAuthData()) {
                            authBuilder.authData(success.getAuthData());
                        }
                        responder.accept(response(authBuilder.build()));
                    }
                    case CONTINUE -> {
                        Continue authContinue = authResult.getContinue();
                        MQTT5MessageBuilders.AuthBuilder authBuilder = MQTT5MessageBuilders
                            .auth(origAuthMethod)
                            .reasonCode(MQTT5AuthReasonCode.Continue)
                            .authData(authContinue.getAuthData())
                            .userProperties(authContinue.getUserProps());
                        if (authContinue.hasReason()) {
                            authBuilder.reasonString(authContinue.getReason());
                        }
                        responder.accept(response(authBuilder.build()));
                    }
                    default -> responder.accept(farewellNow(MQTT5MessageBuilders.disconnect()
                            .reasonCode(MQTT5DisconnectReasonCode.NotAuthorized)
                            .build(),
                        getLocal(ReAuthFailed.class)
                            .clientInfo(clientInfo)
                    ));
                }
            }, executor);
    }
}
