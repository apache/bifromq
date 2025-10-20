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

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_BAD_AUTHENTICATION_METHOD;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_BANNED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_IMPLEMENTATION_SPECIFIC;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_MALFORMED_PACKET;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED_5;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_PACKET_TOO_LARGE;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_PAYLOAD_FORMAT_INVALID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_PROTOCOL_ERROR;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_QOS_NOT_SUPPORTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_QUOTA_EXCEEDED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_RETAIN_NOT_SUPPORTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_BUSY;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_MOVED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_TOPIC_NAME_INVALID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_UNSPECIFIED_ERROR;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_UNSUPPORTED_PROTOCOL_VERSION;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_USE_ANOTHER_SERVER;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL;
import static org.apache.bifromq.metrics.TenantMetric.MqttAuthFailureCount;
import static org.apache.bifromq.mqtt.handler.MQTTConnectHandler.AuthResult.goAway;
import static org.apache.bifromq.mqtt.handler.MQTTConnectHandler.AuthResult.ok;
import static org.apache.bifromq.mqtt.handler.condition.ORCondition.or;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.authData;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.authMethod;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.isUTF8Payload;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.maximumPacketSize;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.mqttProps;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.receiveMaximum;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.requestProblemInformation;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.requestResponseInformation;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.toUserProperties;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.toWillMessage;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.topicAliasMaximum;
import static org.apache.bifromq.mqtt.utils.AuthUtil.buildConnAction;
import static org.apache.bifromq.mqtt.utils.MQTT5MessageSizer.MIN_CONTROL_PACKET_SIZE;
import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ADDRESS_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_BROKER_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_5_VALUE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;

import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.mqtt.handler.ChannelAttrs;
import org.apache.bifromq.mqtt.handler.MQTTConnectHandler;
import org.apache.bifromq.mqtt.handler.MQTTSessionHandler;
import org.apache.bifromq.mqtt.handler.TenantSettings;
import org.apache.bifromq.mqtt.handler.condition.DirectMemPressureCondition;
import org.apache.bifromq.mqtt.handler.condition.HeapMemPressureCondition;
import org.apache.bifromq.mqtt.handler.record.GoAway;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5AuthReasonCode;
import org.apache.bifromq.mqtt.utils.AuthUtil;
import org.apache.bifromq.mqtt.utils.IMQTTMessageSizer;
import org.apache.bifromq.plugin.authprovider.IAuthProvider;
import org.apache.bifromq.plugin.authprovider.type.Continue;
import org.apache.bifromq.plugin.authprovider.type.Failed;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTTAction;
import org.apache.bifromq.plugin.authprovider.type.Success;
import org.apache.bifromq.plugin.clientbalancer.IClientBalancer;
import org.apache.bifromq.plugin.clientbalancer.Redirection;
import org.apache.bifromq.plugin.eventcollector.OutOfTenantResource;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed.AuthError;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed.EnhancedAuthAbortByClient;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed.MalformedClientIdentifier;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed.MalformedUserName;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed.MalformedWillTopic;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed.NotAuthorizedClient;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed.ProtocolError;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed.UnauthenticatedClient;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InboxTransientError;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InvalidTopic;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ProtocolViolation;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.Redirect;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ResourceThrottled;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ServerBusy;
import org.apache.bifromq.plugin.resourcethrottler.TenantResourceType;
import org.apache.bifromq.sysprops.props.MaxMqtt5ClientIdLength;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.StringPair;
import org.apache.bifromq.type.UserProperties;
import org.apache.bifromq.util.TopicUtil;
import org.apache.bifromq.util.UTF8Util;

@Slf4j
public class MQTT5ConnectHandler extends MQTTConnectHandler {
    public static final String NAME = "MQTT5ConnectHandler";
    private static final int MAX_CLIENT_ID_LEN = MaxMqtt5ClientIdLength.INSTANCE.get();
    private ChannelHandlerContext ctx;
    private IClientBalancer clientBalancer;
    private IAuthProvider authProvider;
    private boolean isAuthing;
    private MqttConnectMessage connMsg = null;
    private boolean requestProblemInfo;
    private String authMethod = null;
    private CompletableFuture<AuthResult> extendedAuthFuture;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        super.handlerAdded(ctx);
        this.ctx = ctx;
        authProvider = ChannelAttrs.mqttSessionContext(ctx).authProvider(ctx);
        clientBalancer = ChannelAttrs.mqttSessionContext(ctx).clientBalancer;
    }

    @Override
    protected GoAway sanityCheck(MqttConnectMessage connMsg) {
        final InetSocketAddress clientAddress = ChannelAttrs.socketAddress(ctx.channel());
        // sanity check
        final String requestClientId = connMsg.payload().clientIdentifier();
        if (!UTF8Util.isWellFormed(requestClientId, SANITY_CHECK)) {
            return new GoAway(MqttMessageBuilders
                .connAck()
                .properties(MQTT5MessageBuilders.connAckProperties()
                    .reasonString("Malformed clientId")
                    .build())
                .returnCode(CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID) // [MQTT-3.1.3-8]
                .build(),
                getLocal(MalformedClientIdentifier.class).peerAddress(clientAddress));
        }
        if (requestClientId.length() > MAX_CLIENT_ID_LEN) {
            return new GoAway(MqttMessageBuilders
                .connAck()
                .properties(MQTT5MessageBuilders.connAckProperties()
                    .reasonString("Max " + MAX_CLIENT_ID_LEN + "chars allowed")
                    .build())
                .returnCode(CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID) // [MQTT-3.1.3-8]
                .build(),
                getLocal(MalformedClientIdentifier.class).peerAddress(clientAddress));
        }
        if (!connMsg.variableHeader().isCleanSession() && requestClientId.isEmpty()) {
            return new GoAway(MqttMessageBuilders
                .connAck()
                .properties(MQTT5MessageBuilders.connAckProperties()
                    .reasonString("ClientId missing when CleanStart is set to true")
                    .build())
                .returnCode(CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID) // [MQTT-3.1.3-8]
                .build(),
                getLocal(MalformedClientIdentifier.class).peerAddress(clientAddress));
        }
        if (connMsg.variableHeader().hasUserName()
            && !UTF8Util.isWellFormed(connMsg.payload().userName(), SANITY_CHECK)) {
            return new GoAway(MqttMessageBuilders
                .connAck()
                .properties(MQTT5MessageBuilders.connAckProperties()
                    .reasonString("Malformed username")
                    .build())
                .returnCode(CONNECTION_REFUSED_MALFORMED_PACKET) // [MQTT-4.13.1-1]
                .build(),
                getLocal(MalformedUserName.class).peerAddress(clientAddress));
        }
        if (authMethod(connMsg.variableHeader().properties()).isEmpty()
            && authData(connMsg.variableHeader().properties()).isPresent()) {
            return new GoAway(MqttMessageBuilders
                .connAck()
                .properties(MQTT5MessageBuilders.connAckProperties()
                    .reasonString("Missing auth method for authData")
                    .build())
                .returnCode(CONNECTION_REFUSED_PROTOCOL_ERROR) // [MQTT-4.13.1-1]
                .build(),
                getLocal(ProtocolError.class)
                    .statement("Missing auth method for authData")
                    .peerAddress(clientAddress));
        }
        if (connMsg.variableHeader().isWillFlag()) {
            if (!UTF8Util.isWellFormed(connMsg.payload().willTopic(), SANITY_CHECK)) {
                return new GoAway(MqttMessageBuilders
                    .connAck()
                    .properties(MQTT5MessageBuilders.connAckProperties()
                        .reasonString("Malformed will topic")
                        .build())
                    .returnCode(CONNECTION_REFUSED_MALFORMED_PACKET) // [MQTT-4.13.1-1]
                    .build(),
                    getLocal(MalformedWillTopic.class).peerAddress(clientAddress));
            }
        }
        // Validate Receive Maximum property per MQTT v5.0 [3.1.2.11.3]
        Optional<Integer> receiveMaxProps = receiveMaximum(connMsg.variableHeader().properties());
        if (receiveMaxProps.isPresent() && receiveMaxProps.get() == 0) {
            return new GoAway(MqttMessageBuilders
                .connAck()
                .returnCode(CONNECTION_REFUSED_PROTOCOL_ERROR)
                .properties(MQTT5MessageBuilders.connAckProperties()
                    .reasonString("MQTT5-3.1.2.11.3")
                    .build())
                .build(),
                getLocal(ProtocolError.class).statement("MQTT5-3.1.2.11.3"));
        }
        return null;
    }

    @Override
    protected CompletableFuture<AuthResult> authenticate(MqttConnectMessage message) {
        this.connMsg = message;
        this.requestProblemInfo = requestProblemInformation(message.variableHeader().properties());
        Optional<String> authMethodOpt = authMethod(message.variableHeader().properties());
        if (authMethodOpt.isEmpty()) {
            MQTT5AuthData mqtt5AuthData = AuthUtil.buildMQTT5AuthData(ctx.channel(), message);
            return authProvider.auth(mqtt5AuthData)
                .thenApplyAsync(authResult -> {
                    final InetSocketAddress clientAddress = ChannelAttrs.socketAddress(ctx.channel());
                    switch (authResult.getTypeCase()) {
                        case SUCCESS -> {
                            ClientInfo clientInfo = buildClientInfo(clientAddress, authResult.getSuccess());
                            Success success = authResult.getSuccess();
                            Optional<String> respInfo = Optional.ofNullable(success.hasResponseInfo()
                                ? success.getResponseInfo() : null);
                            Optional<ByteString> authData =
                                Optional.ofNullable(success.hasAuthData() ? success.getAuthData() : null);
                            SuccessInfo successInfo =
                                new SuccessInfo(clientInfo, respInfo, authData, success.getUserProps());
                            return ok(successInfo);
                        }
                        case FAILED -> {
                            Failed failed = authResult.getFailed();
                            if (failed.hasTenantId()) {
                                ITenantMeter.get(failed.getTenantId()).recordCount(MqttAuthFailureCount);
                            }

                            switch (failed.getCode()) {
                                case NotAuthorized -> {
                                    return goAway(MqttMessageBuilders
                                            .connAck()
                                            .returnCode(CONNECTION_REFUSED_NOT_AUTHORIZED_5)
                                            .properties(mqttProps().addUserProperties(failed.getUserProps()).build())
                                            .build(),
                                        getLocal(NotAuthorizedClient.class)
                                            .tenantId(failed.getTenantId())
                                            .userId(failed.getUserId())
                                            .clientId(mqtt5AuthData.getClientId())
                                            .peerAddress(clientAddress));
                                }
                                case BadPass -> {
                                    return goAway(MqttMessageBuilders
                                            .connAck()
                                            .returnCode(CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD)
                                            .properties(mqttProps().addUserProperties(failed.getUserProps()).build())
                                            .build(),
                                        getLocal(UnauthenticatedClient.class)
                                            .tenantId(failed.getTenantId())
                                            .userId(failed.getUserId())
                                            .clientId(mqtt5AuthData.getClientId())
                                            .peerAddress(clientAddress));
                                }
                                case Banned -> {
                                    return goAway(MqttMessageBuilders
                                            .connAck()
                                            .returnCode(CONNECTION_REFUSED_BANNED)
                                            .properties(mqttProps().addUserProperties(failed.getUserProps()).build())
                                            .build(),
                                        getLocal(UnauthenticatedClient.class).peerAddress(clientAddress));
                                }
                                case BadAuthMethod -> {
                                    return goAway(MqttMessageBuilders
                                            .connAck()
                                            .returnCode(CONNECTION_REFUSED_BAD_AUTHENTICATION_METHOD)
                                            .properties(mqttProps().addUserProperties(failed.getUserProps()).build())
                                            .build(),
                                        getLocal(AuthError.class)
                                            .cause(failed.getReason())
                                            .peerAddress(clientAddress));
                                }
                                // fallthrough
                                default -> {
                                    log.error("Unexpected auth error:{}", failed.getReason());
                                    return goAway(MqttMessageBuilders
                                            .connAck()
                                            .returnCode(CONNECTION_REFUSED_UNSPECIFIED_ERROR)
                                            .build(),
                                        getLocal(AuthError.class).cause(failed.getReason())
                                            .peerAddress(clientAddress));
                                }
                            }
                        }
                        default -> {
                            log.error("Unexpected auth result: {}", authResult.getTypeCase());
                            return goAway(MqttMessageBuilders
                                    .connAck()
                                    .returnCode(CONNECTION_REFUSED_UNSPECIFIED_ERROR)
                                    .build(),
                                getLocal(AuthError.class).peerAddress(clientAddress)
                                    .cause("Unknown auth result"));
                        }
                    }
                }, ctx.executor());
        } else {
            // extended auth
            this.authMethod = authMethodOpt.get();
            this.extendedAuthFuture = new CompletableFuture<>();
            // resume read
            ctx.channel().config().setAutoRead(true);
            ctx.read();
            extendedAuth(AuthUtil.buildMQTT5ExtendedAuthData(ctx.channel(), message));
            return extendedAuthFuture;
        }
    }

    @Override
    protected CompletableFuture<AuthResult> checkConnectPermission(MqttConnectMessage message,
                                                                   SuccessInfo successInfo) {
        MQTTAction connAction = buildConnAction(toUserProperties(message.variableHeader().properties()));
        return authProvider.checkPermission(successInfo.clientInfo(), connAction)
            .thenApply(checkResult -> {
                switch (checkResult.getTypeCase()) {
                    case GRANTED -> {
                        return AuthResult.ok(successInfo);
                    }
                    case DENIED -> {
                        return goAway(MqttMessageBuilders
                                .connAck()
                                .properties(MQTT5MessageBuilders.connAckProperties()
                                    .reasonString("Not authorized")
                                    .build())
                                .returnCode(CONNECTION_REFUSED_NOT_AUTHORIZED_5)
                                .build(),
                            getLocal(NotAuthorizedClient.class)
                                .tenantId(successInfo.clientInfo().getTenantId())
                                .userId(successInfo.clientInfo().getMetadataOrDefault(MQTT_USER_ID_KEY, ""))
                                .clientId(connMsg.payload().clientIdentifier())
                                .peerAddress(ChannelAttrs.socketAddress(ctx.channel())));
                    }
                    default -> {
                        return goAway(MqttMessageBuilders
                                .connAck()
                                .properties(MQTT5MessageBuilders.connAckProperties()
                                    .reasonString("Failed to check connect permission")
                                    .build())
                                .returnCode(CONNECTION_REFUSED_UNSPECIFIED_ERROR)
                                .build(),
                            getLocal(AuthError.class)
                                .cause("Failed to check connect permission")
                                .peerAddress(ChannelAttrs.socketAddress(ctx.channel())));
                    }
                }
            });
    }

    private void extendedAuth(MQTT5ExtendedAuthData authData) {
        this.isAuthing = true;
        authProvider.extendedAuth(authData)
            .thenAcceptAsync(authResult -> {
                this.isAuthing = false;
                final InetSocketAddress clientAddress = ChannelAttrs.socketAddress(ctx.channel());
                switch (authResult.getTypeCase()) {
                    case SUCCESS -> extendedAuthFuture.complete(ok(authResult.getSuccess(),
                        buildClientInfo(clientAddress, authResult.getSuccess())));
                    case CONTINUE -> {
                        Continue authContinue = authResult.getContinue();
                        MQTT5MessageBuilders.AuthBuilder authBuilder = MQTT5MessageBuilders
                            .auth(authMethod)
                            .reasonCode(MQTT5AuthReasonCode.Continue)
                            .authData(authContinue.getAuthData());
                        if (requestProblemInfo) {
                            // [MQTT-3.1.2-29]
                            if (authContinue.hasReason()) {
                                authBuilder.reasonString(authContinue.getReason());
                            }
                            authBuilder.userProperties(authContinue.getUserProps());
                        }
                        ctx.channel().writeAndFlush(authBuilder.build());
                    }
                    default -> {
                        Failed failed = authResult.getFailed();
                        switch (failed.getCode()) {
                            case NotAuthorized -> extendedAuthFuture.complete(goAway(MqttMessageBuilders
                                    .connAck()
                                    .returnCode(CONNECTION_REFUSED_NOT_AUTHORIZED_5)
                                    .build(),
                                getLocal(NotAuthorizedClient.class)
                                    .tenantId(failed.getTenantId())
                                    .userId(failed.getUserId())
                                    .clientId(connMsg.payload().clientIdentifier())
                                    .peerAddress(clientAddress)));
                            case BadPass -> extendedAuthFuture.complete(goAway(MqttMessageBuilders
                                    .connAck()
                                    .returnCode(CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD)
                                    .build(),
                                getLocal(UnauthenticatedClient.class)
                                    .tenantId(failed.getTenantId())
                                    .userId(failed.getUserId())
                                    .clientId(connMsg.payload().clientIdentifier())
                                    .peerAddress(clientAddress)));
                            case Banned -> extendedAuthFuture.complete(goAway(MqttMessageBuilders
                                    .connAck()
                                    .returnCode(CONNECTION_REFUSED_BANNED)
                                    .build(),
                                getLocal(UnauthenticatedClient.class)
                                    .tenantId(failed.getTenantId())
                                    .userId(failed.getUserId())
                                    .clientId(connMsg.payload().clientIdentifier())
                                    .peerAddress(clientAddress)));
                            // fallthrough
                            default -> {
                                log.error("Unexpected ext-auth error:{}", failed.getReason());
                                extendedAuthFuture.complete(goAway(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_UNSPECIFIED_ERROR)
                                        .build(),
                                    getLocal(AuthError.class).cause(failed.getReason())
                                        .peerAddress(clientAddress)));
                            }
                        }
                    }
                }
            }, ctx.executor());
    }

    private ClientInfo buildClientInfo(InetSocketAddress clientAddress, Success success) {
        String clientId = connMsg.payload().clientIdentifier();
        if (Strings.isNullOrEmpty(clientId)) {
            clientId = ctx.channel().id().asLongText();
        }
        ClientInfo.Builder clientInfoBuilder = ClientInfo.newBuilder()
            .setTenantId(success.getTenantId())
            .setType(MQTT_TYPE_VALUE)
            .putAllMetadata(success.getAttrsMap()) // custom attrs
            .putMetadata(MQTT_PROTOCOL_VER_KEY, MQTT_PROTOCOL_VER_5_VALUE)
            .putMetadata(MQTT_USER_ID_KEY, success.getUserId())
            .putMetadata(MQTT_CLIENT_ID_KEY, clientId)
            .putMetadata(MQTT_CHANNEL_ID_KEY, ctx.channel().id().asLongText())
            .putMetadata(MQTT_CLIENT_ADDRESS_KEY,
                Optional.ofNullable(clientAddress).map(InetSocketAddress::toString).orElse(""))
            .putMetadata(MQTT_CLIENT_BROKER_KEY, ChannelAttrs.mqttSessionContext(ctx).serverId);
        return clientInfoBuilder.build();
    }

    @Override
    protected void handleMqttMessage(MqttMessage message) {
        if (isAuthing) {
            switch (message.fixedHeader().messageType()) {
                case AUTH -> handleGoAway(GoAway.now(getLocal(ProtocolError.class)
                    .statement("Enhanced Auth in progress")
                    .peerAddress(ChannelAttrs.socketAddress(ctx.channel()))));
                case DISCONNECT -> handleGoAway(GoAway.now(getLocal(EnhancedAuthAbortByClient.class)));
                default -> handleGoAway(GoAway.now(getLocal(ProtocolError.class)
                    .statement("Unexpected control packet during enhanced auth: " + message.fixedHeader().messageType())
                    .peerAddress(ChannelAttrs.socketAddress(ctx.channel()))));
            }
        } else {
            switch (message.fixedHeader().messageType()) {
                case AUTH -> {
                    MQTT5AuthReasonCode reasonCode = MQTT5AuthReasonCode.valueOf(
                        ((MqttReasonCodeAndPropertiesVariableHeader) message.variableHeader()).reasonCode());
                    if (reasonCode != MQTT5AuthReasonCode.Continue) {
                        handleGoAway(GoAway.now(getLocal(ProtocolError.class)
                            .statement("Invalid auth reason code: " + reasonCode)
                            .peerAddress(ChannelAttrs.socketAddress(ctx.channel()))));
                        return;
                    }
                    MQTT5ExtendedAuthData authData = AuthUtil.buildMQTT5ExtendedAuthData(message, false);
                    if (!authData.getAuth().getAuthMethod().equals(authMethod)) {
                        handleGoAway(GoAway.now(getLocal(ProtocolError.class)
                            .statement("Invalid auth method: " + authData.getAuth().getAuthMethod())
                            .peerAddress(ChannelAttrs.socketAddress(ctx.channel()))));
                        return;
                    }
                    extendedAuth(authData);
                }
                case DISCONNECT -> handleGoAway(GoAway.now(getLocal(EnhancedAuthAbortByClient.class)));
                default -> handleGoAway(GoAway.now(getLocal(ProtocolError.class)
                    .statement("Unexpected control packet during enhanced auth: " + message.fixedHeader().messageType())
                    .peerAddress(ChannelAttrs.socketAddress(ctx.channel()))));
            }
        }
    }

    @Override
    protected GoAway onNoEnoughResources(MqttConnectMessage message, TenantResourceType resourceType,
                                         ClientInfo clientInfo) {
        return new GoAway(MqttMessageBuilders.connAck()
            .returnCode(CONNECTION_REFUSED_QUOTA_EXCEEDED)
            .properties(MQTT5MessageBuilders.connAckProperties()
                .reasonString(resourceType.name())
                .build())
            .build(),
            getLocal(OutOfTenantResource.class)
                .reason(resourceType.name())
                .clientInfo(clientInfo),
            getLocal(ResourceThrottled.class)
                .reason(resourceType.name())
                .clientInfo(clientInfo));
    }

    @Override
    protected GoAway validate(MqttConnectMessage message, TenantSettings settings, ClientInfo clientInfo) {
        if (message.variableHeader().version() == 5 && !settings.mqtt5Enabled) {
            return new GoAway(MqttMessageBuilders.connAck()
                .returnCode(CONNECTION_REFUSED_UNSUPPORTED_PROTOCOL_VERSION)
                .properties(MQTT5MessageBuilders.connAckProperties()
                    .reasonString("MQTT5 not enabled")
                    .build())
                .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT5 not enabled")
                    .clientInfo(clientInfo));
        }
        if (IMQTTMessageSizer.mqtt5().lastWillSize(message) > settings.maxLastWillSize) {
            return new GoAway(MqttMessageBuilders.connAck()
                .returnCode(CONNECTION_REFUSED_PACKET_TOO_LARGE)
                .properties(MQTT5MessageBuilders.connAckProperties()
                    .reasonString("Too large connect packet: max=" + settings.maxPacketSize)
                    .build())
                .build(),
                getLocal(ProtocolViolation.class)
                    .statement("Too large connect packet")
                    .clientInfo(clientInfo));
        }
        Optional<Integer> requestMaxPacketSize = maximumPacketSize(message.variableHeader().properties());
        if (requestMaxPacketSize.isPresent()) {
            if (requestMaxPacketSize.get() < MIN_CONTROL_PACKET_SIZE) {
                return new GoAway(MqttMessageBuilders.connAck()
                    .returnCode(CONNECTION_REFUSED_IMPLEMENTATION_SPECIFIC)
                    .properties(MQTT5MessageBuilders.connAckProperties()
                        .reasonString("Invalid max packet size: " + requestMaxPacketSize.get())
                        .build())
                    .build(),
                    getLocal(ProtocolViolation.class)
                        .statement("Invalid max packet size:" + requestMaxPacketSize.get())
                        .clientInfo(clientInfo));
            }
            if (requestMaxPacketSize.get() > settings.maxPacketSize) {
                return new GoAway(MqttMessageBuilders
                    .connAck()
                    .returnCode(CONNECTION_REFUSED_IMPLEMENTATION_SPECIFIC)
                    .properties(MQTT5MessageBuilders.connAckProperties()
                        .reasonString("Max packet size: " + settings.maxPacketSize)
                        .build())
                    .build(),
                    getLocal(ProtocolViolation.class)
                        .statement("Invalid max packet size: " + requestMaxPacketSize.get())
                        .clientInfo(clientInfo));
            }
        }
        Optional<Integer> topicAliasMaximum = topicAliasMaximum(message.variableHeader().properties());
        if (topicAliasMaximum.orElse(0) > settings.maxTopicAlias) {
            return new GoAway(MqttMessageBuilders
                .connAck()
                .returnCode(CONNECTION_REFUSED_QUOTA_EXCEEDED)
                .properties(MQTT5MessageBuilders.connAckProperties()
                    .reasonString("Too large TopicAliasMaximum: max=" + settings.maxTopicAlias)
                    .build())
                .build(),
                getLocal(InvalidTopic.class)
                    .topic(message.payload().willTopic())
                    .clientInfo(clientInfo));
        }
        if (message.variableHeader().isWillFlag()) {
            // will topic conforms to tenant spec limit
            if (!TopicUtil.isValidTopic(message.payload().willTopic(),
                settings.maxTopicLevelLength,
                settings.maxTopicLevels,
                settings.maxTopicLength)) {
                return new GoAway(MqttMessageBuilders
                    .connAck()
                    .returnCode(CONNECTION_REFUSED_TOPIC_NAME_INVALID)
                    .properties(MQTT5MessageBuilders.connAckProperties()
                        .reasonString("Invalid will topic:" + message.payload().willTopic())
                        .build())
                    .build(),
                    getLocal(InvalidTopic.class)
                        .topic(message.payload().willTopic())
                        .clientInfo(clientInfo));
            }
            // if retain enabled?
            if (message.variableHeader().isWillRetain() && !settings.retainEnabled) {
                return new GoAway(MqttMessageBuilders
                    .connAck()
                    .returnCode(CONNECTION_REFUSED_RETAIN_NOT_SUPPORTED)
                    .build(),
                    getLocal(ProtocolViolation.class)
                        .statement("Retain not supported")
                        .clientInfo(clientInfo));
            }
            if (message.variableHeader().willQos() > settings.maxQoS.getNumber()) {
                return new GoAway(MqttMessageBuilders
                    .connAck()
                    .returnCode(CONNECTION_REFUSED_QOS_NOT_SUPPORTED)
                    .build(),
                    getLocal(ProtocolViolation.class)
                        .statement("Will QoS not supported")
                        .clientInfo(clientInfo));
            }
            if (settings.payloadFormatValidationEnabled
                && isUTF8Payload(connMsg.payload().willProperties())
                && !UTF8Util.isValidUTF8Payload(connMsg.payload().willMessageInBytes())) {
                return new GoAway(MqttMessageBuilders
                    .connAck()
                    .properties(MQTT5MessageBuilders.connAckProperties()
                        .reasonString("Invalid payload format")
                        .build())
                    .returnCode(CONNECTION_REFUSED_PAYLOAD_FORMAT_INVALID) // [MQTT-4.13.1-1]
                    .build(),
                    getLocal(ProtocolViolation.class)
                        .statement("Invalid payload format")
                        .clientInfo(clientInfo));
            }
        }
        return null;
    }

    @Override
    protected GoAway needRedirect(ClientInfo clientInfo) {
        Optional<Redirection> redirection = clientBalancer.needRedirect(clientInfo);
        return redirection.map(value -> new GoAway(MqttMessageBuilders
                .connAck()
                .properties(MQTT5MessageBuilders.connAckProperties()
                    .serverReference(value.serverReference().orElse(null))
                    .build())
                .returnCode(value.permanentMove()
                    ? CONNECTION_REFUSED_SERVER_MOVED : CONNECTION_REFUSED_USE_ANOTHER_SERVER) // [MQTT-4.13.1-1]
                .build(),
                getLocal(Redirect.class)
                    .isPermanent(value.permanentMove())
                    .serverReference(value.serverReference().orElse(null))
                    .clientInfo(clientInfo)))
            .orElse(null);
    }

    @Override
    protected LWT getWillMessage(MqttConnectMessage message, ClientInfo clientInfo) {
        return toWillMessage(message, clientInfo, sessionCtx.userPropsCustomizer);
    }

    @Override
    protected boolean isCleanStart(MqttConnectMessage message, TenantSettings settings) {
        return settings.forceTransient || message.variableHeader().isCleanSession();
    }

    @Override
    protected int getSessionExpiryInterval(MqttConnectMessage message, TenantSettings settings) {
        if (settings.forceTransient) {
            return 0;
        }
        int requestSEI = Optional.ofNullable((MqttProperties.IntegerProperty) message.variableHeader()
                .properties()
                .getProperty(SESSION_EXPIRY_INTERVAL.value()))
            .map(MqttProperties.MqttProperty::value)
            .orElse(0);
        if (requestSEI == 0) {
            return 0;
        }
        if (Integer.compareUnsigned(requestSEI, settings.minSEI) < 0) {
            return settings.minSEI;
        }
        if (Integer.compareUnsigned(requestSEI, settings.maxSEI) > 0) {
            return settings.maxSEI;
        }
        return requestSEI;
    }

    @Override
    protected GoAway onInboxCallError(ClientInfo clientInfo, String reason) {
        return new GoAway(MqttMessageBuilders
            .connAck()
            .returnCode(CONNECTION_REFUSED_IMPLEMENTATION_SPECIFIC)
            .properties(MQTT5MessageBuilders.connAckProperties()
                .reasonString(reason)
                .build())
            .build(),
            getLocal(InboxTransientError.class)
                .reason(reason)
                .clientInfo(clientInfo));
    }

    @Override
    protected GoAway onInboxCallRetry(ClientInfo clientInfo, String reason) {
        return new GoAway(MqttMessageBuilders
            .connAck()
            .returnCode(CONNECTION_REFUSED_IMPLEMENTATION_SPECIFIC)
            .properties(MQTT5MessageBuilders.connAckProperties()
                .reasonString(reason)
                .build())
            .build(),
            getLocal(InboxTransientError.class)
                .reason(reason)
                .clientInfo(clientInfo));
    }

    @Override
    protected GoAway onInboxCallBusy(ClientInfo clientInfo, String reason) {
        return new GoAway(MqttMessageBuilders
            .connAck()
            .returnCode(CONNECTION_REFUSED_SERVER_BUSY)
            .properties(MQTT5MessageBuilders.connAckProperties()
                .reasonString(reason)
                .build())
            .build(),
            getLocal(ServerBusy.class)
                .reason(reason)
                .clientInfo(clientInfo));
    }

    @Override
    protected MQTTSessionHandler buildTransientSessionHandler(MqttConnectMessage connMsg,
                                                              TenantSettings settings,
                                                              ITenantMeter tenantMeter,
                                                              String userSessionId,
                                                              int keepAliveSeconds,
                                                              LWT willMessage, // nullable
                                                              ClientInfo clientInfo,
                                                              ChannelHandlerContext ctx) {
        return MQTT5TransientSessionHandler.builder()
            .connMsg(connMsg)
            .settings(settings)
            .tenantMeter(tenantMeter)
            .oomCondition(or(DirectMemPressureCondition.INSTANCE, HeapMemPressureCondition.INSTANCE))
            .userSessionId(userSessionId)
            .clientInfo(clientInfo)
            .keepAliveTimeSeconds(keepAliveSeconds)
            .willMessage(willMessage)
            .ctx(ctx)
            .build();
    }

    @Override
    protected MQTTSessionHandler buildPersistentSessionHandler(MqttConnectMessage connMsg,
                                                               TenantSettings settings,
                                                               ITenantMeter tenantMeter,
                                                               String userSessionId,
                                                               int keepAliveSeconds,
                                                               int sessionExpiryInterval,
                                                               InboxVersion inboxVersion,
                                                               LWT noDelayLWT, // nullable
                                                               ClientInfo clientInfo,
                                                               ChannelHandlerContext ctx) {
        return MQTT5PersistentSessionHandler.builder()
            .connMsg(connMsg)
            .settings(settings)
            .tenantMeter(tenantMeter)
            .oomCondition(or(DirectMemPressureCondition.INSTANCE, HeapMemPressureCondition.INSTANCE))
            .userSessionId(userSessionId)
            .clientInfo(clientInfo)
            .inboxVersion(inboxVersion)
            .keepAliveTimeSeconds(keepAliveSeconds)
            .sessionExpirySeconds(sessionExpiryInterval)
            .noDelayLWT(noDelayLWT)
            .ctx(ctx)
            .build();
    }

    @Override
    protected MqttConnAckMessage onConnected(MqttConnectMessage connMsg,
                                             TenantSettings settings,
                                             String userSessionId,
                                             int keepAliveSeconds,
                                             int sessionExpiryInterval,
                                             boolean sessionExists,
                                             ClientInfo clientInfo,
                                             Optional<String> responseInfo, // mqtt5
                                             Optional<ByteString> authData, // mqtt5
                                             UserProperties userProperties) { // mqtt5
        MQTT5MessageBuilders.ConnAckPropertiesBuilder connPropsBuilder = MQTT5MessageBuilders.connAckProperties();
        if (connMsg.variableHeader().keepAliveTimeSeconds() != keepAliveSeconds) {
            connPropsBuilder.serverKeepAlive(keepAliveSeconds);
        }
        MqttProperties connProps = connMsg.variableHeader().properties();
        MqttProperties.IntegerProperty intProp =
            (MqttProperties.IntegerProperty) connProps.getProperty(SESSION_EXPIRY_INTERVAL.value());
        if (intProp != null && intProp.value() != sessionExpiryInterval) {
            connPropsBuilder.sessionExpiryInterval(sessionExpiryInterval);
        }
        if (Strings.isNullOrEmpty(connMsg.payload().clientIdentifier())) {
            connPropsBuilder.assignedClientId(clientInfo.getMetadataOrDefault(MQTT_CLIENT_ID_KEY, ""));
        }
        if (!settings.retainEnabled) {
            connPropsBuilder.retainAvailable(false);
        }
        if (!settings.wildcardSubscriptionEnabled) {
            connPropsBuilder.wildcardSubscriptionAvailable(false);
        }
        if (!settings.subscriptionIdentifierEnabled) {
            connPropsBuilder.subscriptionIdentifiersAvailable(false);
        }
        if (!settings.sharedSubscriptionEnabled) {
            connPropsBuilder.sharedSubscriptionAvailable(false);
        }
        if (settings.maxQoS != QoS.EXACTLY_ONCE) {
            connPropsBuilder.maximumQos((byte) settings.maxQoS.getNumber());
        }
        connPropsBuilder.maximumPacketSize(settings.maxPacketSize);
        connPropsBuilder.topicAliasMaximum(settings.maxTopicAlias);
        connPropsBuilder.receiveMaximum(settings.receiveMaximum);
        if (requestResponseInformation(connProps) && responseInfo.isPresent()) {
            // include response information only when client requested it
            connPropsBuilder.responseInformation(responseInfo.get());
        }
        authMethod(connProps).ifPresent(methodName -> {
            connPropsBuilder.authenticationMethod(methodName);
            authData.ifPresent(bytes -> connPropsBuilder.authenticationData(bytes.toByteArray()));
        });
        for (StringPair userProp : userProperties.getUserPropertiesList()) {
            connPropsBuilder.userProperty(userProp.getKey(), userProp.getValue());
        }
        return MqttMessageBuilders
            .connAck()
            .sessionPresent(sessionExists)
            .properties(connPropsBuilder.build())
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
            .build();
    }

    @Override
    protected int maxPacketSize(MqttConnectMessage connMsg, TenantSettings settings) {
        return maximumPacketSize(connMsg.variableHeader().properties()).orElse(settings.maxPacketSize);
    }
}
