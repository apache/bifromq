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
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.isUTF8Payload;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.messageExpiryInterval;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.receiveMaximum;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.requestProblemInformation;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.responseTopic;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.subscriptionIdentifier;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.toUserProperties;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.topicAlias;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.topicAliasMaximum;
import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ADDRESS_KEY;
import static org.apache.bifromq.util.TopicUtil.isValidTopic;
import static org.apache.bifromq.util.UTF8Util.isWellFormed;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_BUSY;

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.storage.proto.RetainHandling;
import org.apache.bifromq.inbox.storage.proto.TopicFilterOption;
import org.apache.bifromq.mqtt.handler.IMQTTProtocolHelper;
import org.apache.bifromq.mqtt.handler.MQTTSessionHandler;
import org.apache.bifromq.mqtt.handler.TenantSettings;
import org.apache.bifromq.mqtt.handler.record.ProtocolResponse;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5DisconnectReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5PubAckReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5PubCompReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5PubRecReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5PubRelReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5SubAckReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5UnsubAckReasonCode;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.eventcollector.Event;
import org.apache.bifromq.plugin.eventcollector.OutOfTenantResource;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.BadPacket;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ByServer;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ExceedPubRate;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ExceedReceivingLimit;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.Idle;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InboxTransientError;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InvalidTopic;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.Kicked;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.MalformedTopic;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.MalformedTopicFilter;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.NoPubPermission;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ProtocolViolation;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.Redirect;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ResourceThrottled;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ServerBusy;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.TooLargeSubscription;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.TooLargeUnsubscription;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS1PubAcked;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS2PubReced;
import org.apache.bifromq.retain.rpc.proto.RetainReply;
import org.apache.bifromq.sysprops.props.SanityCheckMqttUtf8String;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.UserProperties;
import org.apache.bifromq.util.TopicUtil;
import org.apache.bifromq.util.UTF8Util;
import org.apache.bifromq.plugin.resourcethrottler.TenantResourceType;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

public class MQTT5ProtocolHelper implements IMQTTProtocolHelper {
    private static final boolean SANITY_CHECK = SanityCheckMqttUtf8String.INSTANCE.get();
    private final TenantSettings settings;
    private final ClientInfo clientInfo;
    private final int clientReceiveMaximum;
    private final boolean requestProblemInfo;
    private final ReceiverTopicAliasManager receiverTopicAliasManager;
    private final SenderTopicAliasManager senderTopicAliasManager;


    public MQTT5ProtocolHelper(MqttConnectMessage connMsg, TenantSettings settings, ClientInfo clientInfo) {
        this.settings = settings;
        this.clientInfo = clientInfo;
        this.receiverTopicAliasManager = new ReceiverTopicAliasManager();
        this.senderTopicAliasManager =
            new SenderTopicAliasManager(topicAliasMaximum(connMsg.variableHeader().properties()).orElse(0),
                Duration.ofSeconds(60));
        this.clientReceiveMaximum = receiveMaximum(connMsg.variableHeader().properties()).orElse(65535);
        this.requestProblemInfo = requestProblemInformation(connMsg.variableHeader().properties());
    }

    @Override
    public UserProperties getUserProps(MqttPublishMessage mqttMessage) {
        return toUserProperties(mqttMessage.variableHeader().properties());
    }

    @Override
    public UserProperties getUserProps(MqttUnsubscribeMessage mqttMessage) {
        return toUserProperties(mqttMessage.idAndPropertiesVariableHeader().properties());
    }

    @Override
    public boolean checkPacketIdUsage() {
        return true;
    }

    @Override
    public ProtocolResponse onInboxTransientError(String reason) {
        return farewell(
            MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ImplementationSpecificError)
                .reasonString(reason).build(),
            getLocal(InboxTransientError.class).reason(reason).clientInfo(clientInfo));
    }

    @Override
    public ProtocolResponse onInboxBusy(String reason) {
        return farewell(MqttMessageBuilders
                .connAck()
                .returnCode(CONNECTION_REFUSED_SERVER_BUSY)
                .properties(MQTT5MessageBuilders.connAckProperties().build())
                .build(),
            getLocal(ServerBusy.class)
                .reason(reason)
                .clientInfo(clientInfo));
    }

    @Override
    public Optional<Integer> sessionExpiryIntervalOnDisconnect(MqttMessage disconnectMessage) {
        MqttReasonCodeAndPropertiesVariableHeader variableHeader =
            (MqttReasonCodeAndPropertiesVariableHeader) disconnectMessage.variableHeader();
        return Optional.ofNullable((MqttProperties.IntegerProperty) variableHeader.properties()
                .getProperty(MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value()))
            .map(MqttProperties.MqttProperty::value);
    }

    @Override
    public ProtocolResponse onDisconnect() {
        return farewellNow(
            MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ServerShuttingDown).build(),
            getLocal(ByServer.class).clientInfo(clientInfo));
    }

    @Override
    public ProtocolResponse onResourceExhaustedDisconnect(TenantResourceType resourceType) {
        return farewellNow(MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.QuotaExceeded)
                .reasonString(resourceType.name()).build(),
            getLocal(OutOfTenantResource.class).reason(resourceType.name()).clientInfo(clientInfo),
            getLocal(ResourceThrottled.class).reason(resourceType.name()).clientInfo(clientInfo));
    }

    @Override
    public ProtocolResponse respondDisconnectProtocolError() {
        return farewell(
            MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ProtocolError) // Protocol Error
                .reasonString("MQTT5-3.14.2.2.2").build(),
            getLocal(ProtocolViolation.class).statement("MQTT5-3.14.2.2.2").clientInfo(clientInfo));
    }

    @Override
    public boolean isNormalDisconnect(MqttMessage message) {
        MqttReasonCodeAndPropertiesVariableHeader variableHeader =
            (MqttReasonCodeAndPropertiesVariableHeader) message.variableHeader();
        return variableHeader.reasonCode() == MQTT5DisconnectReasonCode.Normal.value();
    }

    @Override
    public boolean isDisconnectWithLWT(MqttMessage message) {
        MqttReasonCodeAndPropertiesVariableHeader variableHeader =
            (MqttReasonCodeAndPropertiesVariableHeader) message.variableHeader();
        return variableHeader.reasonCode() == MQTT5DisconnectReasonCode.DisconnectWithWillMessage.value();
    }

    @Override
    public ProtocolResponse respondDecodeError(MqttMessage message) {
        if (message.decoderResult().cause() instanceof TooLongFrameException) {
            return farewell(
                MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.PacketTooLarge).build(),
                getLocal(BadPacket.class).cause(message.decoderResult().cause()).clientInfo(clientInfo));
        }
        return farewell(MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.MalformedPacket)
                .reasonString(message.decoderResult().cause().getMessage()).build(),
            getLocal(BadPacket.class).cause(message.decoderResult().cause()).clientInfo(clientInfo));
    }

    @Override
    public ProtocolResponse respondDuplicateConnect(MqttConnectMessage message) {
        return farewell(MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                .reasonString("MQTT5-3.1.0-2").build(),
            getLocal(ProtocolViolation.class).statement("MQTT5-3.1.0-2").clientInfo(clientInfo));
    }

    @Override
    public ProtocolResponse validateSubMessage(MqttSubscribeMessage message) {
        List<MqttTopicSubscription> topicSubscriptions = message.payload().topicSubscriptions();
        if (topicSubscriptions.isEmpty()) {
            // Ignore instead of disconnect [MQTT-3.8.3-3]
            return farewell(MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                    .reasonString("MQTT5-3.8.3-2").build(),
                getLocal(ProtocolViolation.class).statement("MQTT5-3.8.3-2").clientInfo(clientInfo));
        }
        if (topicSubscriptions.size() > settings.maxTopicFiltersPerSub) {
            return farewell(
                MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.AdministrativeAction).build(),
                getLocal(TooLargeSubscription.class).actual(topicSubscriptions.size())
                    .max(settings.maxTopicFiltersPerSub).clientInfo(clientInfo));
        }
        for (MqttTopicSubscription topicSub : topicSubscriptions) {
            if (!isWellFormed(topicSub.topicFilter(), SANITY_CHECK)) {
                return farewell(MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.MalformedPacket)
                        .reasonString("Malformed topic filter:" + topicSub.topicFilter()).build(),
                    getLocal(MalformedTopicFilter.class).topicFilter(topicSub.topicFilter()).clientInfo(clientInfo));
            }
        }
        Optional<Integer> subId = Optional.ofNullable(
                (MqttProperties.IntegerProperty) message.idAndPropertiesVariableHeader().properties()
                    .getProperty(MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER.value()))
            .map(MqttProperties.MqttProperty::value);
        if (subId.isPresent()) {
            if (subId.get() == 0) {
                return farewell(MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                        .reasonString("MQTT5-3.8.2.1.2").build(),
                    getLocal(ProtocolViolation.class).statement("MQTT5-3.8.2.1.2").clientInfo(clientInfo));
            }
            if (!settings.subscriptionIdentifierEnabled) {
                return response(MQTT5MessageBuilders.subAck().packetId(message.variableHeader().messageId())
                    .reasonCodes(
                        topicSubscriptions.stream().map(s -> MQTT5SubAckReasonCode.SubscriptionIdentifierNotSupported)
                            .toArray(MQTT5SubAckReasonCode[]::new)).build());
            }
        }
        return null;
    }

    @Override
    public List<SubTask> getSubTask(MqttSubscribeMessage message) {
        Optional<Integer> subId = Optional.ofNullable(
                (MqttProperties.IntegerProperty) message.idAndPropertiesVariableHeader().properties()
                    .getProperty(MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER.value()))
            .map(MqttProperties.MqttProperty::value);
        UserProperties userProps = toUserProperties(message.idAndPropertiesVariableHeader().properties());
        return message.payload().topicSubscriptions().stream().map(sub -> {
            TopicFilterOption.Builder optionBuilder =
                TopicFilterOption.newBuilder().setQos(QoS.forNumber(sub.option().qos().value()))
                    .setRetainAsPublished(sub.option().isRetainAsPublished()).setNoLocal(sub.option().isNoLocal())
                    .setRetainHandling(RetainHandling.forNumber(sub.option().retainHandling().value()))
                    .setIncarnation(HLC.INST.get());
            subId.ifPresent(optionBuilder::setSubId);
            return new SubTask(sub.topicFilter(), optionBuilder.build(), userProps);
        }).toList();
    }

    @Override
    public ProtocolResponse onSubBackPressured(MqttSubscribeMessage subMessage) {
        return farewell(MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ServerBusy)
                .reasonString("Too many subscribe").build(),
            getLocal(ServerBusy.class).reason("Too many subscribe").clientInfo(clientInfo));
    }

    @Override
    public ProtocolResponse buildSubAckMessage(MqttSubscribeMessage subMessage, List<SubResult> results) {
        MQTT5MessageBuilders.SubAckBuilder subAckBuilder =
            MQTT5MessageBuilders.subAck().packetId(subMessage.variableHeader().messageId());
        MQTT5SubAckReasonCode[] reasonCodes = new MQTT5SubAckReasonCode[results.size()];
        assert subMessage.payload().topicSubscriptions().size() == results.size();
        for (int i = 0; i < results.size(); i++) {
            reasonCodes[i] = switch (results.get(i)) {
                case OK, EXISTS -> MQTT5SubAckReasonCode.valueOf(
                    subMessage.payload().topicSubscriptions().get(i).option().qos().value());
                case EXCEED_LIMIT -> MQTT5SubAckReasonCode.QuotaExceeded;
                case TOPIC_FILTER_INVALID -> MQTT5SubAckReasonCode.TopicFilterInvalid;
                case NOT_AUTHORIZED -> MQTT5SubAckReasonCode.NotAuthorized;
                case WILDCARD_NOT_SUPPORTED -> MQTT5SubAckReasonCode.WildcardSubscriptionsNotSupported;
                case SUBSCRIPTION_IDENTIFIER_NOT_SUPPORTED -> MQTT5SubAckReasonCode.SubscriptionIdentifierNotSupported;
                case SHARED_SUBSCRIPTION_NOT_SUPPORTED -> MQTT5SubAckReasonCode.SharedSubscriptionsNotSupported;
                case TRY_LATER -> {
                    subAckBuilder.reasonString(results.get(i).name());
                    yield MQTT5SubAckReasonCode.ImplementationSpecificError;
                }
                default -> MQTT5SubAckReasonCode.UnspecifiedError;
            };
        }
        return response(subAckBuilder.reasonCodes(reasonCodes).build());
    }

    @Override
    public MqttSubAckMessage respondPacketIdInUse(MqttSubscribeMessage message) {
        return MQTT5MessageBuilders.subAck().packetId(message.variableHeader().messageId()).reasonCodes(
            message.payload().topicSubscriptions().stream().map(v -> MQTT5SubAckReasonCode.PacketIdentifierInUse)
                .toArray(MQTT5SubAckReasonCode[]::new)).build();
    }

    @Override
    public ProtocolResponse validateUnsubMessage(MqttUnsubscribeMessage message) {
        List<String> topicFilters = message.payload().topics();
        if (topicFilters.isEmpty()) {
            // Ignore instead of disconnect [3.10.3-2]
            return farewell(MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                    .reasonString("MQTT5-3.10.3-2").build(),
                getLocal(ProtocolViolation.class).statement("MQTT5-3.10.3-2").clientInfo(clientInfo));
        }
        if (topicFilters.size() > settings.maxTopicFiltersPerSub) {
            return farewell(
                MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.AdministrativeAction).build(),
                getLocal(TooLargeUnsubscription.class).max(settings.maxTopicFiltersPerSub).actual(topicFilters.size())
                    .clientInfo(clientInfo));
        }

        for (String topicFilter : topicFilters) {
            if (!isWellFormed(topicFilter, SANITY_CHECK)) {
                return farewell(MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.MalformedPacket)
                        .reasonString("Malformed topic filter:" + topicFilter).build(),
                    getLocal(MalformedTopicFilter.class).topicFilter(topicFilter).clientInfo(clientInfo),
                    getLocal(ProtocolViolation.class).statement("MQTT5-3.8.3-2").clientInfo(clientInfo));
            }
        }
        return null;
    }

    @Override
    public MqttUnsubAckMessage respondPacketIdInUse(MqttUnsubscribeMessage message) {
        return MQTT5MessageBuilders.unsubAck().packetId(message.variableHeader().messageId()).addReasonCodes(
            message.payload().topics().stream().map(v -> MQTT5UnsubAckReasonCode.PacketIdentifierInUse)
                .toArray(MQTT5UnsubAckReasonCode[]::new)).build();
    }

    @Override
    public ProtocolResponse onUnsubBackPressured(MqttUnsubscribeMessage unsubMessage) {
        return farewell(MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ServerBusy)
                .reasonString("Too many unsubscribe").build(),
            getLocal(ServerBusy.class).reason("Too many unsubscribe").clientInfo(clientInfo));
    }

    @Override
    public ProtocolResponse buildUnsubAckMessage(MqttUnsubscribeMessage unsubMessage, List<UnsubResult> results) {
        MQTT5MessageBuilders.UnsubAckBuilder unsubAckBuilder =
            MQTT5MessageBuilders.unsubAck().packetId(unsubMessage.variableHeader().messageId());
        MQTT5UnsubAckReasonCode[] reasonCodes = results.stream().map(result -> switch (result) {
            case OK -> MQTT5UnsubAckReasonCode.Success;
            case NO_SUB -> MQTT5UnsubAckReasonCode.NoSubscriptionExisted;
            case TOPIC_FILTER_INVALID -> MQTT5UnsubAckReasonCode.TopicFilterInvalid;
            case NOT_AUTHORIZED -> MQTT5UnsubAckReasonCode.NotAuthorized;
            case TRY_LATER -> {
                unsubAckBuilder.reasonString(result.name());
                yield MQTT5UnsubAckReasonCode.ImplementationSpecificError;
            }
            default -> MQTT5UnsubAckReasonCode.UnspecifiedError;
        }).toArray(MQTT5UnsubAckReasonCode[]::new);

        return response(MQTT5MessageBuilders.unsubAck().packetId(unsubMessage.variableHeader().messageId())
            .addReasonCodes(reasonCodes).build());
    }

    @Override
    public MqttMessage onPubRelReceived(MqttMessage message, boolean packetIdFound) {
        if (packetIdFound) {
            return MQTT5MessageBuilders.pubComp(requestProblemInfo)
                .packetId(((MqttMessageIdVariableHeader) message.variableHeader()).messageId())
                .reasonCode(MQTT5PubCompReasonCode.Success).build();
        } else {
            return MQTT5MessageBuilders.pubComp(requestProblemInfo)
                .packetId(((MqttMessageIdVariableHeader) message.variableHeader()).messageId())
                .reasonCode(MQTT5PubCompReasonCode.PacketIdentifierNotFound).build();
        }
    }

    @Override
    public boolean isQoS2Received(MqttMessage message) {
        MqttPubReplyMessageVariableHeader variableHeader = (MqttPubReplyMessageVariableHeader) message.variableHeader();
        MQTT5PubRecReasonCode reasonCode = MQTT5PubRecReasonCode.valueOf(variableHeader.reasonCode());
        return reasonCode == MQTT5PubRecReasonCode.Success || reasonCode == MQTT5PubRecReasonCode.NoMatchingSubscribers;
    }

    @Override
    public ProtocolResponse respondPubRecMsg(MqttMessage message, boolean packetIdNotFound) {
        MqttPubReplyMessageVariableHeader variableHeader = (MqttPubReplyMessageVariableHeader) message.variableHeader();
        int packetId = variableHeader.messageId();
        if (packetIdNotFound) {
            return farewell(MQTT5MessageBuilders.pubRel(requestProblemInfo).packetId(packetId)
                    .reasonCode(MQTT5PubRelReasonCode.PacketIdentifierNotFound).build(),
                getLocal(ProtocolViolation.class).statement("MQTT5-4.3.3-8").clientInfo(clientInfo));
        } else {
            return response(MQTT5MessageBuilders.pubRel(requestProblemInfo).packetId(packetId)
                .reasonCode(MQTT5PubRelReasonCode.Success).build());
        }
    }

    @Override
    public int clientReceiveMaximum() {
        return clientReceiveMaximum;
    }

    @Override
    public ProtocolResponse onKick(ClientInfo killer) {
        return farewellNow(MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.SessionTakenOver)
                .reasonString(killer.getMetadataOrDefault(MQTT_CLIENT_ADDRESS_KEY, "")).build(),
            getLocal(Kicked.class).kicker(killer).clientInfo(clientInfo));
    }

    @Override
    public ProtocolResponse onRedirect(boolean isPermanent, String serverReference) {
        return farewellNow(MQTT5MessageBuilders.disconnect().reasonCode(
                    isPermanent ? MQTT5DisconnectReasonCode.ServerMoved : MQTT5DisconnectReasonCode.UseAnotherServer)
                .serverReference(serverReference).build(),
            getLocal(Redirect.class).isPermanent(isPermanent).clientInfo(clientInfo));
    }

    @Override
    public MqttPublishMessage buildMqttPubMessage(int packetId, MQTTSessionHandler.SubMessage message, boolean isDup) {
        Optional<SenderTopicAliasManager.AliasCreationResult> aliasCreationResult =
            senderTopicAliasManager.tryAlias(message.topic());
        if (aliasCreationResult.isPresent()) {
            if (aliasCreationResult.get().isFirstTime()) {
                return MQTT5MessageBuilders.pub().packetId(packetId).setupAlias(true)
                    .topicAlias(aliasCreationResult.get().alias()).message(message).build();
            } else {
                return MQTT5MessageBuilders.pub().packetId(packetId).topicAlias(aliasCreationResult.get().alias())
                    .message(message).build();
            }
        }
        return MQTT5MessageBuilders.pub().packetId(packetId).message(message).build();
    }

    @Override
    public ProtocolResponse respondReceivingMaximumExceeded(MqttPublishMessage message) {
        return farewell(
            MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ReceiveMaximumExceeded).build(),
            getLocal(ExceedReceivingLimit.class).limit(settings.receiveMaximum).clientInfo(clientInfo));
    }

    @Override
    public ProtocolResponse respondPubRateExceeded(MqttPublishMessage message) {
        return farewell(
            MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.MessageRateToHigh).build(),
            getLocal(ExceedPubRate.class).limit(settings.maxMsgPerSec).clientInfo(clientInfo));
    }

    @Override
    public ProtocolResponse validatePubMessage(MqttPublishMessage message) {
        if (message.fixedHeader().isRetain() && !settings.retainEnabled) {
            return farewell(
                MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.RetainNotSupported).build(),
                getLocal(ProtocolViolation.class).statement("MQTT5-3.2.2-14").clientInfo(clientInfo));
        }
        if (message.fixedHeader().qosLevel().value() > settings.maxQoS.getNumber()) {
            return farewell(
                MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.QoSNotSupported).build(),
                getLocal(ProtocolViolation.class).statement("MQTT5-3.2.2-11").clientInfo(clientInfo));
        }
        String topic = message.variableHeader().topicName();
        MqttProperties mqttProperties = message.variableHeader().properties();
        if (messageExpiryInterval(mqttProperties).orElse(Integer.MAX_VALUE) == 0) {
            return farewell(MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                    .reasonString("MessageExpiryInterval must be positive integer").build(),
                getLocal(ProtocolViolation.class).statement("MessageExpiryInterval must be positive integer")
                    .clientInfo(clientInfo));
        }
        if (subscriptionIdentifier(mqttProperties).isPresent()) {
            return farewell(MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                    .reasonString("MQTT5-3.3.4-6").build(),
                getLocal(ProtocolViolation.class).statement("MQTT5-3.3.4-6").clientInfo(clientInfo));
        }
        // disconnect if malformed packet
        if (!isWellFormed(topic, SANITY_CHECK)) {
            return farewell(MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.MalformedPacket)
                    .reasonString("Malformed topic:" + topic).build(),
                getLocal(MalformedTopic.class).topic(topic).clientInfo(clientInfo));
        }
        if (!topic.isEmpty() && !TopicUtil.isValidTopic(topic, settings.maxTopicLevelLength, settings.maxTopicLevels,
            settings.maxTopicLength)) {
            switch (message.fixedHeader().qosLevel()) {
                case AT_MOST_ONCE -> {
                    return farewell(
                        MQTT5MessageBuilders
                            .disconnect()
                            .reasonCode(MQTT5DisconnectReasonCode.TopicNameInvalid)
                            .reasonString("Invalid topic:" + topic).build(),
                        getLocal(InvalidTopic.class).topic(topic).clientInfo(clientInfo));
                }
                case AT_LEAST_ONCE -> {
                    return response(
                        MQTT5MessageBuilders.pubAck(requestProblemInfo)
                            .packetId(message.variableHeader().packetId())
                            .reasonCode(MQTT5PubAckReasonCode.TopicNameInvalid)
                            .reasonString("Invalid topic:" + topic)
                            .build(),
                        getLocal(InvalidTopic.class).topic(topic).clientInfo(clientInfo));
                }
                case EXACTLY_ONCE -> {
                    return response(
                        MQTT5MessageBuilders.pubRec(requestProblemInfo)
                            .packetId(message.variableHeader().packetId())
                            .reasonCode(MQTT5PubRecReasonCode.TopicNameInvalid)
                            .reasonString("Invalid topic:" + topic)
                            .build(),
                        getLocal(InvalidTopic.class).topic(topic).clientInfo(clientInfo));
                }
                default -> {
                    return farewell(
                        MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.MalformedPacket)
                            .reasonString("Unexpected QoS").build(), getLocal(ProtocolViolation.class).statement(
                            "Unexpected QoS:" + message.fixedHeader().qosLevel()).clientInfo(clientInfo));
                }
            }
        }
        // disconnect if protocol error
        if (message.fixedHeader().qosLevel() == MqttQoS.AT_MOST_ONCE && message.fixedHeader().isDup()) {
            // ignore the QoS = 0 Dup = 1 messages according to [MQTT-3.3.1-2]
            return farewell(MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                    .reasonString("MQTT5-3.3.1-2").build(),
                getLocal(ProtocolViolation.class).statement("MQTT5-3.3.1-2").clientInfo(clientInfo));
        }
        Optional<String> responseTopic = responseTopic(mqttProperties);
        if (responseTopic.map(respTopic -> !isWellFormed(respTopic, SANITY_CHECK)).orElse(false)) {
            return farewell(MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.MalformedPacket)
                    .reasonString("Malformed response topic").build(),
                getLocal(ProtocolViolation.class).statement("MQTT5-3.3.2-13").clientInfo(clientInfo));
        }
        if (responseTopic.map(
            respTopic -> !isValidTopic(respTopic, settings.maxTopicLevelLength, settings.maxTopicLevels,
                settings.maxTopicLength)).orElse(false)) {
            switch (message.fixedHeader().qosLevel()) {
                case AT_MOST_ONCE -> {
                    return farewell(
                        MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.TopicNameInvalid)
                            .reasonString("Invalid response topic:" + responseTopic.get()).build(),
                        getLocal(ProtocolViolation.class).statement("MQTT5-3.3.2-14").clientInfo(clientInfo));
                }
                case AT_LEAST_ONCE -> {
                    return response(
                        MQTT5MessageBuilders.pubAck(requestProblemInfo)
                            .packetId(message.variableHeader().packetId())
                            .reasonCode(MQTT5PubAckReasonCode.TopicNameInvalid)
                            .reasonString("Invalid response topic:" + responseTopic.get())
                            .build(),
                        getLocal(ProtocolViolation.class).statement("MQTT5-3.3.2-14").clientInfo(clientInfo));
                }
                case EXACTLY_ONCE -> {
                    return response(
                        MQTT5MessageBuilders.pubRec(requestProblemInfo)
                            .packetId(message.variableHeader().packetId())
                            .reasonCode(MQTT5PubRecReasonCode.TopicNameInvalid)
                            .reasonString("Invalid response topic:" + responseTopic.get()).build(),
                        getLocal(ProtocolViolation.class).statement("MQTT5-3.3.2-14").clientInfo(clientInfo));
                }
                default -> {
                    return farewell(
                        MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.MalformedPacket)
                            .reasonString("Unexpected QoS").build(), getLocal(ProtocolViolation.class).statement(
                            "Unexpected QoS:" + message.fixedHeader().qosLevel()).clientInfo(clientInfo));
                }
            }
        }
        if (settings.payloadFormatValidationEnabled && isUTF8Payload(mqttProperties) &&
            !UTF8Util.isValidUTF8Payload(message.payload().nioBuffer())) {
            return switch (message.fixedHeader().qosLevel()) {
                case AT_MOST_ONCE -> farewell(
                    MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.PayloadFormatInvalid)
                        .reasonString("MQTT5-3.3.2.3.2").build(),
                    getLocal(ProtocolViolation.class).statement("MQTT5-3.3.2.3.2").clientInfo(clientInfo));
                case AT_LEAST_ONCE -> response(
                    MQTT5MessageBuilders.pubAck(requestProblemInfo)
                        .packetId(message.variableHeader().packetId())
                        .reasonCode(MQTT5PubAckReasonCode.PayloadFormatInvalid)
                        .build());
                case EXACTLY_ONCE -> response(
                    MQTT5MessageBuilders.pubRec(requestProblemInfo)
                        .packetId(message.variableHeader().packetId())
                        .reasonCode(MQTT5PubRecReasonCode.PayloadFormatInvalid)
                        .build());
                default -> farewell(
                    MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                        .reasonString("Invalid QoS").build(),
                    getLocal(ProtocolViolation.class).statement("Invalid QoS").clientInfo(clientInfo));
            };
        }
        // process topic alias
        Optional<Integer> topicAlias = topicAlias(mqttProperties);
        if (settings.maxTopicAlias == 0 && topicAlias.isPresent()) {
            return farewell(
                MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.TopicAliasInvalid).build(),
                getLocal(ProtocolViolation.class).statement("MQTT5-3.2.2-18").clientInfo(clientInfo));
        }
        if (settings.maxTopicAlias > 0 && topicAlias.orElse(0) > settings.maxTopicAlias) {
            return farewell(
                MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.TopicAliasInvalid).build(),
                getLocal(ProtocolViolation.class).statement("MQTT5-3.2.2-17").clientInfo(clientInfo));
        }
        // create or update alias
        if (topic.isEmpty()) {
            if (topicAlias.isPresent()) {
                Optional<String> aliasedTopic = receiverTopicAliasManager.getTopic(topicAlias.get());
                if (aliasedTopic.isEmpty()) {
                    return farewell(
                        MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                            .reasonString("MQTT5-3.3.4").build(),
                        getLocal(ProtocolViolation.class).statement("MQTT5-3.3.4").clientInfo(clientInfo));
                }
            } else {
                return farewell(MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                        .reasonString("MQTT5-3.3.4").build(),
                    getLocal(ProtocolViolation.class).statement("MQTT5-3.3.4").clientInfo(clientInfo));
            }
        } else {
            topicAlias.ifPresent(integer -> receiverTopicAliasManager.setAlias(topic, integer));
        }
        return null;
    }

    @Override
    public String getTopic(MqttPublishMessage message) {
        String topic = message.variableHeader().topicName();
        if (topic.isEmpty()) {
            MqttProperties pubMsgProperties = message.variableHeader().properties();
            // process topic alias
            Optional<Integer> topicAlias = topicAlias(pubMsgProperties);
            assert topicAlias.isPresent();
            Optional<String> aliasedTopic = receiverTopicAliasManager.getTopic(topicAlias.get());
            assert aliasedTopic.isPresent();
            topic = aliasedTopic.get();
        }
        return topic;
    }

    @Override
    public Message buildDistMessage(MqttPublishMessage message) {
        return MQTT5MessageUtils.toMessage(message);
    }

    @Override
    public ProtocolResponse onQoS0DistDenied(String topic, Message distMessage, CheckResult result) {
        assert !result.hasGranted();
        return switch (result.getTypeCase()) {
            case DENIED -> farewell(
                MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.NotAuthorized)
                    .reasonString(result.getDenied().hasReason() ? result.getDenied().getReason() : null)
                    .userProps(result.getDenied().getUserProps()).build(),
                getLocal(NoPubPermission.class).topic(topic).qos(QoS.AT_MOST_ONCE).retain(distMessage.getIsRetain())
                    .clientInfo(clientInfo));
            case ERROR -> farewell(
                MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ImplementationSpecificError)
                    .reasonString(result.getError().hasReason() ? result.getError().getReason() : null)
                    .userProps(result.getError().getUserProps()).build(),
                getLocal(NoPubPermission.class).topic(topic).qos(QoS.AT_MOST_ONCE).retain(distMessage.getIsRetain())
                    .clientInfo(clientInfo));
            default -> farewell(
                MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.UnspecifiedError).build(),
                getLocal(NoPubPermission.class).topic(topic).qos(QoS.AT_MOST_ONCE).retain(distMessage.getIsRetain())
                    .clientInfo(clientInfo));
        };
    }

    @Override
    public ProtocolResponse onQoS0PubHandled(PubResult result, MqttPublishMessage message, UserProperties userProps) {
        if (result.distResult() == org.apache.bifromq.dist.client.PubResult.BACK_PRESSURE_REJECTED ||
            result.retainResult() == RetainReply.Result.BACK_PRESSURE_REJECTED) {
            String reason =
                result.retainResult() == RetainReply.Result.BACK_PRESSURE_REJECTED ? "Too many retained qos0 publish" :
                    "Too many qos0 publish";
            return farewell(
                MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ServerBusy).reasonString(reason)
                    .userProps(userProps).build(), getLocal(ServerBusy.class).reason(reason).clientInfo(clientInfo));
        } else {
            return ProtocolResponse.responseNothing();
        }
    }

    @Override
    public ProtocolResponse onQoS1DistDenied(String topic, int packetId, Message distMessage, CheckResult result) {
        assert !result.hasGranted();
        return switch (result.getTypeCase()) {
            case DENIED -> response(MQTT5MessageBuilders.pubAck(requestProblemInfo)
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.NotAuthorized)
                .reasonString(result.getDenied().hasReason() ? result.getDenied().getReason() : null)
                .userProps(result.getDenied().getUserProps()).build());
            case ERROR -> response(MQTT5MessageBuilders.pubAck(requestProblemInfo)
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.ImplementationSpecificError)
                .reasonString(result.getError().hasReason() ? result.getError().getReason() : null)
                .userProps(result.getError().getUserProps()).build());
            default -> response(MQTT5MessageBuilders.pubAck(requestProblemInfo)
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.UnspecifiedError).build());
        };
    }

    @Override
    public ProtocolResponse respondQoS1PacketInUse(MqttPublishMessage message) {
        return response(MQTT5MessageBuilders.pubAck(requestProblemInfo)
                .packetId(message.variableHeader().packetId())
                .reasonCode(MQTT5PubAckReasonCode.PacketIdentifierInUse)
                .build(),
            getLocal(ProtocolViolation.class).statement("MQTT5-2.2.1-4").clientInfo(clientInfo));
    }

    @Override
    public ProtocolResponse onQoS1PubHandled(PubResult result, MqttPublishMessage message, UserProperties userProps) {
        if (result.distResult() == org.apache.bifromq.dist.client.PubResult.BACK_PRESSURE_REJECTED ||
            result.retainResult() == RetainReply.Result.BACK_PRESSURE_REJECTED) {
            String reason =
                result.retainResult() == RetainReply.Result.BACK_PRESSURE_REJECTED ? "Too many retained qos1 publish" :
                    "Too many qos1 publish";
            return farewell(
                MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ServerBusy).reasonString(reason)
                    .build(), getLocal(ServerBusy.class).reason(reason).clientInfo(clientInfo));
        }
        int packetId = message.variableHeader().packetId();
        Event<?>[] debugEvents;
        if (settings.debugMode) {
            debugEvents = new Event<?>[] {
                getLocal(QoS1PubAcked.class).reqId(packetId).isDup(message.fixedHeader().isDup())
                    .topic(message.variableHeader().topicName()).size(message.payload().readableBytes()).clientInfo(
                    clientInfo)};
        } else {
            debugEvents = new Event[0];
        }
        if (result.retainResult() == RetainReply.Result.EXCEED_LIMIT) {
            return response(MQTT5MessageBuilders.pubAck(requestProblemInfo)
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.QuotaExceeded).reasonString("Retain resource throttled")
                .userProps(userProps).build(), debugEvents);
        }
        return switch (result.distResult()) {
            case OK -> response(MQTT5MessageBuilders.pubAck(requestProblemInfo)
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.Success).userProps(userProps).build(), debugEvents);
            case NO_MATCH -> response(MQTT5MessageBuilders.pubAck(requestProblemInfo)
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.NoMatchingSubscribers)
                .userProps(userProps).build(), debugEvents);
            case TRY_LATER -> response(MQTT5MessageBuilders.pubAck(requestProblemInfo)
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.ImplementationSpecificError)
                .reasonString(result.distResult().name())
                .userProps(userProps).build(), debugEvents);
            default -> response(MQTT5MessageBuilders.pubAck(requestProblemInfo)
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.UnspecifiedError)
                .userProps(userProps).build(), debugEvents);
        };
    }

    @Override
    public ProtocolResponse respondQoS2PacketInUse(MqttPublishMessage message) {
        return response(MQTT5MessageBuilders.pubRec(requestProblemInfo)
                .packetId(message.variableHeader().packetId())
                .reasonCode(MQTT5PubRecReasonCode.PacketIdentifierInUse)
                .build(),
            getLocal(ProtocolViolation.class).statement("MQTT3-2.2.1-4").clientInfo(clientInfo));
    }

    @Override
    public ProtocolResponse onQoS2DistDenied(String topic, int packetId, Message distMessage, CheckResult result) {
        assert !result.hasGranted();
        return switch (result.getTypeCase()) {
            case DENIED -> response(MQTT5MessageBuilders.pubRec(requestProblemInfo).packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.NotAuthorized)
                .reasonString(result.getDenied().hasReason() ? result.getDenied().getReason() : null)
                .userProps(result.getDenied().getUserProps()).build());
            case ERROR -> response(MQTT5MessageBuilders.pubRec(requestProblemInfo).packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.ImplementationSpecificError)
                .reasonString(result.getError().hasReason() ? result.getError().getReason() : null)
                .userProps(result.getError().getUserProps()).build());
            default -> response(MQTT5MessageBuilders.pubRec(requestProblemInfo).packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.UnspecifiedError).build());
        };
    }

    @Override
    public ProtocolResponse onQoS2PubHandled(PubResult result, MqttPublishMessage message, UserProperties userProps) {
        if (result.distResult() == org.apache.bifromq.dist.client.PubResult.BACK_PRESSURE_REJECTED ||
            result.retainResult() == RetainReply.Result.BACK_PRESSURE_REJECTED) {
            String reason =
                result.retainResult() == RetainReply.Result.BACK_PRESSURE_REJECTED ? "Too many retained qos2 publish" :
                    "Too many qos2 publish";
            return farewell(
                MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.ServerBusy).reasonString(reason)
                    .build(), getLocal(ServerBusy.class).reason(reason).clientInfo(clientInfo));
        }
        int packetId = message.variableHeader().packetId();
        Event<?>[] debugEvents;
        if (settings.debugMode) {
            debugEvents = new Event<?>[] {
                getLocal(QoS2PubReced.class).reqId(packetId).isDup(message.fixedHeader().isDup())
                    .topic(message.variableHeader().topicName()).size(message.payload().readableBytes()).clientInfo(
                    clientInfo)};
        } else {
            debugEvents = new Event[0];
        }
        if (result.retainResult() == RetainReply.Result.EXCEED_LIMIT) {
            return response(MQTT5MessageBuilders.pubRec(requestProblemInfo).packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.QuotaExceeded).reasonString("Retain resource throttled")
                .userProps(userProps).build(), debugEvents);
        }
        return switch (result.distResult()) {
            case OK -> response(MQTT5MessageBuilders.pubRec(requestProblemInfo).packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.Success).userProps(userProps).build(), debugEvents);
            case NO_MATCH -> response(MQTT5MessageBuilders.pubRec(requestProblemInfo).packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.NoMatchingSubscribers).userProps(userProps).build(), debugEvents);
            case TRY_LATER -> response(MQTT5MessageBuilders.pubRec(requestProblemInfo).packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.ImplementationSpecificError).reasonString(result.distResult().name())
                .userProps(userProps).build(), debugEvents);
            default -> response(MQTT5MessageBuilders.pubRec(requestProblemInfo).packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.UnspecifiedError).userProps(userProps).build(), debugEvents);
        };
    }

    @Override
    public ProtocolResponse onIdleTimeout(int keepAliveTimeSeconds) {
        return farewellNow(
            MQTT5MessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.KeepAliveTimeout).build(),
            getLocal(Idle.class).keepAliveTimeSeconds(keepAliveTimeSeconds).clientInfo(clientInfo));
    }
}
