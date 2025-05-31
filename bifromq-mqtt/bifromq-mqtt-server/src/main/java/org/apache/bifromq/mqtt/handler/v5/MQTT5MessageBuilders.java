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

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.storage.proto.TopicFilterOption;
import org.apache.bifromq.mqtt.handler.MQTTSessionHandler;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5AuthReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5DisconnectReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5PubAckReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5PubCompReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5PubRecReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5PubRelReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5SubAckReasonCode;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5UnsubAckReasonCode;
import org.apache.bifromq.type.UserProperties;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckPayload;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class MQTT5MessageBuilders {
    public static AuthBuilder auth(String authMethod) {
        return new AuthBuilder(authMethod);
    }

    public static ConnAckPropertiesBuilder connAckProperties() {
        return new ConnAckPropertiesBuilder();
    }

    public static DisconnectBuilder disconnect() {
        return new DisconnectBuilder();
    }

    public static SubAckBuilder subAck() {
        return new SubAckBuilder();
    }

    public static UnsubAckBuilder unsubAck() {
        return new UnsubAckBuilder();
    }

    public static PubBuilder pub() {
        return new PubBuilder();
    }

    public static PubAckBuilder pubAck(boolean includeProblemInfo) {
        return new PubAckBuilder(includeProblemInfo);
    }

    public static PubRecBuilder pubRec(boolean includeProblemInfo) {
        return new PubRecBuilder(includeProblemInfo);
    }

    public static PubRelBuilder pubRel(boolean includeProblemInfo) {
        return new PubRelBuilder(includeProblemInfo);
    }

    public static PubCompBuilder pubComp(boolean includeProblemInfo) {
        return new PubCompBuilder(includeProblemInfo);
    }

    public static final class AuthBuilder {
        private final String authMethod;
        private ByteString authData;
        private MQTT5AuthReasonCode reasonCode;
        private String reasonString;
        private UserProperties userProps;

        public AuthBuilder(String authMethod) {
            this.authMethod = authMethod;
        }

        public AuthBuilder authData(ByteString authData) {
            this.authData = authData;
            return this;
        }

        public AuthBuilder reasonCode(MQTT5AuthReasonCode reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public AuthBuilder reasonString(String reasonString) {
            this.reasonString = reasonString;
            return this;
        }

        public AuthBuilder userProperties(UserProperties userProps) {
            this.userProps = userProps;
            return this;
        }

        public MqttMessage build() {
            MQTT5MessageUtils.MqttPropertiesBuilder propsBuilder = MQTT5MessageUtils.mqttProps();
            propsBuilder.addAuthMethod(authMethod);
            if (authData != null) {
                propsBuilder.addAuthData(authData);
            }
            if (!Strings.isNullOrEmpty(reasonString)) {
                propsBuilder.addReasonString(reasonString);
            }
            if (userProps != null) {
                propsBuilder.addUserProperties(userProps);
            }
            MqttProperties mqttProperties = propsBuilder.build();
            MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.AUTH,
                false,
                MqttQoS.AT_MOST_ONCE,
                false,
                0);
            if (mqttProperties.isEmpty() && reasonCode == MQTT5AuthReasonCode.Success) {
                // The Reason Code and Property Length can be omitted if the Reason Code is 0x00 (Success) and there are no Properties. In this case the AUTH has a Remaining Length of 0
                return new MqttMessage(fixedHeader);
            }
            MqttReasonCodeAndPropertiesVariableHeader variableHeader =
                new MqttReasonCodeAndPropertiesVariableHeader(reasonCode.value(), mqttProperties);
            return new MqttMessage(fixedHeader, variableHeader);
        }
    }

    public static final class ConnAckPropertiesBuilder {
        private final MqttProperties.UserProperties userProperties = new MqttProperties.UserProperties();
        private String clientId;
        private Long sessionExpiryInterval;
        private Integer receiveMaximum;
        private Byte maximumQos;
        private Boolean retain;
        private Long maximumPacketSize;
        private Integer topicAliasMaximum;
        private String reasonString;
        private Boolean wildcardSubscriptionAvailable;
        private Boolean subscriptionIdentifiersAvailable;
        private Boolean sharedSubscriptionAvailable;
        private Integer serverKeepAlive;
        private String responseInformation;
        private String serverReference;
        private String authenticationMethod;
        private byte[] authenticationData;

        public MqttProperties build() {
            final MqttProperties props = new MqttProperties();
            if (clientId != null) {
                props.add(new MqttProperties.StringProperty(
                    MqttProperties.MqttPropertyType.ASSIGNED_CLIENT_IDENTIFIER.value(),
                    clientId));
            }
            if (sessionExpiryInterval != null) {
                props.add(new MqttProperties.IntegerProperty(
                    MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value(), sessionExpiryInterval.intValue()));
            }
            if (receiveMaximum != null) {
                props.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value(),
                    receiveMaximum));
            }
            if (maximumQos != null) {
                props.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.MAXIMUM_QOS.value(),
                    receiveMaximum));
            }
            if (maximumPacketSize != null) {
                props.add(
                    new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.MAXIMUM_PACKET_SIZE.value(),
                        maximumPacketSize.intValue()));
            }
            if (topicAliasMaximum != null) {
                props.add(
                    new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.TOPIC_ALIAS_MAXIMUM.value(),
                        topicAliasMaximum));
            }
            if (reasonString != null) {
                props.add(new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                    reasonString));
            }
            if (!userProperties.value().isEmpty()) {
                props.add(userProperties);
            }
            if (retain != null) {
                props.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.RETAIN_AVAILABLE.value(),
                    retain ? 1 : 0));
            }
            if (wildcardSubscriptionAvailable != null) {
                props.add(new MqttProperties.IntegerProperty(
                    MqttProperties.MqttPropertyType.WILDCARD_SUBSCRIPTION_AVAILABLE.value(),
                    wildcardSubscriptionAvailable ? 1 : 0));
            }
            if (subscriptionIdentifiersAvailable != null) {
                props.add(new MqttProperties.IntegerProperty(
                    MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER_AVAILABLE.value(),
                    subscriptionIdentifiersAvailable ? 1 : 0));
            }
            if (sharedSubscriptionAvailable != null) {
                props.add(new MqttProperties.IntegerProperty(
                    MqttProperties.MqttPropertyType.SHARED_SUBSCRIPTION_AVAILABLE.value(),
                    sharedSubscriptionAvailable ? 1 : 0));
            }
            if (serverKeepAlive != null) {
                props.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.SERVER_KEEP_ALIVE.value(),
                    serverKeepAlive));
            }
            if (responseInformation != null) {
                props.add(
                    new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.RESPONSE_INFORMATION.value(),
                        responseInformation));
            }
            if (serverReference != null) {
                props.add(new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.SERVER_REFERENCE.value(),
                    serverReference));
            }
            if (authenticationMethod != null) {
                props.add(
                    new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.AUTHENTICATION_METHOD.value(),
                        authenticationMethod));
            }
            if (authenticationData != null) {
                props.add(new MqttProperties.BinaryProperty(MqttProperties.MqttPropertyType.AUTHENTICATION_DATA.value(),
                    authenticationData));
            }

            return props;
        }

        public ConnAckPropertiesBuilder sessionExpiryInterval(long seconds) {
            this.sessionExpiryInterval = seconds;
            return this;
        }

        public ConnAckPropertiesBuilder receiveMaximum(int value) {
            this.receiveMaximum = value;
            return this;
        }

        public ConnAckPropertiesBuilder maximumQos(byte value) {
            if (value != 0 && value != 1) {
                throw new IllegalArgumentException("maximum QoS property could be 0 or 1");
            }
            this.maximumQos = value;
            return this;
        }

        public ConnAckPropertiesBuilder retainAvailable(boolean retain) {
            this.retain = retain;
            return this;
        }

        public ConnAckPropertiesBuilder maximumPacketSize(long size) {
            this.maximumPacketSize = size;
            return this;
        }

        public ConnAckPropertiesBuilder assignedClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public ConnAckPropertiesBuilder topicAliasMaximum(int value) {
            this.topicAliasMaximum = value;
            return this;
        }

        public ConnAckPropertiesBuilder reasonString(String reason) {
            this.reasonString = reason;
            return this;
        }

        public ConnAckPropertiesBuilder userProperty(String name, String value) {
            userProperties.add(name, value);
            return this;
        }

        public ConnAckPropertiesBuilder wildcardSubscriptionAvailable(boolean value) {
            this.wildcardSubscriptionAvailable = value;
            return this;
        }

        public ConnAckPropertiesBuilder subscriptionIdentifiersAvailable(boolean value) {
            this.subscriptionIdentifiersAvailable = value;
            return this;
        }

        public ConnAckPropertiesBuilder sharedSubscriptionAvailable(boolean value) {
            this.sharedSubscriptionAvailable = value;
            return this;
        }

        public ConnAckPropertiesBuilder serverKeepAlive(int seconds) {
            this.serverKeepAlive = seconds;
            return this;
        }

        public ConnAckPropertiesBuilder responseInformation(String value) {
            this.responseInformation = value;
            return this;
        }

        public ConnAckPropertiesBuilder serverReference(String host) {
            this.serverReference = host;
            return this;
        }

        public ConnAckPropertiesBuilder authenticationMethod(String methodName) {
            this.authenticationMethod = methodName;
            return this;
        }

        public ConnAckPropertiesBuilder authenticationData(byte[] rawData) {
            this.authenticationData = rawData.clone();
            return this;
        }
    }

    public static final class PubBuilder {
        private int packetId;
        private MQTTSessionHandler.SubMessage message;
        private boolean dup;
        private boolean setupAlias;
        private int topicAlias;

        public PubBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public PubBuilder dup(boolean dup) {
            this.dup = dup;
            return this;
        }

        public PubBuilder setupAlias(boolean setupAlias) {
            this.setupAlias = setupAlias;
            return this;
        }

        public PubBuilder topicAlias(int alias) {
            this.topicAlias = alias;
            return this;
        }

        public PubBuilder message(MQTTSessionHandler.SubMessage message) {
            this.message = message;
            return this;
        }

        public MqttPublishMessage build() {
            MQTT5MessageUtils.MqttPropertiesBuilder propsBuilder = MQTT5MessageUtils.mqttProps();
            TopicFilterOption option = message.option();
            if (option.hasSubId()) {
                propsBuilder.addSubscriptionIdentifier(option.getSubId());
            }
            String topicName;
            if (topicAlias > 0) {
                if (setupAlias) {
                    topicName = message.topic();
                } else {
                    topicName = "";
                }
                propsBuilder.addTopicAlias(topicAlias);
            } else {
                topicName = message.topic();
            }
            if (message.message().getIsUTF8String()) {
                propsBuilder.addPayloadFormatIndicator(1);
            }
            if (message.message().hasContentType()) {
                propsBuilder.addContentType(message.message().getContentType());
            }
            if (message.message().hasCorrelationData()) {
                propsBuilder.addCorrelationData(message.message().getCorrelationData());
            }
            if (message.message().hasResponseTopic()) {
                propsBuilder.addResponseTopic(message.message().getResponseTopic());
            }
            if (message.message().getUserProperties().getUserPropertiesCount() > 0) {
                propsBuilder.addUserProperties(message.message().getUserProperties());
            }
            if (message.message().getExpiryInterval() < Integer.MAX_VALUE) {
                // If absent, the Application Message does not expire
                int leftDelayInterval = (int) Duration.ofMillis(
                        Duration.ofSeconds(message.message().getExpiryInterval()).toMillis()
                            - (HLC.INST.getPhysical() - HLC.INST.getPhysical(message.message().getTimestamp())))
                    .toSeconds();
                propsBuilder.addMessageExpiryInterval(leftDelayInterval);
            }

            MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBLISH, dup, MqttQoS.valueOf(message.qos().getNumber()),
                    message.isRetain(), 0);
            MqttPublishVariableHeader mqttVariableHeader =
                new MqttPublishVariableHeader(topicName, packetId, propsBuilder.build());
            return new MqttPublishMessage(mqttFixedHeader, mqttVariableHeader,
                Unpooled.wrappedBuffer(message.message().getPayload().asReadOnlyByteBuffer()));
        }
    }

    public static final class PubAckBuilder {

        private final boolean includeProblemInfo;
        private int packetId;
        private MQTT5PubAckReasonCode reasonCode;
        private String reasonString;
        private UserProperties userProps;

        PubAckBuilder(boolean includeProblemInfo) {
            this.includeProblemInfo = includeProblemInfo;
        }

        public PubAckBuilder reasonCode(MQTT5PubAckReasonCode reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public PubAckBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public PubAckBuilder reasonString(String reason) {
            this.reasonString = reason;
            return this;
        }

        public PubAckBuilder userProps(UserProperties userProps) {
            this.userProps = userProps;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
            MqttMessageIdVariableHeader varHeader;
            if ((!includeProblemInfo || (Strings.isNullOrEmpty(reasonString) && userProps == null))
                && reasonCode == MQTT5PubAckReasonCode.Success) {
                varHeader = MqttMessageIdVariableHeader.from(packetId);
            } else {
                MQTT5MessageUtils.MqttPropertiesBuilder propsBuilder = MQTT5MessageUtils.mqttProps();
                if (includeProblemInfo && !Strings.isNullOrEmpty(reasonString)) {
                    propsBuilder.addReasonString(reasonString);
                }
                if (includeProblemInfo && userProps != null) {
                    propsBuilder.addUserProperties(userProps);
                }
                varHeader = new MqttPubReplyMessageVariableHeader(packetId, reasonCode.value(), propsBuilder.build());
            }
            return new MqttMessage(fixedHeader, varHeader);
        }
    }

    public static final class PubRecBuilder {
        private final boolean includeProblemInfo;

        private int packetId;
        private MQTT5PubRecReasonCode reasonCode;
        private String reasonString;
        private UserProperties userProps;

        PubRecBuilder(boolean includeProblemInfo) {
            this.includeProblemInfo = includeProblemInfo;
        }

        public PubRecBuilder reasonCode(MQTT5PubRecReasonCode reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public PubRecBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public PubRecBuilder reasonString(String reason) {
            this.reasonString = reason;
            return this;
        }

        public PubRecBuilder userProps(UserProperties userProps) {
            this.userProps = userProps;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 2);
            MqttMessageIdVariableHeader varHeader;
            if ((!includeProblemInfo || Strings.isNullOrEmpty(reasonString) && userProps == null)
                && reasonCode == MQTT5PubRecReasonCode.Success) {
                varHeader = MqttMessageIdVariableHeader.from(packetId);
            } else {
                MQTT5MessageUtils.MqttPropertiesBuilder propsBuilder = MQTT5MessageUtils.mqttProps();
                if (includeProblemInfo && !Strings.isNullOrEmpty(reasonString)) {
                    propsBuilder.addReasonString(reasonString);
                }
                if (includeProblemInfo && userProps != null) {
                    propsBuilder.addUserProperties(userProps);
                }
                varHeader = new MqttPubReplyMessageVariableHeader(packetId, reasonCode.value(), propsBuilder.build());
            }
            return new MqttMessage(fixedHeader, varHeader);
        }
    }

    public static final class PubRelBuilder {
        private final boolean includeProblemInfo;
        private int packetId;
        private MQTT5PubRelReasonCode reasonCode;
        private String reasonString;
        private UserProperties userProps;

        PubRelBuilder(boolean includeProblemInfo) {
            this.includeProblemInfo = includeProblemInfo;
        }

        public PubRelBuilder reasonCode(MQTT5PubRelReasonCode reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public PubRelBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public PubRelBuilder reasonString(String reason) {
            this.reasonString = reason;
            return this;
        }

        public PubRelBuilder userProps(UserProperties userProps) {
            this.userProps = userProps;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 2);
            MqttMessageIdVariableHeader varHeader;
            if ((!includeProblemInfo || Strings.isNullOrEmpty(reasonString) && userProps == null)
                && reasonCode == MQTT5PubRelReasonCode.Success) {
                varHeader = MqttMessageIdVariableHeader.from(packetId);
            } else {
                MQTT5MessageUtils.MqttPropertiesBuilder propsBuilder = MQTT5MessageUtils.mqttProps();
                if (includeProblemInfo && !Strings.isNullOrEmpty(reasonString)) {
                    propsBuilder.addReasonString(reasonString);
                }
                if (includeProblemInfo && userProps != null) {
                    propsBuilder.addUserProperties(userProps);
                }
                varHeader = new MqttPubReplyMessageVariableHeader(packetId, reasonCode.value(), propsBuilder.build());
            }
            return new MqttMessage(fixedHeader, varHeader);
        }
    }

    public static final class PubCompBuilder {
        private final boolean includeProblemInfo;
        private int packetId;
        private MQTT5PubCompReasonCode reasonCode;
        private String reasonString;
        private UserProperties userProps;

        PubCompBuilder(boolean includeProblemInfo) {
            this.includeProblemInfo = includeProblemInfo;
        }

        public PubCompBuilder reasonCode(MQTT5PubCompReasonCode reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public PubCompBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public PubCompBuilder reasonString(String reason) {
            this.reasonString = reason;
            return this;
        }

        public PubCompBuilder userProps(UserProperties userProps) {
            this.userProps = userProps;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 2);
            MqttMessageIdVariableHeader varHeader;
            if ((!includeProblemInfo || Strings.isNullOrEmpty(reasonString) && userProps == null)
                && reasonCode == MQTT5PubCompReasonCode.Success) {
                varHeader = MqttMessageIdVariableHeader.from(packetId);
            } else {
                MQTT5MessageUtils.MqttPropertiesBuilder propsBuilder = MQTT5MessageUtils.mqttProps();
                if (includeProblemInfo && !Strings.isNullOrEmpty(reasonString)) {
                    propsBuilder.addReasonString(reasonString);
                }
                if (includeProblemInfo && userProps != null) {
                    propsBuilder.addUserProperties(userProps);
                }
                varHeader = new MqttPubReplyMessageVariableHeader(packetId, reasonCode.value(), propsBuilder.build());
            }
            return new MqttMessage(fixedHeader, varHeader);
        }
    }

    public static final class SubAckBuilder {
        private int packetId;
        private List<MQTT5SubAckReasonCode> reasonCodes;
        private String reasonString;
        private UserProperties userProps;

        SubAckBuilder() {

        }

        public SubAckBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public SubAckBuilder reasonCodes(MQTT5SubAckReasonCode... reasonCodes) {
            this.reasonCodes = List.of(reasonCodes);
            return this;
        }

        public SubAckBuilder reasonString(String reason) {
            this.reasonString = reason;
            return this;
        }

        public SubAckBuilder userProps(UserProperties userProps) {
            this.userProps = userProps;
            return this;
        }

        public MqttSubAckMessage build() {
            MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK,
                false,
                MqttQoS.AT_MOST_ONCE,
                false,
                0);
            MqttMessageIdVariableHeader variableHeader;
            if (!Strings.isNullOrEmpty(reasonString) || userProps != null) {
                MQTT5MessageUtils.MqttPropertiesBuilder propsBuilder = MQTT5MessageUtils.mqttProps();
                if (!Strings.isNullOrEmpty(reasonString)) {
                    propsBuilder.addReasonString(reasonString);
                }
                if (userProps != null) {
                    propsBuilder.addUserProperties(userProps);
                }
                variableHeader = new MqttMessageIdAndPropertiesVariableHeader(packetId, propsBuilder.build());
            } else {
                variableHeader = MqttMessageIdVariableHeader.from(packetId);
            }
            MqttSubAckPayload mqttSubAckPayload =
                new MqttSubAckPayload(Lists.transform(reasonCodes, MQTT5SubAckReasonCode::value));
            return new MqttSubAckMessage(mqttFixedHeader, variableHeader, mqttSubAckPayload);
        }
    }

    public static final class DisconnectBuilder {
        private MQTT5DisconnectReasonCode reasonCode;
        private String reasonString;
        private UserProperties userProps;
        private String serverReference;

        DisconnectBuilder() {
        }

        public DisconnectBuilder reasonCode(MQTT5DisconnectReasonCode reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public DisconnectBuilder reasonString(String reason) {
            this.reasonString = reason;
            return this;
        }

        public DisconnectBuilder userProps(UserProperties userProps) {
            this.userProps = userProps;
            return this;
        }

        public DisconnectBuilder serverReference(String serverReference) {
            this.serverReference = serverReference;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
            MQTT5MessageUtils.MqttPropertiesBuilder propsBuilder = MQTT5MessageUtils.mqttProps();
            if (!Strings.isNullOrEmpty(reasonString) || userProps != null || !Strings.isNullOrEmpty(serverReference)) {
                if (!Strings.isNullOrEmpty(reasonString)) {
                    propsBuilder.addReasonString(reasonString);
                }
                if (!Strings.isNullOrEmpty(serverReference)) {
                    propsBuilder.addServerReference(serverReference);
                }
                if (userProps != null) {
                    propsBuilder.addUserProperties(userProps);
                }
            }
            MqttProperties mqttProperties = propsBuilder.build();
            if (reasonCode == MQTT5DisconnectReasonCode.Normal && mqttProperties.isEmpty()) {
                // The Reason Code and Property Length can be omitted if the Reason Code is 0x00 (Normal disconnection)
                // and there are no Properties
                return new MqttMessage(fixedHeader);
            }
            MqttReasonCodeAndPropertiesVariableHeader variableHeader =
                new MqttReasonCodeAndPropertiesVariableHeader(reasonCode.value(), mqttProperties);
            return new MqttMessage(fixedHeader, variableHeader);
        }
    }

    public static final class UnsubAckBuilder {
        private final List<MQTT5UnsubAckReasonCode> reasonCodes = new ArrayList<>();
        private int packetId;
        private String reasonString;
        private UserProperties userProps;

        UnsubAckBuilder() {
        }

        public UnsubAckBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public UnsubAckBuilder addReasonCode(MQTT5UnsubAckReasonCode reasonCode) {
            this.reasonCodes.add(reasonCode);
            return this;
        }

        public UnsubAckBuilder addReasonCodes(MQTT5UnsubAckReasonCode... reasonCodes) {
            this.reasonCodes.addAll(List.of(reasonCodes));
            return this;
        }

        public UnsubAckBuilder reasonString(String reason) {
            this.reasonString = reason;
            return this;
        }

        public UnsubAckBuilder userProps(UserProperties userProps) {
            this.userProps = userProps;
            return this;
        }

        public MqttUnsubAckMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
            MqttMessageIdVariableHeader variableHeader;
            if (!Strings.isNullOrEmpty(reasonString) || userProps != null) {
                MQTT5MessageUtils.MqttPropertiesBuilder propsBuilder = MQTT5MessageUtils.mqttProps();
                if (!Strings.isNullOrEmpty(reasonString)) {
                    propsBuilder.addReasonString(reasonString);
                }
                if (userProps != null) {
                    propsBuilder.addUserProperties(userProps);
                }
                variableHeader = new MqttMessageIdAndPropertiesVariableHeader(packetId, propsBuilder.build());
            } else {
                variableHeader = MqttMessageIdVariableHeader.from(packetId);
            }
            MqttUnsubAckPayload unsubAckPayload =
                new MqttUnsubAckPayload(Lists.transform(reasonCodes, MQTT5UnsubAckReasonCode::value));
            return new MqttUnsubAckMessage(fixedHeader, variableHeader, unsubAckPayload);
        }
    }
}
