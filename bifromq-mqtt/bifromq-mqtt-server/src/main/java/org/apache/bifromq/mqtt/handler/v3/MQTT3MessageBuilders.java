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

import org.apache.bifromq.type.QoS;
import com.google.protobuf.ByteString;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;

public class MQTT3MessageBuilders {
    public static PublishBuilder pub() {
        return new PublishBuilder();
    }

    public static PubRecBuilder pubRec() {
        return new PubRecBuilder();
    }

    public static PubRelBuilder pubRel() {
        return new PubRelBuilder();
    }

    public static PubCompBuilder pubComp() {
        return new PubCompBuilder();
    }

    public static final class PublishBuilder {
        private String topic;
        private boolean dup;
        private boolean retained;
        private QoS qos;
        private ByteString payload;
        private int messageId;

        PublishBuilder() {
        }

        public PublishBuilder topicName(String topic) {
            this.topic = topic;
            return this;
        }

        public PublishBuilder retained(boolean retained) {
            this.retained = retained;
            return this;
        }

        public PublishBuilder dup(boolean dup) {
            this.dup = dup;
            return this;
        }

        public PublishBuilder qos(QoS qos) {
            this.qos = qos;
            return this;
        }

        public PublishBuilder payload(ByteString payload) {
            this.payload = payload;
            return this;
        }

        public PublishBuilder messageId(int messageId) {
            this.messageId = messageId;
            return this;
        }

        public MqttPublishMessage build() {
            MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBLISH, dup, MqttQoS.valueOf(qos.getNumber()), retained, 0);
            MqttPublishVariableHeader mqttVariableHeader =
                new MqttPublishVariableHeader(topic, messageId, null);
            return new MqttPublishMessage(mqttFixedHeader, mqttVariableHeader,
                Unpooled.wrappedBuffer(payload.asReadOnlyByteBuffer()));
        }
    }

    public static final class PubRecBuilder {
        private int packetId;

        PubRecBuilder() {
        }

        public PubRecBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 2);
            MqttMessageIdVariableHeader varHeader = MqttMessageIdVariableHeader.from(packetId);
            return new MqttMessage(fixedHeader, varHeader);
        }
    }

    public static final class PubRelBuilder {
        private int packetId;

        PubRelBuilder() {
        }

        public PubRelBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 2);
            MqttMessageIdVariableHeader varHeader = MqttMessageIdVariableHeader.from(packetId);
            return new MqttMessage(fixedHeader, varHeader);
        }
    }

    public static final class PubCompBuilder {
        private int packetId;

        PubCompBuilder() {
        }

        public PubCompBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 2);
            MqttMessageIdVariableHeader varHeader = MqttMessageIdVariableHeader.from(packetId);
            return new MqttMessage(fixedHeader, varHeader);
        }
    }

}
