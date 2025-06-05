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

package org.apache.bifromq.mqtt.handler;

import static org.apache.bifromq.metrics.TenantMetric.MqttChannelLatency;
import static org.apache.bifromq.metrics.TenantMetric.MqttEgressBytes;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS0EgressBytes;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS1EgressBytes;
import static org.apache.bifromq.metrics.TenantMetric.MqttQoS2EgressBytes;
import static org.apache.bifromq.mqtt.utils.MQTTMessageTrimmer.trim;
import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_5_VALUE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;

import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.mqtt.utils.IMQTTMessageSizer;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.OversizePacketDropped;
import org.apache.bifromq.type.ClientInfo;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The packet filter is a duplex handler, MUST be configured after MqttEncoder and before MqttDecoder
 */
public class MQTTPacketFilter extends ChannelOutboundHandlerAdapter {
    public static final String NAME = "MQTT5SizeBasedPacketFilter";
    private final ClientInfo clientInfo;
    private final IEventCollector eventCollector;
    private final ITenantMeter tenantMeter;
    private final int maxPacketSize;
    private final boolean enableTrim;
    private final IMQTTMessageSizer sizer;

    public MQTTPacketFilter(int maxPacketSize,
                            TenantSettings settings,
                            ClientInfo clientInfo,
                            IEventCollector eventCollector) {
        this.eventCollector = eventCollector;
        tenantMeter = ITenantMeter.get(clientInfo.getTenantId());
        this.clientInfo = clientInfo;
        this.maxPacketSize = Math.min(maxPacketSize, settings.maxPacketSize);
        this.enableTrim = clientInfo.getMetadataOrDefault(MQTT_PROTOCOL_VER_KEY, "").equals(MQTT_PROTOCOL_VER_5_VALUE);
        this.sizer = enableTrim ? IMQTTMessageSizer.mqtt5() : IMQTTMessageSizer.mqtt3();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        assert msg instanceof MqttMessage;
        MqttMessage mqttMessage = (MqttMessage) msg;
        IMQTTMessageSizer.MqttMessageSize messageSize = sizer.sizeOf(mqttMessage);
        AtomicInteger encodedBytes = new AtomicInteger(messageSize.encodedBytes());
        if (encodedBytes.get() <= maxPacketSize) {
            promise.addListener(logMetric(mqttMessage, encodedBytes.get()));
            super.write(ctx, msg, promise);
            return;
        }
        if (enableTrim && isTrimable(mqttMessage)) {
            encodedBytes.set(messageSize.encodedBytes(true, false));
            if (encodedBytes.get() <= maxPacketSize) {
                // trim reason string
                promise.addListener(logMetric(mqttMessage, encodedBytes.get()));
                super.write(ctx, trim(mqttMessage, true, false), promise);
                return;
            }
            encodedBytes.set(messageSize.encodedBytes(false, false));
            if (encodedBytes.get() <= maxPacketSize) {
                // trim reason string and user properties
                promise.addListener(logMetric(mqttMessage, encodedBytes.get()));
                super.write(ctx, trim(mqttMessage, true, true), promise);
                return;
            }
        }
        eventCollector.report(
            getLocal(OversizePacketDropped.class)
                .mqttPacketType(mqttMessage.fixedHeader().messageType().value())
                .clientInfo(clientInfo));
    }

    private GenericFutureListener<? extends Future<? super Void>> logMetric(MqttMessage message, int size) {
        Timer.Sample start = Timer.start();
        return future -> {
            if (future.isSuccess()) {
                if (Objects.requireNonNull(message.fixedHeader().messageType()) == MqttMessageType.PUBLISH) {
                    switch (message.fixedHeader().qosLevel()) {
                        case AT_MOST_ONCE -> tenantMeter.recordSummary(MqttQoS0EgressBytes, size);
                        case AT_LEAST_ONCE -> tenantMeter.recordSummary(MqttQoS1EgressBytes, size);
                        case EXACTLY_ONCE -> tenantMeter.recordSummary(MqttQoS2EgressBytes, size);
                    }
                }
                tenantMeter.recordSummary(MqttEgressBytes, size);
                start.stop(tenantMeter.timer(MqttChannelLatency));
            }
        };
    }

    private boolean isTrimable(MqttMessage message) {
        return switch (message.fixedHeader().messageType()) {
            case PUBLISH, CONNACK, DISCONNECT -> false;
            default -> true;
        };
    }
}
