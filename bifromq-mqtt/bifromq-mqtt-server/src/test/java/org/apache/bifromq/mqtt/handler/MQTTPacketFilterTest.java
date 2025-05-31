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

package org.apache.bifromq.mqtt.handler;

import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_3_1_1_VALUE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_5_VALUE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.metrics.TenantMetric;
import org.apache.bifromq.mqtt.MockableTest;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.OversizePacketDropped;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.type.ClientInfo;
import io.micrometer.core.instrument.Timer;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

public class MQTTPacketFilterTest extends MockableTest {
    private String tenantId = "tenantA";
    @Mock
    private IEventCollector eventCollector;
    @Mock
    private ITenantMeter tenantMeter;
    @Mock
    private Timer timer;
    private ISettingProvider settingProvider = Setting::current;
    private TenantSettings settings = new TenantSettings(tenantId, settingProvider);
    private ClientInfo mqtt3Client = ClientInfo.newBuilder()
        .setTenantId(tenantId)
        .putMetadata(MQTT_PROTOCOL_VER_KEY, MQTT_PROTOCOL_VER_3_1_1_VALUE)

        .build();
    private ClientInfo mqtt5Client = ClientInfo.newBuilder()
        .setTenantId(tenantId)
        .putMetadata(MQTT_PROTOCOL_VER_KEY, MQTT_PROTOCOL_VER_5_VALUE)
        .build();

    @Test
    public void mqtt3DropPacket() {
        try (MockedStatic<ITenantMeter> mockedStatic = mockStatic(ITenantMeter.class)) {
            // simulate MyUtility.staticMethod()
            mockedStatic.when(() -> ITenantMeter.get(tenantId)).thenReturn(tenantMeter);
            when(tenantMeter.timer(any())).thenReturn(timer);
            MQTTPacketFilter testFilter =
                new MQTTPacketFilter(10, settings, mqtt3Client, eventCollector);
            EmbeddedChannel channel = new EmbeddedChannel(testFilter);
            MqttMessage largeMessage = MqttMessageBuilders.publish()
                .topicName("topic")
                .qos(MqttQoS.AT_MOST_ONCE)
                .payload(Unpooled.wrappedBuffer(new byte[100]))
                .build();
            channel.writeOutbound(largeMessage);
            verify(tenantMeter, never()).recordSummary(eq(TenantMetric.MqttEgressBytes), anyLong());
            verify(eventCollector).report(argThat(e -> e instanceof OversizePacketDropped));
            assertNull(channel.readOutbound());
        }
    }

    @Test
    public void logEgressMetric() {
        try (MockedStatic<ITenantMeter> mockedStatic = mockStatic(ITenantMeter.class)) {
            mockedStatic.when(() -> ITenantMeter.get(tenantId)).thenReturn(tenantMeter);
            when(tenantMeter.timer(any())).thenReturn(timer);
            MQTTPacketFilter testFilter =
                new MQTTPacketFilter(150, settings, mqtt3Client, eventCollector);
            EmbeddedChannel channel = new EmbeddedChannel(testFilter);
            MqttMessage largeMessage = MqttMessageBuilders.publish()
                .topicName("topic")
                .qos(MqttQoS.AT_MOST_ONCE)
                .payload(Unpooled.wrappedBuffer(new byte[100]))
                .build();
            channel.writeOutbound(largeMessage);
            verify(tenantMeter).recordSummary(eq(TenantMetric.MqttEgressBytes), anyDouble());
            verify(eventCollector, never()).report(argThat(e -> e instanceof OversizePacketDropped));
            assertEquals(channel.readOutbound(), largeMessage);
        }
    }

    @Test
    public void trimReasonStringOnly() {
        try (MockedStatic<ITenantMeter> mockedStatic = mockStatic(ITenantMeter.class)) {
            mockedStatic.when(() -> ITenantMeter.get(tenantId)).thenReturn(tenantMeter);
            when(tenantMeter.timer(any())).thenReturn(timer);
            // trim is enabled for MQTT5 client
            MQTTPacketFilter testFilter =
                new MQTTPacketFilter(17, settings, mqtt5Client, eventCollector);
            EmbeddedChannel channel = new EmbeddedChannel(testFilter);
            MqttProperties props = new MqttProperties();
            props.add(new MqttProperties.UserProperties(List.of(new MqttProperties.StringPair("key", "val"))));
            props.add(new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                "11111111111"));
            MqttMessage largeMessage = MqttMessageBuilders.pubAck()
                .packetId(1)
                .properties(props)
                .build();
            channel.writeOutbound(largeMessage);
            verify(tenantMeter).recordSummary(eq(TenantMetric.MqttEgressBytes), anyDouble());
            verify(eventCollector, never()).report(argThat(e -> e instanceof OversizePacketDropped));
            MqttMessage trimmedMessage = channel.readOutbound();
            assertNull(
                ((MqttPubReplyMessageVariableHeader) trimmedMessage.variableHeader()).properties().getProperty(
                    MqttProperties.MqttPropertyType.REASON_STRING.value()));
            assertEquals(
                ((MqttPubReplyMessageVariableHeader) trimmedMessage.variableHeader()).properties().getProperties(
                    MqttProperties.MqttPropertyType.USER_PROPERTY.value()).size(), 1);
        }
    }

    @Test
    public void trimReasonStringAndUserProps() {
        try (MockedStatic<ITenantMeter> mockedStatic = mockStatic(ITenantMeter.class)) {
            mockedStatic.when(() -> ITenantMeter.get(tenantId)).thenReturn(tenantMeter);
            when(tenantMeter.timer(any())).thenReturn(timer);
            // trim is enabled for MQTT5 client
            MQTTPacketFilter testFilter =
                new MQTTPacketFilter(6, settings, mqtt5Client, eventCollector);
            EmbeddedChannel channel = new EmbeddedChannel(testFilter);
            MqttProperties props = new MqttProperties();
            props.add(new MqttProperties.UserProperties(List.of(new MqttProperties.StringPair("key", "val"))));
            props.add(new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                "11111111111"));
            MqttMessage largeMessage = MqttMessageBuilders.pubAck()
                .packetId(1)
                .properties(props)
                .build();
            channel.writeOutbound(largeMessage);
            verify(tenantMeter).recordSummary(eq(TenantMetric.MqttEgressBytes), anyDouble());
            verify(eventCollector, never()).report(argThat(e -> e instanceof OversizePacketDropped));
            MqttMessage trimmedMessage = channel.readOutbound();
            assertNull(
                ((MqttPubReplyMessageVariableHeader) trimmedMessage.variableHeader()).properties().getProperty(
                    MqttProperties.MqttPropertyType.REASON_STRING.value()));
            assertEquals(
                ((MqttPubReplyMessageVariableHeader) trimmedMessage.variableHeader()).properties().getProperties(
                    MqttProperties.MqttPropertyType.USER_PROPERTY.value()).size(), 0);
        }
    }

    @Test
    public void dropUntrimableMessage() {
        try (MockedStatic<ITenantMeter> mockedStatic = mockStatic(ITenantMeter.class)) {
            mockedStatic.when(() -> ITenantMeter.get(tenantId)).thenReturn(tenantMeter);
            when(tenantMeter.timer(any())).thenReturn(timer);
            MQTTPacketFilter testFilter =
                new MQTTPacketFilter(108, settings, mqtt3Client, eventCollector);
            EmbeddedChannel channel = new EmbeddedChannel(testFilter);
            MqttProperties props = new MqttProperties();
            props.add(new MqttProperties.UserProperties(List.of(new MqttProperties.StringPair("key", "val"))));
            props.add(new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                "11111111111"));

            MqttMessage largeMessage = MqttMessageBuilders.publish()
                .topicName("topic")
                .qos(MqttQoS.AT_MOST_ONCE)
                .payload(Unpooled.wrappedBuffer(new byte[100]))
                .properties(props)
                .build();
            channel.writeOutbound(largeMessage);
            verify(tenantMeter, never()).recordSummary(eq(TenantMetric.MqttEgressBytes), anyDouble());
            verify(eventCollector).report(argThat(e -> e instanceof OversizePacketDropped));
            assertNull(channel.readOutbound());
        }
    }
}
