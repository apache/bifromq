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

import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalConnectPerSecond;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalConnections;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalSessionMemoryBytes;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;

import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.mqtt.MockableTest;
import org.apache.bifromq.mqtt.handler.record.GoAway;
import org.apache.bifromq.mqtt.session.MQTTSessionContext;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttVersion;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MQTTConnectHandlerTest extends MockableTest {
    private final String serverId = "serverId";
    private final int keepAlive = 2;
    private final String remoteIp = "127.0.0.1";
    private final int remotePort = 8888;
    private MQTTConnectHandler connectHandler;
    private EmbeddedChannel channel;

    @Mock
    private ISettingProvider settingProvider;
    @Mock
    private IInboxClient inboxClient;
    @Mock
    private IEventCollector eventCollector;
    @Mock
    private IResourceThrottler resourceThrottler;
    private MQTTSessionContext sessionContext;

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        connectHandler = Mockito.spy(MQTTConnectHandler.class);
        when(settingProvider.provide(any(Setting.class), anyString()))
            .thenAnswer(invocation -> {
                Setting setting = invocation.getArgument(0);
                switch (setting) {
                    case MinKeepAliveSeconds -> {
                        return keepAlive;
                    }
                    default -> {
                        return ((Setting) invocation.getArgument(0)).current(invocation.getArgument(1));
                    }
                }
            });
        sessionContext = MQTTSessionContext.builder()
            .serverId(serverId)
            .inboxClient(inboxClient)
            .eventCollector(eventCollector)
            .resourceThrottler(resourceThrottler)
            .settingProvider(settingProvider)
            .build();
        channel = new EmbeddedChannel(true, true, new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ch.attr(ChannelAttrs.MQTT_SESSION_CTX).set(sessionContext);
                ch.attr(ChannelAttrs.PEER_ADDR).set(new InetSocketAddress(remoteIp, remotePort));
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(connectHandler);
            }
        });
        channel.freezeTime();
    }

    @Test
    public void sanitizeCheckFailed() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();
        when(connectHandler.sanityCheck(connMsg)).thenReturn(new GoAway());
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        assertFalse(channel.isOpen());
    }

    @Test
    public void authenticateFailed() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();
        when(connectHandler.sanityCheck(connMsg)).thenReturn(null);
        when(connectHandler.authenticate(connMsg)).thenReturn(
            CompletableFuture.completedFuture(MQTTConnectHandler.AuthResult.goAway(null)));
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        assertFalse(channel.isOpen());
    }

    @Test
    public void checkConnPermissionFailed() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();
        ClientInfo clientInfo = ClientInfo.newBuilder().build();
        when(connectHandler.sanityCheck(connMsg)).thenReturn(null);
        when(connectHandler.authenticate(connMsg)).thenReturn(
            CompletableFuture.completedFuture(MQTTConnectHandler.AuthResult.ok(clientInfo)));
        when(connectHandler.checkConnectPermission(eq(connMsg), eq(MQTTConnectHandler.SuccessInfo.of(clientInfo))))
            .thenReturn(CompletableFuture.completedFuture(MQTTConnectHandler.AuthResult.goAway(null)));
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        assertFalse(channel.isOpen());
    }

    @Test
    public void noTotalConnectionResource() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();
        String tenantId = "tenantId";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        when(connectHandler.sanityCheck(connMsg)).thenReturn(null);
        when(connectHandler.authenticate(connMsg)).thenReturn(
            CompletableFuture.completedFuture(MQTTConnectHandler.AuthResult.ok(clientInfo)));
        when(connectHandler.checkConnectPermission(eq(connMsg), eq(MQTTConnectHandler.SuccessInfo.of(clientInfo))))
            .thenReturn(CompletableFuture.completedFuture(MQTTConnectHandler.AuthResult.ok(clientInfo)));
        when(connectHandler.onNoEnoughResources(connMsg, TotalConnections, clientInfo)).thenReturn(new GoAway());
        when(resourceThrottler.hasResource(tenantId, TotalConnections)).thenReturn(false);
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        assertFalse(channel.isOpen());
    }

    @Test
    public void noTotalSessionMemoryBytesResource() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();
        String tenantId = "tenantId";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        when(connectHandler.sanityCheck(connMsg)).thenReturn(null);
        when(connectHandler.authenticate(connMsg)).thenReturn(
            CompletableFuture.completedFuture(MQTTConnectHandler.AuthResult.ok(clientInfo)));
        when(connectHandler.checkConnectPermission(eq(connMsg), eq(MQTTConnectHandler.SuccessInfo.of(clientInfo))))
            .thenReturn(CompletableFuture.completedFuture(MQTTConnectHandler.AuthResult.ok(clientInfo)));
        when(connectHandler.onNoEnoughResources(connMsg, TotalSessionMemoryBytes, clientInfo)).thenReturn(new GoAway());
        when(resourceThrottler.hasResource(tenantId, TotalSessionMemoryBytes)).thenReturn(false);
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        assertFalse(channel.isOpen());
    }

    @Test
    public void noTotalConnectPerSecondResource() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();
        String tenantId = "tenantId";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        when(connectHandler.sanityCheck(connMsg)).thenReturn(null);
        when(connectHandler.authenticate(connMsg)).thenReturn(
            CompletableFuture.completedFuture(MQTTConnectHandler.AuthResult.ok(clientInfo)));
        when(connectHandler.checkConnectPermission(eq(connMsg), eq(MQTTConnectHandler.SuccessInfo.of(clientInfo))))
            .thenReturn(CompletableFuture.completedFuture(MQTTConnectHandler.AuthResult.ok(clientInfo)));
        when(connectHandler.onNoEnoughResources(connMsg, TotalConnectPerSecond, clientInfo)).thenReturn(new GoAway());
        when(resourceThrottler.hasResource(tenantId, TotalConnectPerSecond)).thenReturn(false);
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        assertFalse(channel.isOpen());
    }

    @Test
    public void validationFailed() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId("tenantId").build();
        when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        when(connectHandler.sanityCheck(connMsg)).thenReturn(null);
        when(connectHandler.authenticate(connMsg)).thenReturn(
            CompletableFuture.completedFuture(MQTTConnectHandler.AuthResult.ok(clientInfo)));
        when(connectHandler.checkConnectPermission(eq(connMsg), eq(MQTTConnectHandler.SuccessInfo.of(clientInfo))))
            .thenReturn(CompletableFuture.completedFuture(MQTTConnectHandler.AuthResult.ok(clientInfo)));
        when(connectHandler.validate(eq(connMsg), any(), eq(clientInfo))).thenReturn(new GoAway());
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        assertFalse(channel.isOpen());
    }

    @Test
    public void needRedirect() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId("tenantId").build();
        when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        when(connectHandler.sanityCheck(connMsg)).thenReturn(null);
        when(connectHandler.authenticate(connMsg)).thenReturn(
            CompletableFuture.completedFuture(MQTTConnectHandler.AuthResult.ok(clientInfo)));
        when(connectHandler.checkConnectPermission(eq(connMsg), eq(MQTTConnectHandler.SuccessInfo.of(clientInfo))))
            .thenReturn(CompletableFuture.completedFuture(MQTTConnectHandler.AuthResult.ok(clientInfo)));
        when(connectHandler.needRedirect(clientInfo)).thenReturn(new GoAway());
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        assertFalse(channel.isOpen());
    }
}
