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

package org.apache.bifromq.mqtt.handler.v3;

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.inbox.rpc.proto.AttachReply;
import org.apache.bifromq.inbox.rpc.proto.DetachReply;
import org.apache.bifromq.mqtt.MockableTest;
import org.apache.bifromq.mqtt.handler.ChannelAttrs;
import org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils;
import org.apache.bifromq.mqtt.session.MQTTSessionContext;
import org.apache.bifromq.plugin.authprovider.IAuthProvider;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Granted;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTTAction;
import org.apache.bifromq.plugin.authprovider.type.Ok;
import org.apache.bifromq.plugin.clientbalancer.IClientBalancer;
import org.apache.bifromq.plugin.clientbalancer.Redirection;
import org.apache.bifromq.plugin.eventcollector.EventType;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.Redirect;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttVersion;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MQTT3ConnectHandlerTest extends MockableTest {

    private final String serverId = "serverId";
    private final int keepAlive = 2;
    private final String remoteIp = "127.0.0.1";
    private final int remotePort = 8888;
    private MQTT3ConnectHandler connectHandler;
    private EmbeddedChannel channel;
    @Mock
    private IClientBalancer clientBalancer;
    @Mock
    private IAuthProvider authProvider;
    @Mock
    private IInboxClient inboxClient;
    @Mock
    private IEventCollector eventCollector;
    @Mock
    private IResourceThrottler resourceThrottler;
    @Mock
    private ISettingProvider settingProvider;
    private MQTTSessionContext sessionContext;

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        connectHandler = new MQTT3ConnectHandler();
        when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        when(clientBalancer.needRedirect(any())).thenReturn(Optional.empty());
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
            .authProvider(authProvider)
            .inboxClient(inboxClient)
            .eventCollector(eventCollector)
            .resourceThrottler(resourceThrottler)
            .settingProvider(settingProvider)
            .clientBalancer(clientBalancer)
            .build();
        channel = new EmbeddedChannel(true, true, new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ch.attr(ChannelAttrs.MQTT_SESSION_CTX)
                    .set(sessionContext);
                ch.attr(ChannelAttrs.PEER_ADDR)
                    .set(new InetSocketAddress(remoteIp, remotePort));
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(connectHandler);
            }
        });
        channel.freezeTime();
    }

    @Test
    public void needMove() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();
        when(authProvider.auth(any(MQTT3AuthData.class))).thenReturn(CompletableFuture.completedFuture(
            MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId("tenantId")
                    .build())
                .build()));
        when(authProvider.checkPermission(any(ClientInfo.class), argThat(MQTTAction::hasConn))).thenReturn(
            CompletableFuture.completedFuture(CheckResult.newBuilder()
                .setGranted(Granted.getDefaultInstance())
                .build()));
        when(clientBalancer.needRedirect(any())).thenReturn(Optional.of(new Redirection(true, Optional.of("server1"))));
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertEquals(connAckMessage.variableHeader()
            .connectReturnCode(), CONNECTION_REFUSED_SERVER_UNAVAILABLE);
        assertFalse(channel.isOpen());
        verify(eventCollector).report(argThat(
            e -> e.type() == EventType.SERVER_REDIRECTED && ((Redirect) e).isPermanent() &&
                ((Redirect) e).serverReference()
                    .equals("server1")));
    }

    @Test
    public void needUseAnotherServer() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();
        when(authProvider.auth(any(MQTT3AuthData.class))).thenReturn(CompletableFuture.completedFuture(
            MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId("tenantId")
                    .build())
                .build()));
        when(authProvider.checkPermission(any(ClientInfo.class), argThat(MQTTAction::hasConn))).thenReturn(
            CompletableFuture.completedFuture(CheckResult.newBuilder()
                .setGranted(Granted.getDefaultInstance())
                .build()));
        when(clientBalancer.needRedirect(any())).thenReturn(Optional.of(new Redirection(false, Optional.empty())));
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertEquals(connAckMessage.variableHeader()
            .connectReturnCode(), CONNECTION_REFUSED_SERVER_UNAVAILABLE);
        assertFalse(channel.isOpen());
        verify(eventCollector).report(
            argThat(e -> e.type() == EventType.SERVER_REDIRECTED && !((Redirect) e).isPermanent()));
    }

    @Test
    public void overSizedLastWill() {
        when(settingProvider.provide(eq(Setting.MaxLastWillBytes), anyString())).thenReturn(128);
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .willFlag(true)
            .willTopic("topic")
            .willMessage(new byte[1024])
            .build();
        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId("tenantId")
                    .build())
                .build()));
        when(authProvider.checkPermission(any(ClientInfo.class), argThat(MQTTAction::hasConn)))
            .thenReturn(CompletableFuture.completedFuture(CheckResult.newBuilder()
                .setGranted(Granted.getDefaultInstance())
                .build()));
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertNull(connAckMessage);
        assertFalse(channel.isOpen());
        verify(eventCollector).report(argThat(e -> e.type() == EventType.PROTOCOL_VIOLATION));
    }

    @Test
    public void detachInboxBackPressureRejected() {
        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId("tenantId")
                    .build())
                .build()));
        when(authProvider.checkPermission(any(ClientInfo.class), argThat(MQTTAction::hasConn))).thenReturn(
            CompletableFuture.completedFuture(
                CheckResult.newBuilder().setGranted(Granted.getDefaultInstance()).build()));
        when(inboxClient.detach(any())).thenReturn(CompletableFuture.completedFuture(DetachReply.newBuilder()
            .setCode(DetachReply.Code.BACK_PRESSURE_REJECTED)
            .build()));

        MqttConnectMessage connMsg = MqttMessageBuilders.connect().clientId("client")
            .cleanSession(true)
            .properties(MQTT5MessageUtils.mqttProps()
                .addSessionExpiryInterval(10)
                .build())
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertEquals(connAckMessage.variableHeader().connectReturnCode(), CONNECTION_REFUSED_SERVER_UNAVAILABLE);
        assertFalse(channel.isOpen());
        verify(eventCollector).report(argThat(e -> e.type() == EventType.SERVER_BUSY));
    }

    @Test
    public void attachCallBackPressureRejected() {
        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId("tenantId")
                    .build())
                .build()));
        when(authProvider.checkPermission(any(ClientInfo.class), argThat(MQTTAction::hasConn))).thenReturn(
            CompletableFuture.completedFuture(
                CheckResult.newBuilder().setGranted(Granted.getDefaultInstance()).build()));
        when(inboxClient.attach(any())).thenReturn(CompletableFuture.completedFuture(AttachReply.newBuilder()
            .setCode(AttachReply.Code.BACK_PRESSURE_REJECTED)
            .build()));

        MqttConnectMessage connMsg = MqttMessageBuilders.connect().clientId("client")
            .cleanSession(false)
            .properties(MQTT5MessageUtils.mqttProps()
                .addSessionExpiryInterval(10)
                .build())
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertEquals(connAckMessage.variableHeader().connectReturnCode(), CONNECTION_REFUSED_SERVER_UNAVAILABLE);
        assertFalse(channel.isOpen());
        verify(eventCollector).report(argThat(e -> e.type() == EventType.SERVER_BUSY));
    }
}
