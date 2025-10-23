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

import static io.netty.handler.codec.mqtt.MqttMessageType.DISCONNECT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.dist.client.PubResult;
import org.apache.bifromq.inbox.rpc.proto.DetachReply;
import org.apache.bifromq.inbox.rpc.proto.DetachRequest;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.mqtt.handler.BaseSessionHandlerTest;
import org.apache.bifromq.mqtt.handler.ChannelAttrs;
import org.apache.bifromq.mqtt.handler.TenantSettings;
import org.apache.bifromq.mqtt.handler.v5.reason.MQTT5DisconnectReasonCode;
import org.apache.bifromq.mqtt.session.IMQTTSession;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.type.Message;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PersistentSessionShutdownLWTTest extends BaseSessionHandlerTest {

    private LWT will;

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) {
        super.setup(method);
        mockSessionReg();
        will = LWT.newBuilder()
            .setTopic("willTopic")
            .setMessage(Message.newBuilder()
                .setPayload(ByteString.copyFromUtf8("will"))
                .setTimestamp(HLC.INST.get())
                .build())
            .setDelaySeconds(0)
            .build();

        doAnswer(inv -> null).when(localSessionRegistry).add(anyString(), any(IMQTTSession.class));

        mockInboxReader();
        ChannelDuplexHandler sessionHandlerAdder = buildChannelHandler();
        channel = new EmbeddedChannel(true, true, new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ch.attr(ChannelAttrs.MQTT_SESSION_CTX).set(sessionContext);
                ch.attr(ChannelAttrs.PEER_ADDR).set(new InetSocketAddress(remoteIp, remotePort));
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new ChannelTrafficShapingHandler(512 * 1024, 512 * 1024));
                pipeline.addLast(MqttDecoder.class.getName(), new MqttDecoder(256 * 1024));
                pipeline.addLast(sessionHandlerAdder);
            }
        });
    }

    @SneakyThrows
    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) {
        super.tearDown(method);
        will = null;
    }

    @Override
    protected ChannelDuplexHandler buildChannelHandler() {
        return new ChannelDuplexHandler() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                super.channelActive(ctx);
                ctx.pipeline().addLast(MQTT5PersistentSessionHandler.builder()
                    .settings(new TenantSettings(tenantId, settingProvider))
                    .tenantMeter(tenantMeter)
                    .oomCondition(oomCondition)
                    .inboxVersion(InboxVersion.newBuilder().setMod(0).setIncarnation(0).build())
                    .connMsg(MqttMessageBuilders.connect().protocolVersion(MqttVersion.MQTT_5).build())
                    .userSessionId("userSession")
                    .keepAliveTimeSeconds(120)
                    .sessionExpirySeconds(120)
                    .clientInfo(clientInfo)
                    .noDelayLWT(will)
                    .ctx(ctx)
                    .build());
                ctx.pipeline().remove(this);
            }
        };
    }

    @Test
    public void serverShutdownShouldDiscardLWT() {
        assertTrue(channel.isOpen());

        doAnswer(inv -> CompletableFuture.completedFuture(DetachReply.newBuilder()
            .setCode(DetachReply.Code.OK).build()))
            .when(inboxClient).detach(any());

        MQTT5PersistentSessionHandler handler = (MQTT5PersistentSessionHandler) channel.pipeline().last();
        handler.awaitInitialized().join();
        CompletableFuture<Void> shutdown = handler.onServerShuttingDown(); // non-blocking
        channel.runPendingTasks();
        channel.runScheduledPendingTasks();
        shutdown.join();

        MqttMessage disconnMessage = channel.readOutbound();
        assertEquals(disconnMessage.fixedHeader().messageType(), DISCONNECT);
        assertEquals(((MqttReasonCodeAndPropertiesVariableHeader) disconnMessage.variableHeader()).reasonCode(),
            MQTT5DisconnectReasonCode.ServerShuttingDown.value());
        channel.close();
        channel.runPendingTasks();
        assertFalse(channel.isActive());

        verify(distClient, never()).pub(anyLong(), anyString(), any(), any());

        ArgumentCaptor<DetachRequest> reqCap = ArgumentCaptor.forClass(DetachRequest.class);
        verify(inboxClient, timeout(1000)).detach(reqCap.capture());
        DetachRequest sent = reqCap.getValue();
        assertTrue(sent.getDiscardLWT());
    }

    @Test
    public void serverShutdownShouldSendLWTWhenSettingAllowed() {
        mockCheckPermission(true);
        when(settingProvider.provide(eq(Setting.NoLWTWhenServerShuttingDown), anyString())).thenReturn(false);
        when(distClient.pub(anyLong(), anyString(), any(), any())).thenReturn(
            CompletableFuture.completedFuture(PubResult.OK));
        doAnswer(inv -> CompletableFuture.completedFuture(DetachReply.newBuilder()
            .setCode(DetachReply.Code.OK).build()))
            .when(inboxClient).detach(any());

        ChannelDuplexHandler sessionHandlerAdder = buildChannelHandler();
        EmbeddedChannel ch = new EmbeddedChannel(true, true, new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel c) {
                c.attr(ChannelAttrs.MQTT_SESSION_CTX).set(sessionContext);
                c.attr(ChannelAttrs.PEER_ADDR).set(new InetSocketAddress(remoteIp, remotePort));
                ChannelPipeline pipeline = c.pipeline();
                pipeline.addLast(new ChannelTrafficShapingHandler(512 * 1024, 512 * 1024));
                pipeline.addLast(MqttDecoder.class.getName(), new MqttDecoder(256 * 1024));
                pipeline.addLast(sessionHandlerAdder);
            }
        });

        try {
            assertTrue(ch.isOpen());
            MQTT5PersistentSessionHandler handler = (MQTT5PersistentSessionHandler) ch.pipeline().last();
            handler.awaitInitialized().join();
            CompletableFuture<Void> shutdown = handler.onServerShuttingDown(); // non-blocking
            ch.runPendingTasks();
            ch.runScheduledPendingTasks();
            shutdown.join();

            MqttMessage disconnMessage = ch.readOutbound();
            assertEquals(disconnMessage.fixedHeader().messageType(), DISCONNECT);
            assertEquals(((MqttReasonCodeAndPropertiesVariableHeader) disconnMessage.variableHeader()).reasonCode(),
                MQTT5DisconnectReasonCode.ServerShuttingDown.value());

            ch.close();
            ch.runPendingTasks();
            assertFalse(ch.isActive());

            verify(distClient, timeout(1000)).pub(anyLong(), anyString(), any(), any());

            ArgumentCaptor<DetachRequest> reqCap = ArgumentCaptor.forClass(DetachRequest.class);
            verify(inboxClient, timeout(1000)).detach(reqCap.capture());
            assertTrue(reqCap.getValue().getDiscardLWT());
        } finally {
            if (ch.isOpen()) {
                ch.close();
            }
        }
    }
}
