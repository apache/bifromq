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

import static org.apache.bifromq.mqtt.handler.MQTTSessionIdUtil.userSessionId;
import static org.apache.bifromq.mqtt.handler.v5.MQTT5MessageUtils.toUserProperties;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.mqtt.handler.BaseSessionHandlerTest;
import org.apache.bifromq.mqtt.handler.ChannelAttrs;
import org.apache.bifromq.mqtt.handler.TenantSettings;
import org.apache.bifromq.mqtt.session.IMQTTTransientSession;
import org.apache.bifromq.mqtt.spi.UserProperty;
import org.apache.bifromq.mqtt.utils.MQTTMessageUtils;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Granted;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.StringPair;
import org.apache.bifromq.type.TopicFilterOption;
import org.apache.bifromq.type.TopicMessagePack;
import org.apache.bifromq.type.UserProperties;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TransientSessionCustomizedUserPropsTest extends BaseSessionHandlerTest {
    private MQTT5TransientSessionHandler transientSessionHandler;

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) {
        super.setup(method);
        ChannelDuplexHandler sessionHandlerAdder = buildChannelHandler();
        mockInboxReader();
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
        transientSessionHandler = (MQTT5TransientSessionHandler) channel.pipeline().last();
    }

    @Override
    protected ChannelDuplexHandler buildChannelHandler() {
        return new ChannelDuplexHandler() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                super.channelActive(ctx);
                ctx.pipeline().addLast(MQTT5TransientSessionHandler.builder()
                    .settings(new TenantSettings(tenantId, settingProvider))
                    .tenantMeter(tenantMeter)
                    .oomCondition(oomCondition)
                    .connMsg(MqttMessageBuilders.connect()
                        .protocolVersion(MqttVersion.MQTT_5)
                        .build())
                    .userSessionId(userSessionId(clientInfo))
                    .keepAliveTimeSeconds(120)
                    .clientInfo(clientInfo)
                    .willMessage(null).ctx(ctx)
                    .build());
                ctx.pipeline().remove(this);
            }
        };
    }

    @Test
    public void grantUserPropsForSub() {
        UserProperties grantedUserProps = UserProperties.newBuilder()
            .addUserProperties(StringPair.newBuilder()
                .setKey("key2")
                .setValue("value2")
                .build())
            .build();
        when(authProvider.checkPermission(any(ClientInfo.class), any()))
            .thenReturn(CompletableFuture.completedFuture(
                CheckResult.newBuilder()
                    .setGranted(Granted.newBuilder()
                        .setUserProps(grantedUserProps)
                        .build())
                    .build()));
        mockDistMatch(true);
        mockRetainMatch();
        int[] qos = {0};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(new String[] {topicFilter}, qos);
        channel.writeInbound(subMessage);
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, new int[] {0});

        ArgumentCaptor<Long> longCaptor = ArgumentCaptor.forClass(Long.class);
        verify(localDistService).match(anyLong(), eq(topicFilter), longCaptor.capture(), any());

        when(userPropsCustomizer.outbound(anyString(), any(), any(), anyString(), any(), any(), anyLong()))
            .thenReturn(List.of(new UserProperty("key1", "value1")));

        TopicMessagePack pack = TopicMessagePack.newBuilder()
            .setTopic(topic)
            .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                .setPublisher(ClientInfo.newBuilder().build())
                .addMessage(Message.newBuilder()
                    .setMessageId(0)
                    .setExpiryInterval(Integer.MAX_VALUE)
                    .setPayload(ByteString.EMPTY)
                    .setTimestamp(HLC.INST.get())
                    .setPubQoS(QoS.AT_MOST_ONCE)
                    .build()))
            .build();

        transientSessionHandler.publish(pack,
            Collections.singleton(new IMQTTTransientSession.MatchedTopicFilter(topicFilter, longCaptor.getValue())));
        channel.runPendingTasks();
        MqttPublishMessage message = channel.readOutbound();
        UserProperties userProperties = toUserProperties(message.variableHeader().properties());
        assertEquals(userProperties.getUserPropertiesList().size(), 1);
        StringPair userProperty = userProperties.getUserPropertiesList().get(0);
        assertEquals(userProperty.getKey(), "key1");
        assertEquals(userProperty.getValue(), "value1");

        ArgumentCaptor<TopicFilterOption> topicFilterOptionCaptor = ArgumentCaptor.forClass(TopicFilterOption.class);
        verify(userPropsCustomizer).outbound(anyString(), any(), any(), eq(topicFilter),
            topicFilterOptionCaptor.capture(), any(), anyLong());
        TopicFilterOption topicFilterOption = topicFilterOptionCaptor.getValue();
        assertEquals(topicFilterOption.getUserProperties(), grantedUserProps);
    }
}
